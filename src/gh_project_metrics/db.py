import abc
import logging
import os
from typing import override

import pandas as pd
import pandas_gbq
import supabase
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from supabase.lib import client_options

from gh_project_metrics.util import TIMESTAMP_FORMAT, sanitize_name


class DatabaseWriter(abc.ABC):
    @abc.abstractmethod
    def write(self, df: pd.DataFrame, table: str) -> None: ...


class SupabaseWriter(DatabaseWriter):
    def __init__(self, project_name: str) -> None:
        super().__init__()

        self.project_name = project_name

        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        opts = client_options.ClientOptions(schema=project_name)
        self._client = supabase.create_client(url, key, options=opts)

    @override
    def write(self, df: pd.DataFrame, table: str) -> None:
        logging.debug("Logging data to %s", table)
        data = df.reset_index()

        # Transform datetime types into strings, so they can be JSON-serialized
        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.strftime(TIMESTAMP_FORMAT)

        self._client.table(table).upsert(data.to_dict("records")).execute()


class BigQueryWriter(DatabaseWriter):
    def __init__(
        self,
        project_id: str,
        dataset_name: str,
        dataset_name_prefix: str = "",
        create_dataset: bool = False,
    ) -> None:
        super().__init__()

        dataset_name = sanitize_name(dataset_name_prefix + dataset_name)
        try:
            client = bigquery.Client(project=project_id)
            self._dataset_ref = bigquery.DatasetReference(project_id, dataset_name)
            client.get_dataset(self._dataset_ref)
        except NotFound as e:
            if create_dataset:
                dataset = bigquery.Dataset(self._dataset_ref)
                dataset.location = "EU"
                dataset = client.create_dataset(dataset)
                logging.info(
                    f"BigQuery dataset {self._dataset_ref.dataset_id!r} in project {self._dataset_ref.project!r} created"
                )
            else:
                raise FileNotFoundError(
                    f"BigQuery dataset {dataset_name!r} does not exist in project {project_id!r} and create_dataset=False"
                ) from e

    @override
    def write(self, df: pd.DataFrame, table: str) -> None:
        table_name = sanitize_name(table)
        destination_table = f"{self._dataset_ref.dataset_id}.{table_name}"
        logging.info(
            f"Writing table {table!r} to BigQuery table {destination_table!r} in project {self._dataset_ref.project!r}"
        )
        pandas_gbq.to_gbq(
            df,
            project_id=self._dataset_ref.project,
            destination_table=destination_table,
            if_exists="replace",
        )
