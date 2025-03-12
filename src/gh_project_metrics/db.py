import abc
import os
from typing import override

import pandas as pd
import supabase
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from loguru import logger
from supabase.lib import client_options

from gh_project_metrics.util import TIMESTAMP_FORMAT, sanitize_name


class DatabaseWriter(abc.ABC):
    @property
    def name(self) -> str:
        return self.__class__.__name__

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

    @property
    def name(self) -> str:
        return f"Supabase ({self.project_name})"

    @override
    def write(self, df: pd.DataFrame, table: str) -> None:
        logger.debug("Logging data to {}", table)
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
                logger.info(
                    f"BigQuery dataset {self._dataset_ref.dataset_id!r} in project {self._dataset_ref.project!r} created"
                )
            else:
                raise FileNotFoundError(
                    f"BigQuery dataset {dataset_name!r} does not exist in project {project_id!r} and create_dataset=False"
                ) from e

    @property
    def name(self) -> str:
        return f"BigQuery ({self._dataset_ref.project}.{self._dataset_ref.dataset_id})"

    @override
    def write(self, df: pd.DataFrame, table: str) -> None:
        table_name = sanitize_name(table)
        destination_table = f"{self._dataset_ref.dataset_id}.{table_name}"

        logger.info(
            f"Writing table {table!r} to BigQuery table {destination_table!r} in project {self._dataset_ref.project!r}"
        )

        client = bigquery.Client(project=self._dataset_ref.project)

        # Check if the destination table exists
        try:
            client.get_table(destination_table)
            table_exists = True
        except NotFound:
            table_exists = False

        if not table_exists:
            logger.info(f"Table {destination_table!r} does not exist. Creating and inserting data.")
            job_config = bigquery.LoadJobConfig()
            load_job = client.load_table_from_dataframe(
                df, destination_table, job_config=job_config
            )
            load_job.result()  # Wait for the job to complete
            return

        # Load data into a temporary table
        temp_table = f"{destination_table}_temp"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        load_job = client.load_table_from_dataframe(df, temp_table, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        # Define the merge query
        id_column = df.index.name if df.index.name else "id"
        merge_query = f"""
        MERGE `{destination_table}` T
        USING `{temp_table}` S
        ON {" AND ".join([f"T.{name} = S.{name}" for name in df.index.names]) if isinstance(df.index, pd.MultiIndex) else f"T.{id_column} = S.{id_column}"}
        WHEN MATCHED THEN
          UPDATE SET {", ".join([f"T.{col} = S.{col}" for col in df.columns])}
        WHEN NOT MATCHED THEN
          INSERT ROW
        """

        # Execute the merge query
        logger.info(f"Merging data into destination table {destination_table!r}")
        query_job = client.query(merge_query)
        query_job.result()  # Wait for the job to complete

        # Delete the temporary table
        client.delete_table(temp_table)
