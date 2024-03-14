import abc
import logging
import os
from typing import override

import pandas as pd
import supabase
from supabase.lib import client_options

from gh_project_metrics.util import TIMESTAMP_FORMAT


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
