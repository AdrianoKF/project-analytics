import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import pandas as pd
import requests
from google.cloud import bigquery

from gh_project_metrics.metrics import BaseMetric, MetricsProvider
from gh_project_metrics.util import TIMESTAMP_FORMAT


@dataclass(slots=True, frozen=True)
class Config:
    package_name: str
    gcp_project_id: str | None = None


class PyPIMetric(BaseMetric[Config, "PyPIMetrics"]): ...


class DownloadsMetric(PyPIMetric):
    def _compute(self, *args) -> pd.DataFrame:
        query = f"""
        SELECT
          file.version AS version,
          DATE_TRUNC(timestamp, DAY) AS date,
          COUNT(*) AS num_downloads
        FROM
          `bigquery-public-data.pypi.file_downloads`
        WHERE
          file.project = "{self.provider.config.package_name}"
          -- Only query the last 30 days of history
          AND DATE(timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
          AND CURRENT_DATE()
          -- exclude known crawlers, like PyPI mirror clients
          AND details.installer.name NOT IN ("bandersnatch", "devpi")
        GROUP BY
          version,
          `date`
        ORDER BY
          version DESC,
          `date`
                """
        client = bigquery.Client(project=self.provider.config.gcp_project_id)
        df: pd.DataFrame = client.query_and_wait(query).to_dataframe()
        if len(df) == 0:
            logging.warning("BigQuery returned empty DataFrame")

        df["date"] = df["date"].astype("datetime64[ns, UTC]")  # match existing data
        df = df.set_index(["version", "date"])
        return df

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        downloads = pd.read_csv(
            indir / f"{name}.csv",
            date_format=TIMESTAMP_FORMAT,
            index_col=["version", "date"],
        )
        return cls.from_data(name, downloads)


class ReleasesMetric(PyPIMetric):
    def _compute(self, *args) -> pd.DataFrame:
        r = requests.get(f"https://pypi.org/pypi/{self.provider.config.package_name}/json")
        r.raise_for_status()
        data = r.json()
        records = [
            {
                "version": version,
                "upload_time": rd["upload_time"],
                "yanked": rd["yanked"],
            }
            for version, release_data in data["releases"].items()
            for rd in release_data
            if rd["packagetype"] == "bdist_wheel"
        ]
        df = pd.DataFrame.from_records(records)
        df = df.set_index("version").sort_index(ascending=False)
        return df

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        releases = pd.read_csv(
            indir / f"{name}.csv",
            date_format=TIMESTAMP_FORMAT,
            index_col=["version"],
        )
        return cls.from_data(name, releases)


class PyPIMetrics(MetricsProvider[Config, PyPIMetric]):
    downloads: DownloadsMetric
    releases: ReleasesMetric

    @property
    def name(self) -> str:
        return f"PyPI ({self.config.package_name})"
