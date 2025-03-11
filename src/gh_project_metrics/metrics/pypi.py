import logging
from pathlib import Path
from typing import Self

import pandas as pd
import requests
from google.cloud import bigquery

from gh_project_metrics.metrics import MetricsProvider, metric
from gh_project_metrics.util import TIMESTAMP_FORMAT, combine_csv


class PyPIMetrics(MetricsProvider):
    """PyPI package metrics collector

    Attributes
    ==========
    package_name: str
        PyPI package name
    gcp_project_id: str | None
        Google Cloud project ID (need BigQuery job user permissions)
    """

    @classmethod
    def from_raw_data(cls, data_dir: Path, *init_args) -> Self:
        downloads = pd.read_csv(
            data_dir / "pypi_downloads.csv",
            date_format=TIMESTAMP_FORMAT,
            index_col=["version", "date"],
        )
        releases = pd.read_csv(
            data_dir / "pypi_releases.csv",
            date_format=TIMESTAMP_FORMAT,
            index_col="version",
        )

        instance = cls(*init_args)

        # Restore the caches for the @metric functions called without arguments
        instance._downloads_cache = {(): downloads}  # type: ignore[attr-defined]
        instance._releases_cache = {(): releases}  # type: ignore[attr-defined]
        return instance

    def __init__(
        self,
        package_name: str,
        gcp_project_id: str | None = None,
    ) -> None:
        self.package_name = package_name
        self.gcp_project_id = gcp_project_id

    @property
    def name(self) -> str:
        return f"PyPI ({self.package_name})"

    def dump_raw_data(self, outdir: Path) -> None:
        if not outdir.is_dir():
            raise ValueError(f"not a directory: {outdir!r}")

        combine_csv(
            self.downloads(),
            outdir / "pypi_downloads.csv",
            sort_kwargs={"level": ["version", "date"], "ascending": [False, True]},
        )
        combine_csv(
            self.releases(),
            outdir / "pypi_releases.csv",
            sort_kwargs={"level": "version", "ascending": False},
        )

    @metric
    def downloads(self) -> pd.DataFrame:
        query = f"""
SELECT
  file.version AS version,
  DATE_TRUNC(timestamp, DAY) AS date,
  COUNT(*) AS num_downloads
FROM
  `bigquery-public-data.pypi.file_downloads`
WHERE
  file.project = "{self.package_name}"
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
        client = bigquery.Client(project=self.gcp_project_id)
        df: pd.DataFrame = client.query_and_wait(query).to_dataframe()
        if len(df) == 0:
            logging.warning("BigQuery returned empty DataFrame")

        df["date"] = df["date"].astype("datetime64[ns, UTC]")  # match existing data
        df = df.set_index(["version", "date"])
        return df

    @metric
    def releases(self) -> pd.DataFrame:
        r = requests.get(f"https://pypi.org/pypi/{self.package_name}/json")
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
