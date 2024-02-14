from functools import cache
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from gh_project_metrics.util import combine_csv


class PyPIMetrics:
    """PyPI package metrics collector

    Attributes
    ==========
    package_name: str
        PyPI package name
    """

    def __init__(self, package_name: str) -> None:
        self.package_name = package_name

    def dump_raw_data(self, outdir: Path) -> None:
        if not outdir.is_dir():
            raise ValueError(f"not a directory: {outdir!r}")

        combine_csv(
            self.downloads(),
            outdir / "pypi_downloads.csv",
            sort_kwargs={"level": ["version", "date"], "ascending": [False, True]},
        )

    @cache
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
GROUP BY
  version,
  `date`
ORDER BY
  version DESC,
  `date`
        """
        client = bigquery.Client()
        df: pd.DataFrame = client.query_and_wait(query).to_dataframe()
        df["date"] = df["date"].astype("datetime64[ns, UTC]")  # match existing data
        df = df.set_index(["version", "date"])
        return df
