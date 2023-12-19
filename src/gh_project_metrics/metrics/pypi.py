import datetime
from functools import cache
from pathlib import Path

import pandas as pd

from gh_project_metrics.util import combine_csv


class PyPIMetrics:
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
        today = datetime.date.today().strftime("%F")
        df = pd.read_csv(
            f"https://storage.googleapis.com/pypi-download-stats/{today}/000000000000.csv",
            delimiter=";",
            index_col=["version", "date"],
            parse_dates=True,
        )
        return df
