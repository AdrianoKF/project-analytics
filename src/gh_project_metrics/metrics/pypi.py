import datetime
from functools import cache
from pathlib import Path

import pandas as pd


class PyPIMetrics:
    def dump_raw_data(self, outdir: Path) -> None:
        if not outdir.is_dir():
            raise ValueError(f"not a directory: {outdir!r}")

        self.downloads().to_csv(outdir / "pypi_downloads.csv", index=True)

    @cache
    def downloads(self) -> pd.DataFrame:
        today = datetime.date.today().strftime("%F")
        df = pd.read_csv(
            f"https://storage.googleapis.com/pypi-download-stats/{today}/000000000000.csv",
            delimiter=";",
        )
        df.set_index(["version", "date"], inplace=True)
        return df
