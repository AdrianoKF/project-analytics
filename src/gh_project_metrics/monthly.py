"""Monthly aggregate metrics from combined data for a project in the GitHub repo."""

import json
import logging
from datetime import UTC, datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from gh_project_metrics.util import TIMESTAMP_FORMAT

PROJECT_NAME = "lakefs-spec"
# PROJECT_NAME = "nnbench"
BASE_URL = f"https://raw.githubusercontent.com/AdrianoKF/project-analytics/data/data/{PROJECT_NAME}/combined"


def run(relative_to: datetime) -> None:
    # start of previous month
    start_date = relative_to + relativedelta(
        hour=0, minute=0, second=0, microsecond=0, day=1, months=-1
    )
    # last microsecond of the previous month (since df.truncate is inclusive)
    end_date = start_date + relativedelta(months=1, microseconds=-1)

    logging.debug(f"Date range: {start_date} - {end_date}")

    COUNT_METRICS = {
        "clones": {"count": pd.Series.sum, "unique": pd.Series.sum},
        "views": {"count": pd.Series.sum, "unique": pd.Series.sum},
        "stars": {"stars": pd.Series.max},
    }
    result = {}
    for metric, attributes in COUNT_METRICS.items():
        df = pd.read_csv(
            f"{BASE_URL}/{metric}.csv",
            date_format=TIMESTAMP_FORMAT,
            index_col="date",
        )
        df = df.truncate(before=start_date, after=end_date)
        result[metric] = {
            f"{fn.__name__}_{attr}": round(fn(df[attr])) for attr, fn in attributes.items()
        }

    df = pd.read_csv(
        f"{BASE_URL}/pypi_downloads.csv",
        date_format=TIMESTAMP_FORMAT,
        index_col="date",
    )
    df = df.sort_index()  # truncate() requires the index to be sorted
    df = df.truncate(before=start_date, after=end_date)
    result["pypi_downloads"] = df.groupby("version")["num_downloads"].sum().to_dict()

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # today = datetime.now(tz=UTC)
    today = datetime(year=2024, month=3, day=1, tzinfo=UTC)  # run for February 2024
    logging.info(f"Running for {today}")
    run(today)
