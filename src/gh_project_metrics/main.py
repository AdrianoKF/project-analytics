import datetime
import logging
import os
from pathlib import Path
from pprint import pp

from github import Github

from gh_project_metrics.metrics.github import GithubMetrics, MetricsConfig
from gh_project_metrics.metrics.pypi import PyPIMetrics

logging.basicConfig(level=logging.INFO)


def _header(title: str) -> str:
    return f"---------- {title:<20} ----------"


def github_metrics(datadir: Path):
    gh = Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN"))
    repo = gh.get_repo("aai-institute/lakefs-spec")
    config = MetricsConfig(aggregate_time="D")

    metrics = GithubMetrics(repo, config)

    print(_header("Stars"))
    stars = metrics.stars()
    pp(stars)

    print(_header("Views"))
    pp(metrics.views())

    print(_header("Clones"))
    pp(metrics.clones())

    print(_header("Collaborators"))
    pp(metrics.referrers())

    print(_header("History of time-series metrics"))
    pp(metrics.history())

    metrics.dump_raw_data(datadir)


def pypi_metrics(datadir: Path):
    metrics = PyPIMetrics()
    downloads = metrics.downloads()

    pp(downloads)
    pp(downloads.groupby("date")["num_downloads"].sum())

    metrics.dump_raw_data(datadir)


def run():
    today = datetime.date.today().strftime("%F")
    datadir = Path.cwd() / "data" / today
    datadir.mkdir(exist_ok=True, parents=True)

    github_metrics(datadir)
    pypi_metrics(datadir)


if __name__ == "__main__":
    run()
