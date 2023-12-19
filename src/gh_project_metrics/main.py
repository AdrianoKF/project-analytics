import datetime
import logging
import os
from pathlib import Path
from pprint import pp

import plotly.express as px
from github import Github
from plotly.graph_objects import Figure

from gh_project_metrics.metrics.github import GithubMetrics, MetricsConfig
from gh_project_metrics.metrics.pypi import PyPIMetrics

logging.basicConfig(level=logging.INFO)


def _header(title: str) -> str:
    return f"---------- {title:<20} ----------"


def _plot_path(name: str, plotdir: Path) -> Path:
    return plotdir / f"{name}.png"


def _write_plot(fig: Figure, plotdir: Path, name: str) -> Path:
    image_path = _plot_path(name, plotdir)
    image = fig.to_image(
        format=image_path.suffix.strip("."),
        width=1200,
        height=800,
    )
    image_path.write_bytes(image)

    plot_json = fig.to_json()
    image_path.with_suffix(".json").write_text(plot_json, encoding="utf8")

    return image_path


def github_metrics(datadir: Path, plotdir: Path | None = None) -> None:
    gh = Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN"))
    repo = gh.get_repo("aai-institute/lakefs-spec")
    config = MetricsConfig(aggregate_time="D")

    metrics = GithubMetrics(repo, config)

    print(_header("Stars"))
    stars = metrics.stars()
    pp(stars)

    print(_header("Views"))
    views = metrics.views()
    pp(views)

    print(_header("Clones"))
    clones = metrics.clones()
    pp(clones)

    print(_header("Collaborators"))
    pp(metrics.referrers())

    print(_header("History of time-series metrics"))
    pp(metrics.history())

    metrics.dump_raw_data(datadir)
    metrics.dump_raw_data(datadir.parent / "combined")

    if not plotdir:
        return

    # Plot Stars
    fig = px.line(
        stars.reset_index(),
        x="date",
        y="stars",
        markers=True,
        title="GitHub Stars",
    )
    fig.update_layout(yaxis_title=None, xaxis_title=None)
    _write_plot(fig, plotdir, "stars")

    # Plot Clones
    fig = px.line(
        clones.reset_index().melt(
            id_vars="date",
            value_vars=["unique", "count"],
        ),
        x="date",
        y="value",
        color="variable",
        markers=True,
        title="GitHub Clones",
    )
    fig.update_layout(yaxis_title=None, xaxis_title=None)
    _write_plot(fig, plotdir, "clones")

    # Plot Views
    fig = px.line(
        views.reset_index().melt(
            id_vars="date",
            value_vars=["unique", "count"],
        ),
        x="date",
        y="value",
        color="variable",
        markers=True,
        title="GitHub Views",
    )
    fig.update_layout(yaxis_title=None, xaxis_title=None)
    _write_plot(fig, plotdir, "views")


def pypi_metrics(datadir: Path, plotdir: Path | None = None) -> None:
    metrics = PyPIMetrics()
    downloads = metrics.downloads()

    pp(downloads)
    pp(downloads.groupby("date")["num_downloads"].sum())

    metrics.dump_raw_data(datadir)
    metrics.dump_raw_data(datadir.parent / "combined")

    if not plotdir:
        return

    fig = px.line(
        downloads.reset_index(),
        x="date",
        y="num_downloads",
        color="version",
        markers=True,
        title="PyPI Downloads",
    )
    fig.update_layout(yaxis_title=None, xaxis_title=None)
    _write_plot(fig, plotdir, "pypi_downloads")


def run():
    today = datetime.date.today().strftime("%F")
    datadir = Path.cwd() / "data" / today
    datadir.mkdir(exist_ok=True, parents=True)

    plotdir = Path.cwd() / "plots" / today
    plotdir.mkdir(exist_ok=True, parents=True)

    github_metrics(datadir, plotdir)
    pypi_metrics(datadir, plotdir)


if __name__ == "__main__":
    run()
