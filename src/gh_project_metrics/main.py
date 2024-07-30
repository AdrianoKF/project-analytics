import logging
import os
from pathlib import Path

import plotly.express as px
from github import Github

from gh_project_metrics.cli import args
from gh_project_metrics.db import BigQueryWriter, DatabaseWriter, SupabaseWriter
from gh_project_metrics.gcp import (
    DEFAULT_LOOKERSTUDIO_TEMPLATE_REPORT_ID,
    create_lookerstudio_report_url,
    get_gcp_project_id,
)
from gh_project_metrics.metrics.github import GithubMetrics, MetricsConfig
from gh_project_metrics.metrics.pypi import PyPIMetrics
from gh_project_metrics.plotting import PLOT_TEMPLATE, add_weekends, format_plot, write_plot


def github_metrics(
    repo_name: str,
    datadir: Path,
    combined_data_dir: Path | None = None,
    plotdir: Path | None = None,
    db_writer: DatabaseWriter | None = None,
) -> None:
    gh = Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN"))
    repo = gh.get_repo(repo_name)
    config = MetricsConfig(aggregate_time="D")

    metrics = GithubMetrics(repo, config)
    metrics.dump()

    stars = metrics.stars()
    views = metrics.views()
    clones = metrics.clones()

    metrics.dump_raw_data(datadir)
    if combined_data_dir:
        metrics.dump_raw_data(combined_data_dir)

    if db_writer:
        for metric in metrics:
            db_writer.write(df=metric.data, table=metric.name)

    if not plotdir:
        return

    # Plot Stars
    fig = px.line(
        stars.reset_index(),
        x="date",
        y="stars",
        markers=True,
        title="GitHub Stars",
        template=PLOT_TEMPLATE,
    )
    add_weekends(fig, start=stars.index.min(), end=stars.index.max())
    format_plot(fig)
    write_plot(fig, plotdir, "stars")

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
        template=PLOT_TEMPLATE,
    )
    add_weekends(fig, start=clones.index.min(), end=clones.index.max())
    format_plot(fig)
    write_plot(fig, plotdir, "clones")

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
        template=PLOT_TEMPLATE,
    )
    add_weekends(fig, start=views.index.min(), end=views.index.max())
    format_plot(fig)
    write_plot(fig, plotdir, "views")


def pypi_metrics(
    package_name: str,
    datadir: Path,
    combined_data_dir: Path | None = None,
    plotdir: Path | None = None,
    db_writer: DatabaseWriter | None = None,
    gcp_project_id: str | None = None,
) -> None:
    metrics = PyPIMetrics(package_name=package_name, gcp_project_id=gcp_project_id)

    downloads = metrics.downloads()
    metrics.dump()

    metrics.dump_raw_data(datadir)
    if combined_data_dir:
        metrics.dump_raw_data(combined_data_dir)

    if db_writer:
        for metric in metrics:
            db_writer.write(df=metric.data, table=metric.name)

    if not plotdir:
        return

    plot_df = downloads.reset_index()
    fig = px.line(
        plot_df,
        x="date",
        y="num_downloads",
        color="version",
        markers=True,
        title="PyPI Downloads",
        template=PLOT_TEMPLATE,
    )

    # Overlay rectangles for weekends
    add_weekends(
        fig,
        start=downloads.index.levels[1].min(),
        end=downloads.index.levels[1].max(),
    )

    format_plot(fig)
    write_plot(fig, plotdir, "pypi_downloads")


def run():
    logging.basicConfig(level=logging.INFO)

    # Round-trip conversion to validate input
    today = args.date.strftime("%Y-%m-%d")
    logging.info("Running for %s", today)

    github_name = args.name
    project_name = github_name.split("/")[-1]
    pypi_name = project_name.lower()  # XXX: Assumes package name matches project name in lowercase
    gcp_project_id = get_gcp_project_id(args)

    db_writer: DatabaseWriter | None = None
    if args.supabase:
        logging.info("Enabling logging to Supabase")
        db_writer = SupabaseWriter(project_name)
    elif args.bigquery:
        logging.info("Enabling logging to BigQuery")
        db_writer = BigQueryWriter(
            project_id=gcp_project_id,
            dataset_name=project_name,
            dataset_name_prefix=args.bigquery_dataset_prefix,
            create_dataset=True,
        )

    datadir = Path.cwd() / "data" / project_name / today
    datadir.mkdir(exist_ok=True, parents=True)

    combined_data_dir = Path.cwd() / "data" / project_name / "combined"
    combined_data_dir.mkdir(exist_ok=True, parents=True)

    plotdir = Path.cwd() / "plots" / project_name / today
    plotdir.mkdir(exist_ok=True, parents=True)

    logging.info("Data output dir: %s", datadir)
    logging.info("Combined output dir: %s", combined_data_dir)
    logging.info("Plots output dir: %s", plotdir)

    if args.github:
        logging.info("Collecting GitHub metrics")
        github_metrics(
            github_name,
            datadir,
            combined_data_dir=combined_data_dir,
            plotdir=plotdir,
            db_writer=db_writer,
        )

    if args.pypi:
        logging.info("Collecting PyPI metrics")
        pypi_metrics(
            pypi_name,
            datadir,
            combined_data_dir=combined_data_dir,
            plotdir=plotdir,
            gcp_project_id=gcp_project_id,
            db_writer=db_writer,
        )

    if args.bigquery:
        report_url = create_lookerstudio_report_url(
            DEFAULT_LOOKERSTUDIO_TEMPLATE_REPORT_ID,
            project_name=project_name,
            gcp_project_id=gcp_project_id,
            bq_dataset=db_writer._dataset_ref.dataset_id,
        )
        logging.info(f"Looker Studio report creation URL: {report_url}")


if __name__ == "__main__":
    run()
