import os
from pathlib import Path

from github import Github
from loguru import logger

from gh_project_metrics.cli import args
from gh_project_metrics.db import BigQueryWriter, DatabaseWriter, SupabaseWriter
from gh_project_metrics.gcp import (
    DEFAULT_LOOKERSTUDIO_TEMPLATE_REPORT_ID,
    create_lookerstudio_report_url,
    get_gcp_project_id,
)
from gh_project_metrics.metrics import MetricsProvider
from gh_project_metrics.metrics.github import GithubMetrics, MetricsConfig
from gh_project_metrics.metrics.pypi import PyPIMetrics
from gh_project_metrics.plotting import plot_github_metrics, plot_pypi_metrics


def github_metrics(
    repo_name: str,
) -> GithubMetrics:
    gh = Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN"))
    repo = gh.get_repo(repo_name)
    config = MetricsConfig(aggregate_time="D")
    metrics = GithubMetrics(repo, config)
    return metrics


def pypi_metrics(
    package_name: str,
    gcp_project_id: str | None = None,
) -> PyPIMetrics:
    metrics = PyPIMetrics(package_name=package_name, gcp_project_id=gcp_project_id)
    return metrics


def run() -> None:
    # Round-trip conversion to validate input
    today = args.date.strftime("%Y-%m-%d")
    logger.info("Running for {}", today)

    github_name = args.name
    project_name = github_name.split("/")[-1]
    pypi_name = args.pypi_package_name or project_name.lower()
    gcp_project_id = get_gcp_project_id(args)

    db_writer: DatabaseWriter | None = None
    if args.supabase:
        logger.info(f"Enabling logging to Supabase in project {project_name}")
        db_writer = SupabaseWriter(project_name)
    elif args.bigquery and gcp_project_id:
        logger.info(f"Enabling logging to BigQuery in project {gcp_project_id}")
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

    logger.info("Data output dir: {}", datadir)
    logger.info("Combined output dir: {}", combined_data_dir)
    logger.info("Plots output dir: {}", plotdir)

    all_metrics: list[MetricsProvider] = []

    if args.github:
        logger.info("Collecting GitHub metrics")
        github = github_metrics(github_name)
        all_metrics.append(github)

        if plotdir:
            plot_github_metrics(github, plotdir)

    if args.pypi:
        logger.info("Collecting PyPI metrics")
        pypi = pypi_metrics(pypi_name, gcp_project_id=gcp_project_id)
        all_metrics.append(pypi)

        if plotdir:
            plot_pypi_metrics(pypi, plotdir)

    # Save raw data
    for metrics in all_metrics:
        logger.info(f"Dumping {metrics.name}")

        metrics.dump()
        metrics.dump_raw_data(datadir)

        if combined_data_dir:
            metrics.dump_raw_data(combined_data_dir)

        if db_writer:
            logger.info(f"Writing {metrics.name} to {db_writer.name}")
            metrics.write(db_writer)

    if args.bigquery and db_writer:
        if not isinstance(db_writer, BigQueryWriter):
            logger.warning("BigQuery logging enabled but not using BigQueryWriter")
        else:
            if not gcp_project_id:
                logger.warning(
                    "BigQuery logging enabled but GCP project ID not provided. "
                    "Looker Studio report creation URL will not be generated."
                )
            else:
                report_url = create_lookerstudio_report_url(
                    DEFAULT_LOOKERSTUDIO_TEMPLATE_REPORT_ID,
                    project_name=project_name,
                    gcp_project_id=gcp_project_id,
                    bq_dataset=db_writer._dataset_ref.dataset_id,
                )
                logger.info(f"Looker Studio report creation URL: {report_url}")


if __name__ == "__main__":
    run()
