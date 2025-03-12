import io

import plotly.graph_objects as go

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)
from gh_project_metrics import steps
from gh_project_metrics.dagster.resources import GoogleCloud
from gh_project_metrics.dagster.utils import parse_partition_key, plot_date_range
from gh_project_metrics.metrics.github import GithubMetrics
from gh_project_metrics.metrics.pypi import PyPIMetrics
from gh_project_metrics.plots import Plotter
from gh_project_metrics.plots import github as _gh_plots
from gh_project_metrics.plots import pypi as _pypi_plots

project_partitions_def = StaticPartitionsDefinition([
    "aai-institute/lakefs-spec",
    "aai-institute/nnbench",
    "aai-institute/pyDVL",
    "sbi-dev/sbi",
    "optimagic-dev/optimagic",
])
date_partition_def = DailyPartitionsDefinition(start_date="2024-12-15")
partitions_def = MultiPartitionsDefinition({
    "date": date_partition_def,
    "project": project_partitions_def,
})


@asset(
    description="PyPI download counts for a package",
    partitions_def=partitions_def,
    kinds={"python", "pypi"},
)
def pypi_metrics(
    context: AssetExecutionContext,
    gcp: GoogleCloud,
) -> PyPIMetrics:
    partition = parse_partition_key(context.partition_key)

    package = partition.project.split("/")[-1].lower()
    metrics = steps.pypi_metrics(package, gcp_project_id=gcp.project_id)
    metrics.materialize()
    return metrics


@asset(
    io_manager_key="pypi_history_io_manager",
    partitions_def=partitions_def,
)
def pypi_history(
    pypi_metrics: PyPIMetrics,
):
    """Historical PyPI data"""
    return pypi_metrics


@asset(
    description="GitHub metrics for a project",
    partitions_def=partitions_def,
    kinds={"python", "github"},
)
def github_metrics(context: AssetExecutionContext) -> GithubMetrics:
    partition = parse_partition_key(context.partition_key)

    metrics = steps.github_metrics(partition.project)
    metrics.materialize()
    return metrics


@asset(
    io_manager_key="github_history_io_manager",
    partitions_def=partitions_def,
)
def github_history(
    github_metrics: GithubMetrics,
):
    """Historical GitHub data"""
    return github_metrics


@asset(
    partitions_def=partitions_def,
    kinds={"python", "github", "plotly"},
    io_manager_key="plotly_io_manager",
)
def github_plots(
    context: AssetExecutionContext, github_history: GithubMetrics
) -> dict[str, go.Figure]:
    partition = parse_partition_key(context.partition_key)

    # Plot data starting from the beginning of the month
    start_date, end_date = plot_date_range(partition)

    plots: dict[str, go.Figure] = {}
    plotters: list[type[Plotter[GithubMetrics]]] = [
        _gh_plots.GithubStars,
        _gh_plots.GithubViews,
        _gh_plots.GithubClones,
        _gh_plots.GithubReferrers,
        _gh_plots.GithubIssuesByLabel,
        _gh_plots.GithubIssuesAge,
        _gh_plots.GithubIssuesByStatus,
        _gh_plots.GithubIssuesByExternalContributor,
    ]
    for cls in plotters:
        plotter = cls(github_history)
        fig = plotter.plot(start_date, end_date)
        plots[plotter.name] = fig

    return plots


@asset(
    partitions_def=partitions_def,
    kinds={"python", "pypi", "plotly"},
    io_manager_key="plotly_io_manager",
)
def pypi_plots(context: AssetExecutionContext, pypi_history: PyPIMetrics) -> dict[str, go.Figure]:
    partition = parse_partition_key(context.partition_key)

    # Plot data starting from the beginning of the month
    start_date, end_date = plot_date_range(partition)

    plots: dict[str, go.Figure] = {}
    plotters: list[type[Plotter[PyPIMetrics]]] = [
        _pypi_plots.PyPIDownloadsPerVersion,
        _pypi_plots.PyPIVersionsTreemap,
    ]
    for cls in plotters:
        plotter = cls(pypi_history)
        fig = plotter.plot(start_date, end_date)
        plots[plotter.name] = fig

    return plots


@asset(
    partitions_def=partitions_def,
    kinds={"python", "html"},
    io_manager_key="report_io_manager",
    metadata={
        "filename": "report.html",
    },
)
def html_report(
    context: AssetExecutionContext,
    pypi_plots: dict[str, go.Figure],
    github_plots: dict[str, go.Figure],
) -> str:
    partition = parse_partition_key(context.partition_key)

    html5_skeleton = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Project Metrics for {project}</title>
        <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
    </head>
    <body>
        {placeholder}
    </body>
    </html>
    """

    snippets = io.StringIO()
    for _, plot in pypi_plots.items():
        plot.write_html(snippets, full_html=False, include_plotlyjs=False)
    for _, plot in github_plots.items():
        plot.write_html(snippets, full_html=False, include_plotlyjs=False)

    html_report = html5_skeleton.format(
        project=partition.project,
        placeholder=snippets.getvalue(),
    )
    return html_report
