import plotly.express as px
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
from gh_project_metrics.dagster.utils import parse_partition_key
from gh_project_metrics.metrics.github import GithubMetrics
from gh_project_metrics.metrics.pypi import PyPIMetrics
from gh_project_metrics.plotting import PLOT_TEMPLATE, add_weekends, format_plot

project_partitions_def = StaticPartitionsDefinition([
    "aai-institute/lakefs-spec",
    "aai-institute/nnbench",
])
date_partition_def = DailyPartitionsDefinition(start_date="2024-12-15")


@asset(
    description="PyPI download counts for a package",
    partitions_def=MultiPartitionsDefinition({
        "date": date_partition_def,
        "project": project_partitions_def,
    }),
    kinds={"python", "pypi"},
)
def pypi_metrics(
    context: AssetExecutionContext,
    gcp: GoogleCloud,
) -> PyPIMetrics:
    partition = parse_partition_key(context.partition_key)

    package = partition.project.split("/")[-1].lower()

    context.log.info(f"Creating PyPI metrics for {package}, {partition.date}")

    return steps.pypi_metrics(package, gcp_project_id=gcp.project_id)


@asset(
    description="GitHub metrics for a project",
    partitions_def=MultiPartitionsDefinition({
        "date": date_partition_def,
        "project": project_partitions_def,
    }),
    kinds={"python", "github"},
)
def github_metrics(context: AssetExecutionContext) -> GithubMetrics:
    partition = parse_partition_key(context.partition_key)

    context.log.info(f"Creating GitHub metrics for {partition.project}, {partition.date}")
    return steps.github_metrics(partition.project)


@asset(
    partitions_def=MultiPartitionsDefinition({
        "date": date_partition_def,
        "project": project_partitions_def,
    }),
    kinds={"python", "github", "plotly"},
    io_manager_key="plotly_io_manager",
)
def github_plots(
    context: AssetExecutionContext, github_metrics: GithubMetrics
) -> dict[str, go.Figure]:
    context.log.info(f"{github_metrics.repo=}")
    stars = github_metrics.stars()
    views = github_metrics.views()
    clones = github_metrics.clones()

    plots: dict[str, go.Figure] = {}

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
    plots["stars"] = fig

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
    plots["clones"] = fig

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
    plots["views"] = fig

    return plots


@asset(
    partitions_def=MultiPartitionsDefinition({
        "date": date_partition_def,
        "project": project_partitions_def,
    }),
    kinds={"python", "pypi", "plotly"},
    io_manager_key="plotly_io_manager",
)
def pypi_plots(context: AssetExecutionContext, pypi_metrics: PyPIMetrics) -> dict[str, go.Figure]:
    context.log.info(f"{pypi_metrics.package_name}, {pypi_metrics.gcp_project_id=}")
    downloads = pypi_metrics.downloads()

    plots: dict[str, go.Figure] = {}

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
    plots["downloads"] = fig

    return plots
