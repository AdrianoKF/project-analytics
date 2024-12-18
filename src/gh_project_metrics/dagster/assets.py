import io
from pathlib import Path

import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
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

    context.log.info(f"Creating PyPI metrics for {package}, {partition.date}")

    metrics = steps.pypi_metrics(package, gcp_project_id=gcp.project_id)
    metrics.materialize()
    return metrics


@asset(
    description="GitHub metrics for a project",
    partitions_def=partitions_def,
    kinds={"python", "github"},
)
def github_metrics(context: AssetExecutionContext) -> GithubMetrics:
    partition = parse_partition_key(context.partition_key)

    context.log.info(f"Creating GitHub metrics for {partition.project}, {partition.date}")
    metrics = steps.github_metrics(partition.project)
    metrics.materialize()
    return metrics


@asset(
    partitions_def=partitions_def,
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

    # Tabulate and plot referrers
    referrers = github_metrics.referrers().reset_index()
    tbl = go.Table(
        header={"values": referrers.columns},
        cells={"values": [referrers[k].tolist() for k in referrers.columns]},
    )
    pie = px.pie(
        referrers,
        names="referrer",
        values="count",
    )

    fig = plotly.subplots.make_subplots(
        rows=1, cols=2, specs=[[{"type": "table"}, {"type": "pie"}]]
    )
    fig.add_trace(tbl, row=1, col=1)
    # px objects may consist of multiple traces, add each one separately
    pie.for_each_trace(lambda t: fig.add_trace(t, row=1, col=2))
    fig.update_layout(title="GitHub Referrers", template=PLOT_TEMPLATE)
    plots["referrers"] = fig

    return plots


@asset(
    partitions_def=partitions_def,
    kinds={"python", "pypi", "plotly"},
    io_manager_key="plotly_io_manager",
)
def pypi_plots(context: AssetExecutionContext, pypi_metrics: PyPIMetrics) -> dict[str, go.Figure]:
    context.log.info(f"{pypi_metrics.package_name}, {pypi_metrics.gcp_project_id=}")
    downloads = pypi_metrics.downloads()
    plot_df = downloads.reset_index()

    plots: dict[str, go.Figure] = {}

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

    # Treemap of downloads by version
    plot_df["major_version"] = plot_df["version"].str.split(".", n=1).str[0]
    plot_df["minor_version"] = plot_df["version"].str.rsplit(".", n=1).str[0]
    treemap = px.treemap(
        plot_df,
        path=["major_version", "minor_version", "version"],
        values="num_downloads",
        color="num_downloads",
        color_continuous_scale="Greens",
        title="PyPI Downloads by Version",
        template=PLOT_TEMPLATE,
    )
    treemap.update_traces(
        marker={"cornerradius": 5},
        textinfo="label+value+percent root",
    )
    plots["treemap"] = treemap

    return plots


@asset(
    partitions_def=partitions_def,
    kinds={"python", "html"},
)
def html_report(
    context: AssetExecutionContext,
    pypi_plots: dict[str, go.Figure],
    github_plots: dict[str, go.Figure],
) -> MaterializeResult:
    context.log.info(f"{pypi_plots=}, {github_plots=}")
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

    out_path = Path().cwd() / "reports" / partition.project / partition.date.strftime("%Y-%m-%d")
    out_path.mkdir(parents=True, exist_ok=True)
    out_path /= "report.html"

    out_path.write_text(
        html5_skeleton.format(
            project=partition.project,
            placeholder=snippets.getvalue(),
        )
    )
    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(out_path),
            "project": MetadataValue.text(partition.project),
        },
    )
