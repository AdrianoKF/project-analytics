import io
from pathlib import Path

import pandas as pd
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
from gh_project_metrics.dagster.utils import parse_partition_key, plot_date_range
from gh_project_metrics.metrics.github import GithubMetrics
from gh_project_metrics.metrics.pypi import PyPIMetrics
from gh_project_metrics.plotting import (
    PLOT_TEMPLATE,
    add_weekends,
    format_plot,
    order_version_legend,
)

project_partitions_def = StaticPartitionsDefinition([
    "aai-institute/lakefs-spec",
    "aai-institute/nnbench",
    "aai-institute/pyDVL",
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
    partitions_def=partitions_def,
    kinds={"python", "github", "plotly"},
    io_manager_key="plotly_io_manager",
)
def github_plots(
    context: AssetExecutionContext, github_metrics: GithubMetrics
) -> dict[str, go.Figure]:
    partition = parse_partition_key(context.partition_key)

    # Plot data starting from the beginning of the month
    start_date, end_date = plot_date_range(partition)

    stars = github_metrics.stars().loc[start_date:end_date]  # type: ignore[misc]
    views = github_metrics.views().loc[start_date:end_date]  # type: ignore[misc]
    clones = github_metrics.clones().loc[start_date:end_date]  # type: ignore[misc]

    # Resample to daily data, filling missing values with zero
    stars = stars.resample("D").max().fillna(0)
    views = views.resample("D").sum().fillna(0)
    clones = clones.resample("D").sum().fillna(0)

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

    # Issues by status
    issues = github_metrics.issues().reset_index()
    fig = px.pie(
        issues,
        names="status",
        title="GitHub Issues by status",
        template=PLOT_TEMPLATE,
    )
    plots["issues_by_status"] = fig

    # Issues: external contributions
    fig = px.pie(
        issues,
        names="external_contributor",
        title="GitHub Issues by external contributor",
        template=PLOT_TEMPLATE,
    )
    plots["issues_by_external_contributor"] = fig

    # Issues: Average age
    # Age is the time between issue creation and now for open issues,
    # or between creation and closing for closed issues

    fig = plotly.subplots.make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "histogram"}, {"type": "table"}]],
    )
    issues["age"] = issues["resolution_time"]
    issues.loc[issues["status"] == "open", "age"] = (
        pd.Timestamp.now(tz=issues["created_at"].dt.tz) - issues["created_at"]
    )

    hist_df = issues.copy()
    hist_df["age"] = hist_df["age"].dt.days
    hist = px.histogram(
        hist_df,
        x="age",
        color="status",
        title="GitHub Issues: Average age",
        template=PLOT_TEMPLATE,
    )
    for trace in hist.data:
        fig.add_trace(trace, row=1, col=1)
        fig.update_xaxes(title_text="Age (days)", row=1, col=1)

    issue_age_df = issues.groupby("status")["age"].median().reset_index()
    # Format median age as human-readable string
    issue_age_df["age"] = issue_age_df["age"]
    tbl = go.Table(
        header={"values": issue_age_df.columns},
        cells={"values": [issue_age_df[k].tolist() for k in issue_age_df.columns]},
    )
    fig.add_trace(tbl, row=1, col=2)
    fig.update_layout(title="GitHub Issues: Median age", template=PLOT_TEMPLATE)
    plots["issues_age"] = fig

    # Issues: Distribution by labels and status
    df = issues.loc[:, ["status", "labels"]].explode("labels").groupby("status")
    for name, group in df:
        context.log.info(f"Group: {name}, colums: {group.columns}")
        context.log.info(group.value_counts("labels"))
    labels = df.value_counts().reset_index()
    labels.columns = ["status", "label", "count"]
    fig = px.treemap(
        labels,
        path=["status", "label"],
        values="count",
        title="GitHub Issues: Distribution by labels and status",
        template=PLOT_TEMPLATE,
    )
    plots["issues_labels"] = fig

    return plots


@asset(
    partitions_def=partitions_def,
    kinds={"python", "pypi", "plotly"},
    io_manager_key="plotly_io_manager",
)
def pypi_plots(context: AssetExecutionContext, pypi_metrics: PyPIMetrics) -> dict[str, go.Figure]:
    partition = parse_partition_key(context.partition_key)

    # Plot data starting from the beginning of the month
    start_date, end_date = plot_date_range(partition)

    downloads = pypi_metrics.downloads()

    # Resample to daily data, filling missing values with zero
    downloads = downloads.groupby("version").resample("D", level="date").sum().fillna(0)
    plot_df = downloads.loc[(slice(None), slice(start_date, end_date)), :].reset_index()

    plots: dict[str, go.Figure] = {}

    cat_order = {"version": order_version_legend(plot_df["version"], reverse=True)}
    context.log.info(f"Category order: {cat_order}")
    fig = px.line(
        plot_df,
        x="date",
        y="num_downloads",
        color="version",
        markers=True,
        title="PyPI Downloads",
        template=PLOT_TEMPLATE,
        category_orders=cat_order,
    )
    # Overlay rectangles for weekends
    add_weekends(fig, start=plot_df["date"].min(), end=plot_df["date"].max())
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
