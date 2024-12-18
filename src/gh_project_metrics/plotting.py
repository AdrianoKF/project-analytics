from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from gh_project_metrics.metrics.github import GithubMetrics
from gh_project_metrics.metrics.pypi import PyPIMetrics

# Default Plotly template name - see https://plotly.com/python/templates/
PLOT_TEMPLATE = "plotly_white"


def _plot_path(name: str, plotdir: Path) -> Path:
    return plotdir / f"{name}.png"


def write_plot(
    fig: go.Figure, plotdir: Path, name: str, width: int = 1200, height: int = 800
) -> Path:
    image_path = _plot_path(name, plotdir)
    image = fig.to_image(
        format=image_path.suffix.strip("."),
        width=width,
        height=height,
    )
    image_path.write_bytes(image)

    plot_json = fig.to_json()
    image_path.with_suffix(".json").write_text(plot_json, encoding="utf8")

    return image_path


def add_weekends(
    fig: go.Figure,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    bar_height: int | None = None,
    color: str = "rgba(99, 110, 250, 0.1)",
) -> go.Figure:
    """Overlay weekends for a given time range to a plot

    Parameters
    ----------
    fig: go.Figure
        Plotly ``Figure`` instance containing the base plot
    start: pd.Timestamp
        Start of the time range for which to plot weekends
    end: pd.Timestamp
        End of the time range for which to plot weekends
    bar_height: int | None
        Height of the overlay bars in Y units of the base plot, automatically inferred by default
    color: str
        Fill color for the overlay bars
    """

    # All days covered by the index
    dates = pd.DataFrame({"date": pd.date_range(start=start, end=end, freq="D")})
    dates["weekend"] = np.where((dates.date.dt.weekday == 5) | (dates.date.dt.weekday == 6), 1, 0)

    # Save base plot y axis range for proper scaling later
    yrange = fig.full_figure_for_development(warn=False).layout.yaxis.range
    if bar_height is None:
        bar_height = yrange[1]

    fig.add_trace(
        go.Bar(
            x=dates["date"],
            y=dates["weekend"] * bar_height,
            marker={"color": color, "line": {"width": 0}},
            width=86400000,  # 1 day in milliseconds
            showlegend=False,
        ),
    )
    # Restore old y axis range to prevent resizing based on the weekend overlay
    return fig.update_yaxes(range=yrange)


def format_plot(fig: go.Figure) -> go.Figure:
    """Apply default formatting to a time-series plot"""
    fig.update_xaxes(
        minor={
            "dtick": "D1",
            "ticks": "inside",
            "ticklen": 5,
        },
        ticks="inside",
        ticklen=5,
        tickformat="%a, %b %-d\n%Y",
    )
    return fig.update_layout(
        yaxis_title=None,
        yaxis_showticklabels=None,
        xaxis_title=None,
        xaxis_showgrid=False,
    )


def plot_github_metrics(metrics: GithubMetrics, plotdir: Path) -> None:
    stars = metrics.stars()
    views = metrics.views()
    clones = metrics.clones()

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


def plot_pypi_metrics(metrics: PyPIMetrics, plotdir: Path) -> None:
    downloads = metrics.downloads()

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
