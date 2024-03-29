from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go

# Default Plotly template name - see https://plotly.com/python/templates/
PLOT_TEMPLATE = "plotly_white"


def _plot_path(name: str, plotdir: Path) -> Path:
    return plotdir / f"{name}.png"


def write_plot(fig: go.Figure, plotdir: Path, name: str) -> Path:
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


def add_weekends(
    fig: go.Figure,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    bar_height: int | None = None,
    fill: str = "tozeroy",
    fillcolor: str = "rgba(99, 110, 250, 0.1)",
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
    fill: str
        Plotly ``Scatter` fill mode for the overlay bars
    fillcolor: str
        Plotly ``Scatter` fill color for the overlay bars
    """

    # All days covered by the index
    dates = pd.DataFrame({"date": pd.date_range(start=start, end=end, freq="D")})
    dates["weekend"] = np.where((dates.date.dt.weekday == 5) | (dates.date.dt.weekday == 6), 1, 0)

    # Save base plot y axis range for proper scaling later
    yrange = fig.full_figure_for_development(warn=False).layout.yaxis.range
    if bar_height is None:
        bar_height = yrange[1]

    fig.add_trace(
        go.Scatter(
            x=dates["date"],
            y=dates["weekend"] * bar_height,
            fill=fill,
            fillcolor=fillcolor,
            line_shape="hv",
            line_color="rgba(0,0,0,0)",
            showlegend=False,
        ),
    )
    # Restore old y axis range to prevent resizing based on the weekend overlay
    return fig.update_yaxes(range=yrange)


def format_plot(fig: go.Figure) -> go.Figure:
    """Apply default formatting to a time-series plot"""
    return fig.update_layout(
        yaxis_title=None,
        yaxis_showticklabels=None,
        xaxis_title=None,
        xaxis_showgrid=False,
        xaxis_tickformat="%a, %b %-d\n%Y",
    )
