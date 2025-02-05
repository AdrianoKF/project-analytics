from datetime import datetime

import plotly.express as px
import plotly.graph_objects as go

from gh_project_metrics.metrics.pypi import PyPIMetrics
from gh_project_metrics.plots import Plotter
from gh_project_metrics.plotting import (
    PLOT_TEMPLATE,
    add_weekends,
    format_plot,
    order_version_legend,
)


class PyPIDownloadsPerVersion(Plotter[PyPIMetrics]):
    """Line plot of PyPI downloads per version over time"""

    @property
    def name(self):
        return "downloads"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        downloads = self._metrics.downloads()

        # Resample to daily data, filling missing values with zero
        downloads = downloads.groupby("version").resample("D", level="date").sum().fillna(0)
        plot_df = downloads.loc[(slice(None), slice(start_date, end_date)), :].reset_index()
        cat_order = {"version": order_version_legend(plot_df["version"], reverse=True)}

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

        return fig


class PyPIVersionsTreemap(Plotter[PyPIMetrics]):
    """Treemap of PyPI downloads per version"""

    @property
    def name(self):
        return "treemap"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        downloads = self._metrics.downloads()

        # Resample to daily data, filling missing values with zero
        downloads = downloads.groupby("version").resample("D", level="date").sum().fillna(0)
        plot_df = downloads.loc[(slice(None), slice(start_date, end_date)), :].reset_index()

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
        return treemap
