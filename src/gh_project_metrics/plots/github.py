from datetime import datetime

import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots

from gh_project_metrics.metrics.github import GithubMetrics
from gh_project_metrics.plots import Plotter
from gh_project_metrics.plotting import (
    PLOT_TEMPLATE,
    add_weekends,
    format_plot,
)


class GithubStars(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "stars"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        stars = self._metrics.stars().loc[start_date:end_date]  # type: ignore[misc]

        # Resample to daily data, filling missing values with zero
        stars = stars.resample("D").max().fillna(0)

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
        return fig


class GithubViews(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "views"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        views = self._metrics.views().loc[start_date:end_date]  # type: ignore[misc]

        # Resample to daily data, filling missing values with zero
        views = views.resample("D").sum().fillna(0)

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
        return fig


class GithubClones(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "clones"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        clones = self._metrics.clones().loc[start_date:end_date]  # type: ignore[misc]

        # Resample to daily data, filling missing values with zero
        clones = clones.resample("D").sum().fillna(0)

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
        return fig


class GithubReferrers(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "referrers"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        # Tabulate and plot referrers
        referrers = self._metrics.referrers().reset_index()
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
        return fig


class GithubIssuesByStatus(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "issues_by_status"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        issues = self._metrics.issues().reset_index()
        fig = px.pie(
            issues,
            names="status",
            title="GitHub Issues by status",
            template=PLOT_TEMPLATE,
        )
        return fig


class GithubIssuesByExternalContributor(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "issues_by_external_contributor"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        issues = self._metrics.issues().reset_index()
        fig = px.pie(
            issues,
            names="external_contributor",
            title="GitHub Issues by external contributor",
            template=PLOT_TEMPLATE,
        )
        return fig


class GithubIssuesByLabel(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "issues_labels"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        issues = self._metrics.issues().reset_index()
        df = issues.loc[:, ["status", "labels"]].explode("labels").groupby("status")
        labels = df.value_counts().reset_index()
        labels.columns = ["status", "label", "count"]
        fig = px.treemap(
            labels,
            path=["status", "label"],
            values="count",
            title="GitHub Issues: Distribution by labels and status",
            template=PLOT_TEMPLATE,
        )
        return fig


class GithubIssuesAge(Plotter[GithubMetrics]):
    @property
    def name(self) -> str:
        return "issues_age"

    def plot(self, start_date: datetime, end_date: datetime) -> go.Figure:
        issues = self._metrics.issues().reset_index()
        fig = plotly.subplots.make_subplots(
            rows=1,
            cols=2,
            specs=[[{"type": "histogram"}, {"type": "table"}]],
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
        issue_age_df["age"] = issue_age_df["age"].astype(str)
        tbl = go.Table(
            header={"values": issue_age_df.columns},
            cells={"values": [issue_age_df[k].tolist() for k in issue_age_df.columns]},
        )
        fig.add_trace(tbl, row=1, col=2)
        fig.update_layout(title="GitHub Issues: Median age", template=PLOT_TEMPLATE)
        return fig
