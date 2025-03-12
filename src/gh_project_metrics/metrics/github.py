import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Self

import github as gh
import pandas as pd
from github.Repository import Repository

from gh_project_metrics.metrics import BaseMetric, MetricsProvider
from gh_project_metrics.util import TIMESTAMP_FORMAT


@dataclass(slots=True, frozen=True)
class MetricsConfig:
    repo: Repository | str
    aggregate_time: Literal["D", "W"]

    @property
    def gh_aggregate_period(self):
        match self.aggregate_time:
            case "D":
                return "day"
            case "W":
                return "week"


class GithubMetric(BaseMetric[MetricsConfig, "GithubMetrics"]):
    @property
    def repo(self) -> Repository:
        if isinstance(self.provider.config.repo, str):
            return gh.Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN")).get_repo(
                self.provider.config.repo
            )
        else:
            return self.provider.config.repo


class GithubViewsMetric(GithubMetric):
    def _compute(self, *args) -> pd.DataFrame:
        view_traffic = self.repo.get_views_traffic(self.provider.config.gh_aggregate_period)
        if not view_traffic:
            return pd.DataFrame()

        df = pd.DataFrame([
            {
                "date": view.timestamp.astimezone(UTC),
                "count": view.count,
                "unique": view.uniques,
            }
            for view in view_traffic["views"]
        ])
        return df.set_index("date")

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        df = pd.read_csv(indir / f"{name}.csv", index_col="date", date_format=TIMESTAMP_FORMAT)
        return cls.from_data(name, df)


class GithubStarsMetric(GithubMetric):
    def _compute(self, *args) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                {
                    "date": sg.starred_at.astimezone(UTC),
                    "user": sg.user.login,
                }
                for sg in self.repo.get_stargazers_with_dates()
            ],
        )
        df = df.set_index("date")
        stars_over_time = df.resample(self.provider.config.aggregate_time).count().cumsum()
        stars_over_time = stars_over_time.rename(columns={"user": "stars"})

        # Extend the index to now (otherwise, it will end at the time of the latest star)
        idx = stars_over_time.index
        idx = idx.union(pd.date_range(idx[-1], end=datetime.now(UTC), freq=idx.freq))
        idx = idx.set_names(*stars_over_time.index.names)
        # ... and fill forward the missing values
        stars_over_time = stars_over_time.reindex(idx, method="ffill")

        return stars_over_time

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        df = pd.read_csv(indir / f"{name}.csv", index_col="date", date_format=TIMESTAMP_FORMAT)
        return cls.from_data(name, df)


class GithubIssuesMetric(GithubMetric):
    def _compute(self, *args) -> pd.DataFrame:
        issues = self.repo.get_issues(state="all")

        rows = []
        collaborators = {c.login for c in self.repo.get_collaborators()}
        for issue in issues:
            row = {
                "id": issue.number,
                "title": issue.title,
                "created_at": issue.created_at or pd.NaT,
                "closed_at": issue.closed_at or pd.NaT,
                "updated_at": issue.updated_at or pd.NaT,
                "author": issue.user.login,
                "labels": {label.name for label in issue.labels},
                "status": "closed" if issue.state == "closed" else "open",
                "comments_count": issue.comments,
                "assignees": {assignee.login for assignee in issue.assignees},
                "has_pull_request": issue.pull_request is not None,
                "resolution_time": issue.closed_at - issue.created_at
                if not pd.isna(issue.closed_at)
                else pd.NaT,
                "external_contributor": issue.user.login not in collaborators,
            }
            rows.append(row)

        issues_df = pd.DataFrame(
            rows,
            columns={
                "id": pd.Series(dtype="int"),  # or dtype="string" if IDs are alphanumeric
                "title": pd.Series(dtype="string"),
                "created_at": pd.Series(dtype="datetime64[ns]"),
                "closed_at": pd.Series(dtype="datetime64[ns]"),
                "updated_at": pd.Series(dtype="datetime64[ns]"),
                "author": pd.Series(dtype="string"),
                "labels": pd.Series(dtype="object"),  # Using 'object' for lists of strings
                "status": pd.Series(dtype="string"),  # e.g., "open" or "closed"
                "comments_count": pd.Series(dtype="int"),
                "assignees": pd.Series(dtype="object"),  # Using 'object' for lists of strings
                "has_pull_request": pd.Series(dtype="bool"),
                "resolution_time": pd.Series(dtype="timedelta64[ns]"),
                "external_contributor": pd.Series(
                    dtype="bool"
                ),  # True if author is not a collaborator
            },
        )

        # Derived columns

        # Issue age is the time between issue creation and now for open issues,
        # or between creation and closing for closed issues
        issues_df["age"] = issues_df["resolution_time"]
        issues_df.loc[issues_df["status"] == "open", "age"] = (
            pd.Timestamp.now(tz=issues_df["created_at"].dt.tz) - issues_df["created_at"]
        )

        return issues_df.set_index("id").sort_index()

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        df = pd.read_csv(
            indir / f"{name}.csv",
            index_col="id",
            date_format=TIMESTAMP_FORMAT,
        )
        df["age"] = pd.to_timedelta(df["age"])
        return cls.from_data(name, df)


class GithubReferrersMetric(GithubMetric):
    def _compute(self, *args) -> pd.DataFrame:
        df = pd.DataFrame([r.raw_data for r in self.repo.get_top_referrers() or []])
        return df.set_index("referrer")

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        df = pd.read_csv(indir / f"{name}.csv", index_col="referrer")
        return cls.from_data(name, df)


class GithubClonesMetric(GithubMetric):
    def _compute(self, *args) -> pd.DataFrame:
        clone_traffic = self.repo.get_clones_traffic(self.provider.config.gh_aggregate_period)
        if not clone_traffic:
            return pd.DataFrame()

        df = pd.DataFrame(
            [
                {
                    "date": view.timestamp.astimezone(UTC),
                    "count": view.count,
                    "unique": view.uniques,
                }
                for view in clone_traffic["clones"]
            ],
        )
        return df.set_index("date")

    @classmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        df = pd.read_csv(indir / f"{name}.csv", index_col="date", date_format=TIMESTAMP_FORMAT)
        return cls.from_data(name, df)


class GithubMetrics(MetricsProvider[MetricsConfig, GithubMetric]):
    stars: GithubStarsMetric
    views: GithubViewsMetric
    clones: GithubClonesMetric
    issues: GithubIssuesMetric
    referrers: GithubReferrersMetric

    @property
    def repo(self) -> Repository:
        if isinstance(self.config.repo, str):
            return gh.Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN")).get_repo(
                self.config.repo
            )
        else:
            return self.config.repo

    @property
    def name(self) -> str:
        return f"GitHub ({self.repo.full_name})"
