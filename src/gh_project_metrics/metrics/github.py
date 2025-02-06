import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import bs4
import pandas as pd
import requests
from github.Repository import Repository

from gh_project_metrics.metrics import MetricsProvider, metric
from gh_project_metrics.util import combine_csv


@dataclass(slots=True, frozen=True)
class MetricsConfig:
    aggregate_time: str


class GithubMetrics(MetricsProvider):
    def __init__(self, repo: Repository, config: MetricsConfig) -> None:
        self.repo = repo
        self.config = config

    @property
    def name(self) -> str:
        return f"GitHub ({self.repo.full_name})"

    @property
    def _github_api_period(self) -> Literal["day", "week"]:
        match self.config.aggregate_time:
            case "W":
                return "week"
            case "D":
                return "day"
            case val:
                raise ValueError(f"invalid time spec: {val!r}")

    def dump_raw_data(self, outdir: Path) -> None:
        if not outdir.is_dir():
            raise ValueError(f"not a directory: {outdir!r}")

        self.referrers().to_csv(outdir / "referrers.csv", index=True)
        combine_csv(self.views(), outdir / "views.csv")
        combine_csv(self.stars(), outdir / "stars.csv")
        combine_csv(self.clones(), outdir / "clones.csv")

    @metric
    def stars(self) -> pd.DataFrame:
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
        stars_over_time = df.resample(self.config.aggregate_time).count().cumsum()
        stars_over_time = stars_over_time.rename(columns={"user": "stars"})

        # Extend the index to now (otherwise, it will end at the time of the latest star)
        idx = stars_over_time.index
        idx = idx.union(pd.date_range(idx[-1], end=datetime.now(UTC), freq=idx.freq))
        idx = idx.set_names(*stars_over_time.index.names)
        # ... and fill forward the missing values
        stars_over_time = stars_over_time.reindex(idx, method="ffill")

        return stars_over_time

    @metric
    def views(self) -> pd.DataFrame:
        per = self._github_api_period
        view_traffic = self.repo.get_views_traffic(per)
        if not view_traffic:
            return pd.DataFrame()

        df = pd.DataFrame(
            [
                {
                    "date": view.timestamp.astimezone(UTC),
                    "count": view.count,
                    "unique": view.uniques,
                }
                for view in view_traffic["views"]
            ],
        )
        return df.set_index("date")

    @metric
    def clones(self) -> pd.DataFrame:
        per = self._github_api_period
        clone_traffic = self.repo.get_clones_traffic(per)
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

    @metric
    def referrers(self) -> pd.DataFrame:
        df = pd.DataFrame([r.raw_data for r in self.repo.get_top_referrers() or []])
        return df.set_index("referrer")

    @metric
    def issues(self) -> pd.DataFrame:
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

    @metric
    def dependents(self) -> pd.DataFrame:
        dependents = []
        more_pages = True

        url = f"https://github.com/{self.repo.full_name}/network/dependents"
        while more_pages:
            logging.debug(f"Fetching dependents from {url}")

            resp = requests.get(url)
            resp.raise_for_status()
            parser = bs4.BeautifulSoup(resp.text, "html.parser")

            root = parser.find("div", id="dependents")
            if root is None:
                return pd.DataFrame()

            for dep in root.find_all("div", attrs={"data-test-id": "dg-repo-pkg-dependent"}):
                target = dep.find("a", attrs={"data-hovercard-type": "repository"})
                owner = target["href"].split("/")[1]
                repo = target["href"].split("/")[2]

                # Replace separators in human-readable numbers
                stars = dep.find("svg", class_="octicon-star").parent.text.replace(",", "")
                forks = dep.find("svg", class_="octicon-repo-forked").parent.text.replace(",", "")

                dependents.append({
                    "full_name": f"{owner}/{repo}",
                    "owner": owner,
                    "repo": repo,
                    "stars": int(stars),
                    "forks": int(forks),
                })

            # Next page link with cursor pagination
            next_button = parser.find(
                lambda tag: tag.name == "a" and tag.text == "Next" and tag.get("href") is not None
            )
            if next_button is None:
                more_pages = False
            else:
                url = next_button["href"]

        return pd.DataFrame(dependents)

    def history(self) -> pd.DataFrame:
        stars = self.stars()
        clones = self.clones()
        views = self.views()

        merge_opts = {"how": "outer", "left_index": True, "right_index": True}
        df = pd.merge(clones, views, suffixes=["_clones", "_views"], **merge_opts)
        df = pd.merge(df, stars, suffixes=["", "_stars"], **merge_opts)

        return df
