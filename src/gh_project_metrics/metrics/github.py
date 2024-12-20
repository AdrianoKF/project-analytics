from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import pandas as pd
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

    def history(self) -> pd.DataFrame:
        stars = self.stars()
        clones = self.clones()
        views = self.views()

        merge_opts = {"how": "outer", "left_index": True, "right_index": True}
        df = pd.merge(clones, views, suffixes=["_clones", "_views"], **merge_opts)
        df = pd.merge(df, stars, suffixes=["", "_stars"], **merge_opts)

        return df
