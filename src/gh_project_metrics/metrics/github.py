from dataclasses import dataclass
from functools import cache
from pathlib import Path

import pandas as pd
from github.Repository import Repository


@dataclass(slots=True, frozen=True)
class MetricsConfig:
    aggregate_time: str


class GithubMetrics:
    def __init__(self, repo: Repository, config: MetricsConfig) -> None:
        self.repo = repo
        self.config = config

    @property
    def _github_api_period(self):
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
        self.views().to_csv(outdir / "views.csv", index=True)
        self.stars().to_csv(outdir / "stars.csv", index=True)
        self.clones().to_csv(outdir / "clones.csv", index=True)

    @cache
    def stars(self) -> pd.DataFrame:
        df = pd.DataFrame(
            [
                {"date": sg.starred_at, "user": sg.user.login}
                for sg in self.repo.get_stargazers_with_dates()
            ],
        )
        df.set_index("date", inplace=True)
        stars_over_time = df.resample(self.config.aggregate_time).count().cumsum()
        stars_over_time.rename(columns={"user": "stars"}, inplace=True)
        return stars_over_time

    @cache
    def views(self) -> pd.DataFrame:
        per = self._github_api_period
        view_traffic = self.repo.get_views_traffic(per)
        df = pd.DataFrame(
            [
                {
                    "date": view.timestamp,
                    "count": view.count,
                    "unique": view.uniques,
                }
                for view in view_traffic["views"]
            ],
        )
        return df.set_index("date")

    @cache
    def clones(self) -> pd.DataFrame:
        per = self._github_api_period
        clone_traffic = self.repo.get_clones_traffic(per)
        df = pd.DataFrame(
            [
                {
                    "date": view.timestamp,
                    "count": view.count,
                    "unique": view.uniques,
                }
                for view in clone_traffic["clones"]
            ],
        )
        return df.set_index("date")

    @cache
    def referrers(self):
        df = pd.DataFrame([r.raw_data for r in self.repo.get_top_referrers()])
        return df.set_index("referrer")

    def history(self) -> pd.DataFrame:
        stars = self.stars()
        clones = self.clones()
        views = self.views()

        merge_opts = dict(how="outer", left_index=True, right_index=True)
        df = pd.merge(clones, views, suffixes=["_clones", "_views"], **merge_opts)
        df = pd.merge(df, stars, suffixes=["", "_stars"], **merge_opts)

        return df
