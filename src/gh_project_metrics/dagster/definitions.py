import os

from upath import UPath

from dagster import (
    AssetSelection,
    DagsterInstance,
    Definitions,
    EnvVar,
    build_schedule_from_partitioned_job,
    define_asset_job,
    load_assets_from_modules,
)
from dagster._core.storage.fs_io_manager import PickledObjectFilesystemIOManager
from gh_project_metrics.dagster import assets, io_managers, resources  # noqa: TID252
from gh_project_metrics.metrics.github import GithubMetrics
from gh_project_metrics.metrics.pypi import PyPIMetrics

all_assets = load_assets_from_modules([assets])

nightly_job = define_asset_job("nightly", selection=AssetSelection.all())
nightly_schedule = build_schedule_from_partitioned_job(nightly_job, hour_of_day=3)

environment = "ci" if os.getenv("CI", False) else "local"

if environment == "local":
    storage_base_dir = UPath(DagsterInstance.get().storage_directory())
else:
    # FIXME: Hardcoded GCS bucket name
    storage_base_dir = UPath("gs://project-metrics-assets")

print(f"Using storage base directory: {storage_base_dir}")

defs = Definitions(
    assets=all_assets,
    resources={
        "io_manager": PickledObjectFilesystemIOManager(base_dir=str(storage_base_dir)),
        "plotly_io_manager": io_managers.PlotlyIOManager(base_path=storage_base_dir),
        "report_io_manager": io_managers.CustomPathRawFileIOManager(
            base_path=storage_base_dir, extension=".html"
        ),
        "pypi_history_io_manager": io_managers.HistoryCSVIOManager(
            metrics_cls=PyPIMetrics,
            base_path=storage_base_dir / "combined",
            sort_kwargs={"level": ["version", "date"], "ascending": [False, True]},
        ),
        "github_history_io_manager": io_managers.HistoryCSVIOManager(
            metrics_cls=GithubMetrics,
            base_path=storage_base_dir / "combined",
        ),
        "gcp": resources.GoogleCloud(project_id=EnvVar("GOOGLE_CLOUD_PROJECT")),
    },
    schedules=[nightly_schedule],
)
