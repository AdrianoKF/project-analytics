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
from gh_project_metrics.dagster import assets, io_managers, resources  # noqa: TID252

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
        "plotly_io_manager": io_managers.PlotlyIOManager(base_path=storage_base_dir),
        "report_io_manager": io_managers.CustomPathRawFileIOManager(
            base_path=storage_base_dir, extension=".html"
        ),
        "gcp": resources.GoogleCloud(project_id=EnvVar("GOOGLE_CLOUD_PROJECT")),
    },
    schedules=[nightly_schedule],
)
