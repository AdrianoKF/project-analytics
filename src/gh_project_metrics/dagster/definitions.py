import os
from pathlib import Path

from dagster import (
    AssetSelection,
    Definitions,
    build_schedule_from_partitioned_job,
    define_asset_job,
    load_assets_from_modules,
)

from gh_project_metrics.dagster import assets, io_managers, resources  # noqa: TID252

all_assets = load_assets_from_modules([assets])

nightly_job = define_asset_job("nightly", selection=AssetSelection.all())
nightly_schedule = build_schedule_from_partitioned_job(nightly_job, hour_of_day=3)

defs = Definitions(
    assets=all_assets,
    resources={
        "plotly_io_manager": io_managers.PlotlyIOManager(root_directory=str(Path.cwd() / "plots")),
        "gcp": resources.GoogleCloud(project_id=os.getenv("GOOGLE_CLOUD_PROJECT", None)),
    },
    schedules=[nightly_schedule],
)
