import os
from pathlib import Path

from dagster import Definitions, load_assets_from_modules

from gh_project_metrics.dagster import assets, io_managers, resources  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "plotly_io_manager": io_managers.PlotlyIOManager(root_directory=str(Path.cwd() / "plots")),
        "gcp": resources.GoogleCloud(project_id=os.getenv("GOOGLE_CLOUD_PROJECT", None)),
    },
)
