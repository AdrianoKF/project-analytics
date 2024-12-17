from pathlib import Path

from dagster import Definitions, load_assets_from_modules

from gh_project_metrics.dagster import assets, io_managers  # noqa: TID252

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "plotly_io_manager": io_managers.PlotlyIOManager(plotdir=str(Path.cwd() / "plots")),
    },
)
