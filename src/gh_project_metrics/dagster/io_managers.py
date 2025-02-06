from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import plotly.io as pio
from upath import UPath

from dagster import (
    InputContext,
    OutputContext,
    UPathIOManager,
)
from gh_project_metrics.dagster.utils import parse_partition_key
from gh_project_metrics.metrics import MetricsProvider
from gh_project_metrics.plotting import write_plot


class PlotlyIOManager(UPathIOManager):
    """A Dagster I/O manager that writes Plotly figures to PNG images.

    Expects a dict of names to Plotly figures as input and writes them to PNG images,
    in a folder structure determined by the partitioning scheme of the asset."""

    base_dir: str  # Path to the directory where the PNG images will be saved
    width: int = 1200  # Width of the PNG images in pixels
    height: int = 800  # Height of the PNG images in pixels

    def _partition_path(self, context: OutputContext) -> Path:
        partition = parse_partition_key(context.partition_key)
        return (
            Path(self.base_dir)
            / partition.project
            / partition.date.strftime("%Y-%m-%d")
            / Path(*context.asset_key.path)
        )

    def load_from_path(self, context: InputContext, path: UPath) -> dict[str, go.Figure]:
        plots = {}
        for plot_file in path.glob("*.json"):
            name = plot_file.stem
            plots[name] = pio.from_json(plot_file.read_text())
        return plots

    def dump_to_path(self, context, obj: dict[str, go.Figure], path: UPath):
        path.mkdir(parents=True, exist_ok=True)
        for name, plot in obj.items():
            write_plot(plot, path, name, width=self.width, height=self.height)

    def get_path_for_partition(
        self, context: InputContext | OutputContext, path: UPath, partition: str
    ):
        partition_parts = parse_partition_key(context.partition_key)
        return path / partition_parts.project / partition_parts.date.strftime("%Y-%m-%d")


class CustomPathRawFileIOManager(UPathIOManager):
    """A custom I/O manager that writes raw files to a custom path.

    Expects a dict of names to raw files as input and writes them to the specified path,
    in a folder structure determined by the partitioning scheme of the asset.

    The default name for the file is the key of the asset, assets may configure the
    name by setting the `filename` metadata key."""

    def __init__(self, base_path=None, extension: str | None = None):
        super().__init__(base_path)
        self.extension = extension

    def load_from_path(self, context: InputContext, path: UPath) -> str:
        return path.read_text()

    def dump_to_path(self, context: OutputContext, obj: str, path: UPath):
        path.write_text(obj)

    def get_path_for_partition(
        self, context: InputContext | OutputContext, path: UPath, partition: str
    ):
        partition_parts = parse_partition_key(context.partition_key)
        return path / partition_parts.project / partition_parts.date.strftime("%Y-%m-%d")


class HistoryCSVIOManager(UPathIOManager):
    """History manager that automatically concatenates the outputs for a given asset with a combined historical dataset"""

    sort_kwargs: dict[str, Any] | None = None

    def __init__(self, base_path, sort_kwargs=None):
        super().__init__(base_path)
        self.sort_kwargs = sort_kwargs or {}

    def load_from_path(self, context, path) -> MetricsProvider | None:
        raise NotImplementedError("Loading from HistoryCSVIOManager is not supported")

    def dump_to_path(self, context, obj: MetricsProvider, path):
        if path.protocol in ["gs", "s3"]:
            # Create a placeholder file to ensure the directory is created
            (path.parent / ".keep").touch()
        else:
            path.mkdir(parents=True, exist_ok=True)
        obj.dump_raw_data(path)

    def get_path_for_partition(
        self, context: InputContext | OutputContext, path: UPath, partition: str
    ):
        partition_parts = parse_partition_key(context.partition_key)
        return path / partition_parts.project
