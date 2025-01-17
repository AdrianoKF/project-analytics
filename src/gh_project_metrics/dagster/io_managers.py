from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio

import dagster._check as check
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from gh_project_metrics.dagster.utils import parse_partition_key
from gh_project_metrics.plotting import write_plot


class PlotlyIOManager(ConfigurableIOManager):
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

    def load_input(self, context) -> dict[str, go.Figure]:
        pdir = self._partition_path(context)
        plots = {}
        for plot_file in pdir.glob("*.json"):
            name = plot_file.stem
            plots[name] = pio.from_json(plot_file.read_text())
        return plots

    def handle_output(self, context, obj: dict[str, go.Figure]):
        pdir = self._partition_path(context)
        pdir.mkdir(parents=True, exist_ok=True)
        for name, plot in obj.items():
            write_plot(plot, pdir, name, width=self.width, height=self.height)


class CustomPathRawFileIOManager(ConfigurableIOManager):
    """A custom I/O manager that writes raw files to a custom path.

    Expects a dict of names to raw files as input and writes them to the specified path,
    in a folder structure determined by the partitioning scheme of the asset.

    The default name for the file is the key of the asset, assets may configure the
    name by setting the `filename` metadata key."""

    base_dir: str  # Path to the directory where the raw files will be saved
    default_extension: str = "txt"  # Default extension for the raw files, unless set in metadata

    def _partition_path(self, context: InputContext | OutputContext) -> Path:
        partition = parse_partition_key(context.partition_key)

        filename = (context.definition_metadata or {}).get("filename")
        if not filename:
            filename = f"{str(Path(*context.asset_key.path))}.{self.default_extension}"

        context.log.info("Writing file to %s", filename)
        return (
            Path(self.base_dir)
            / partition.project
            / partition.date.strftime("%Y-%m-%d")
            / check.str_param(filename, "filename")
        )

    def load_input(self, context: InputContext | OutputContext) -> str:
        path = self._partition_path(context)
        return path.read_text()

    def handle_output(self, context: InputContext | OutputContext, obj: str):
        path = self._partition_path(context)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(obj)
