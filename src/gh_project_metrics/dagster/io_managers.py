from pathlib import Path

import plotly.graph_objects as go
import plotly.io as pio
from dagster import ConfigurableIOManager, OutputContext

from gh_project_metrics.dagster.utils import parse_partition_key
from gh_project_metrics.plotting import write_plot


class PlotlyIOManager(ConfigurableIOManager):
    """A Dagster I/O manager that writes Plotly figures to PNG images.

    Expects a dict of names to Plotly figures as input and writes them to PNG images,
    in a folder structure determined by the partitioning scheme of the asset."""

    root_directory: str  # Path to the directory where the PNG images will be saved
    width: int = 1200  # Width of the PNG images in pixels
    height: int = 800  # Height of the PNG images in pixels

    def _partition_path(self, context: OutputContext) -> Path:
        partition = parse_partition_key(context.partition_key)
        return (
            Path(self.root_directory)
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
