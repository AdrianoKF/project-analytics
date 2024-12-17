from pathlib import Path

import plotly.graph_objects as go
from dagster import ConfigurableIOManager

from gh_project_metrics.dagster.utils import parse_partition_key
from gh_project_metrics.plotting import write_plot


class PlotlyIOManager(ConfigurableIOManager):
    """A Dagster I/O manager that writes Plotly figures to PNG images.

    Expects a dict of names to Plotly figures as input and writes them to PNG images,
    in a folder structure determined by the partitioning scheme of the asset."""

    root_directory: str  # Path to the directory where the PNG images will be saved
    width: int = 1200  # Width of the PNG images in pixels
    height: int = 800  # Height of the PNG images in pixels

    def load_input(self, context):
        raise NotImplementedError("Loading not supported")

    def handle_output(self, context, obj: dict[str, go.Figure]):
        partition = parse_partition_key(context.partition_key)

        pdir = Path(self.root_directory) / partition.project / partition.date.strftime("%Y-%m-%d")
        pdir.mkdir(parents=True, exist_ok=True)

        for name, plot in obj.items():
            write_plot(plot, pdir, name, width=self.width, height=self.height)
