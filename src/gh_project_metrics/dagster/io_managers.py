from pathlib import Path

import plotly.graph_objects as go
from dagster import ConfigurableIOManager

from gh_project_metrics.dagster.utils import parse_partition_key
from gh_project_metrics.plotting import write_plot


class PlotlyIOManager(ConfigurableIOManager):
    plotdir: str  # Path to the directory where the PNG images will be saved
    width: int = 1200  # Width of the PNG images in pixels
    height: int = 800  # Height of the PNG images in pixels

    def load_input(self, context):
        raise NotImplementedError("Loading not supported")

    def handle_output(self, context, obj: dict[str, go.Figure]):
        if not isinstance(obj, dict):
            raise ValueError("Output must be a dictionary of plot names to plotly figures")

        context.log.info(f"I/O manager partition key: {context.partition_key}")

        partition = parse_partition_key(context.partition_key)

        pdir = Path(self.plotdir) / partition.project / partition.start_date.strftime("%Y-%m-%d")
        pdir.mkdir(parents=True, exist_ok=True)

        for name, plot in obj.items():
            write_plot(plot, pdir, name, width=self.width, height=self.height)
