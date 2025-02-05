import abc
from datetime import datetime
from typing import Generic, TypeVar

import plotly.graph_objects as go

from gh_project_metrics.metrics import MetricsProvider

T = TypeVar("T", bound=MetricsProvider)


class Plotter(abc.ABC, Generic[T]):
    def __init__(self, metrics: T, plot_args: dict | None = None):
        self._metrics = metrics

        if plot_args is None:
            plot_args = {}
        self._plot_args = plot_args.copy()

    @abc.abstractmethod
    def plot(self, start_date: datetime, end_time: datetime) -> go.Figure:
        pass

    @property
    @abc.abstractmethod
    def name(self) -> str:
        pass
