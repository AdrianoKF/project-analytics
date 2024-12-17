import inspect
import sys
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, NamedTuple, TextIO, TypeAlias

import pandas as pd

from gh_project_metrics.db import DatabaseWriter

MetricFn: TypeAlias = Callable[..., pd.DataFrame]


class MetricDefinition(NamedTuple):
    name: str
    fn: MetricFn


class Metric(NamedTuple):
    name: str
    data: pd.DataFrame


def metric(fn: MetricFn) -> MetricFn:
    """Marks a function as a metrics function"""
    fn.__annotations__["is_metric"] = True
    return fn


def _header(title: str) -> str:
    return f"------------------ [{title.upper():^20}] ------------------"


class MetricsProvider(Iterable[Metric]):
    def __init__(self) -> None:
        def _is_metric(item: Any) -> bool:
            return callable(item) and "is_metric" in inspect.get_annotations(item)

        metrics: list[MetricDefinition] = []
        for name, metrics_fn in inspect.getmembers(self, _is_metric):
            metrics.append(MetricDefinition(name, metrics_fn))

        self._metrics = metrics

    def __iter__(self) -> Iterator[Metric]:
        return (Metric(m.name, m.fn()) for m in self._metrics)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def dump(self, dest: TextIO = sys.stdout) -> None:
        """Write all metrics from this provider into a stream (stdout by default)."""
        for metric in self:
            print(_header(metric.name), file=dest)
            print(metric.data, file=dest)

    def dump_raw_data(self, outdir: Path) -> None: ...

    def write(self, db: DatabaseWriter) -> None:
        for metric in self:
            db.write(metric.data, metric.name)
