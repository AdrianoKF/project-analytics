import inspect
from collections.abc import Iterable
from typing import Callable, Iterator, NamedTuple

import pandas as pd

type MetricFn = Callable[..., pd.DataFrame]  # type: ignore
MetricDefinition = NamedTuple("MetricDefinition", [("name", str), ("fn", MetricFn)])
Metric = NamedTuple("Metric", [("name", str), ("data", pd.DataFrame)])


def metric(fn: MetricFn):
    """Marks a function as a metrics function"""
    fn.__annotations__["is_metric"] = True
    return fn


class MetricsProvider(Iterable[Metric]):
    def __init__(self):
        def _is_metric(item):
            return callable(item) and "is_metric" in inspect.get_annotations(item)

        metrics: list[MetricDefinition] = []
        for name, metrics_fn in inspect.getmembers(self, _is_metric):
            metrics.append(MetricDefinition(name, metrics_fn))

        self._metrics = metrics

    def __iter__(self) -> Iterator[Metric]:
        return (Metric(m.name, m.fn()) for m in self._metrics)
