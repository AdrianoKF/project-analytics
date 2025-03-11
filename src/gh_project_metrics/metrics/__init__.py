import functools
import inspect
import sys
from abc import abstractmethod
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from typing import Any, NamedTuple, Self, TextIO, TypeAlias

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
    """Marks a function as a metrics function with caching support."""

    cache_attr = f"_{fn.__name__}_cache"

    def wrapper(self, *args):
        # Initialize the cache if it doesn't exist
        if not hasattr(self, cache_attr):
            setattr(self, cache_attr, {})
        cache = getattr(self, cache_attr)

        # Return cached result if available
        if args in cache:
            return cache[args]

        # Compute and store the result in the cache
        result = fn(self, *args)
        cache[args] = result
        return result

    functools.update_wrapper(wrapper, fn)
    wrapper.__annotations__["is_metric"] = True

    return wrapper


def _header(title: str) -> str:
    return f"------------------ [{title.upper():^20}] ------------------"


class MetricsProvider(Iterable[Metric]):
    @classmethod
    @abstractmethod
    def from_raw_data(cls, data_dir: Path, *init_args) -> Self:
        """Instantiate a MetricsProvider instance from raw data."""
        pass

    def __iter__(self) -> Iterator[Metric]:
        def _is_metric(item: Any) -> bool:
            return callable(item) and "is_metric" in inspect.get_annotations(item)

        metrics: list[MetricDefinition] = []
        for name, metrics_fn in inspect.getmembers(self, _is_metric):
            metrics.append(MetricDefinition(name, metrics_fn))

        return (Metric(m.name, m.fn()) for m in metrics)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def materialize(self) -> None:
        """Compute all metrics in the provider and store them in the cache."""
        # Iterate over all metrics to compute them
        for _ in self:
            pass

    def dump(self, dest: TextIO = sys.stdout) -> None:
        """Write all metrics from this provider into a stream (stdout by default)."""
        for m in self:
            print(_header(m.name), file=dest)
            print(m.data, file=dest)

    @abstractmethod
    def dump_raw_data(self, outdir: Path) -> None:
        """Dump raw data to a directory."""
        pass

    def write(self, db: DatabaseWriter) -> None:
        for m in self:
            db.write(m.data, m.name)

    def __getstate__(self):
        # Serialize the object, including cache states
        state = self.__dict__.copy()

        # Extract all cache attributes
        caches = {
            key: value
            for key, value in state.items()
            if key.startswith("_") and key.endswith("_cache")
        }
        state["caches"] = caches

        # Remove the actual cache attributes from the dictionary
        for key in caches:
            del state[key]

        return state

    def __setstate__(self, state):
        # Restore the object state, including caches
        self.__dict__.update(state)
        for key, cache in state.get("caches", {}).items():
            setattr(self, key, cache)
