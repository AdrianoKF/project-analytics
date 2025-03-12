import pickle
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Self

import pandas as pd

from gh_project_metrics.db import DatabaseWriter
from gh_project_metrics.util import combine_csv


class BaseMetric(ABC):
    def __init__(self, name: str | None = None) -> None:
        self.name = name or self.__class__.__name__
        self._cache: dict[tuple, pd.DataFrame] = {}

    def __call__(self, *args) -> pd.DataFrame:
        return self.compute(*args)

    def compute(self, *args) -> pd.DataFrame:
        # Return cached result if available
        if args in self._cache:
            return self._cache[args]
        result = self._compute(*args)
        self._cache[args] = result
        return result

    @abstractmethod
    def _compute(self, *args) -> pd.DataFrame:
        pass

    def dump_raw(self, outdir: Path) -> None:
        """Dump the metric data."""
        df = self.compute()
        # FIXME: kwargs
        combine_csv(df, outdir / f"{self.name}.csv")
        df.to_csv(outdir / f"{self.name}.csv", index=True)

    @classmethod
    @abstractmethod
    def load_raw(cls, name: str, indir: Path) -> Self:
        """Load from raw data and rebuild cache."""
        pass

    @classmethod
    def from_data(cls, name: str, data: pd.DataFrame) -> Self:
        """Create an instance from raw data."""
        instance = cls.__new__(cls)
        instance.name = name
        instance._cache = {(): data}
        return instance


class MetricsProviderMeta(type):
    def __new__(mcs, name, bases, namespace):
        cls = super().__new__(mcs, name, bases, namespace)

        # Skip automatic registration for the MetricsProvider base class
        if name == "MetricsProvider":
            return cls

        # Discover all metrics defined as class attributes
        metrics = {}
        annotations = namespace.get("__annotations__", {})
        for attr_name, attr_type in annotations.items():
            if (
                isinstance(attr_type, type)
                and issubclass(attr_type, BaseMetric)
                and attr_type is not BaseMetric
            ):
                metrics.update({attr_name: attr_type})

        # Inject the automatically discovered metrics.
        cls._metrics_types = metrics

        # Update the type annotations for IDE completion.
        updated_annotations = getattr(cls, "__annotations__", {}).copy()
        for metric_cls in metrics.values():
            updated_annotations[metric_cls.__name__] = metric_cls
        cls.__annotations__ = updated_annotations

        return cls


class MetricsProvider[TMetric: BaseMetric](metaclass=MetricsProviderMeta):
    _metrics_types: Mapping[str, type[TMetric]]

    def __init__(self, config) -> None:
        self._metrics: list[TMetric] = []
        self.config = config

    @classmethod
    def from_raw_data(cls, data_dir: Path) -> Self:
        """Instantiate a MetricsProvider instance from raw data."""

        config = pickle.loads((data_dir / "config.pkl").read_bytes())

        # HACK: Bypassing the constructor is ugly but required to prevent re-instantiation of the metrics
        instance = cls.__new__(cls)
        cls.config = config
        cls._metrics = []  # type: ignore[misc]

        for metric_name, metric_cls in cls._metrics_types.items():
            metric_instance = metric_cls.load_raw(metric_name, data_dir)
            instance._metrics.append(metric_instance)
            setattr(instance, metric_name, metric_instance)

        return instance

    def __iter__(self) -> Iterator[TMetric]:
        return iter(self._metrics)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def materialize(self) -> None:
        """Compute all metrics in the provider and store them in the cache."""
        for m in self:
            m.compute()

    def dump_raw_data(self, outdir: Path) -> None:
        (outdir / "config.pkl").write_bytes(pickle.dumps(self.config))
        for m in self:
            m.dump_raw(outdir)

    def write(self, db: DatabaseWriter) -> None:
        for m in self:
            db.write(m.compute(), m.name)
