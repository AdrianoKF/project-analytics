import re
from pathlib import Path
from typing import Any

import pandas as pd

# Example: "2023-08-18 00:00:00+00:00"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S%z"


def combine(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    sort_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame:
    if type(df1.index) is not type(df2.index):  # noqa: E721
        raise ValueError(
            f"DataFrames must have equal base types. {type(df1.index)!r} != {type(df2.index)!r}"
        )
    if isinstance(df1.index, pd.MultiIndex) and isinstance(df2.index, pd.MultiIndex):
        if df1.index.names != df2.index.names:
            raise ValueError(
                f"DataFrames must have equally named indexes. {df1.index.names!r} != {df2.index.names!r}"
            )
        if any(df1.index.dtypes != df2.index.dtypes):
            raise ValueError(
                f"DataFrames must have equally typed indexes. {df1.index.dtypes!r} != {df2.index.dtypes!r}"
            )
    else:
        if df1.index.name != df2.index.name:
            raise ValueError(
                f"DataFrames must have equally named indexes. {df1.index.name!r} != {df2.index.name!r}"
            )
        if df1.index.dtype != df2.index.dtype:
            raise ValueError(
                f"DataFrames must have equally typed indexes. {df1.index.dtype!r} != {df2.index.dtype!r}"
            )
    if any(df1.columns != df2.columns):
        raise ValueError(
            f"DataFrames must have matching columns. {df1.columns!r} != {df2.columns!r}"
        )

    return pd.concat([df2, df1.loc[df1.index.difference(df2.index)]]).sort_index(
        **(sort_kwargs or {})
    )


def combine_csv(
    df: pd.DataFrame,
    outfile: Path,
    sort_kwargs: dict[str, Any] | None = None,
    read_kwargs: dict[str, Any] | None = None,
    write_kwargs: dict[str, Any] | None = None,
) -> None:
    """Write a DataFrame to a CSV file, combining with existing data, if any."""
    write_df = df.copy()
    if outfile.exists():
        old_df = pd.read_csv(
            outfile,
            date_format=TIMESTAMP_FORMAT,
            index_col=write_df.index.names,
            **(read_kwargs or {}),
        )
        write_df = combine(write_df, old_df, sort_kwargs=sort_kwargs)
    write_df.to_csv(outfile, index=True, **(write_kwargs or {}))


def sanitize_name(name: str) -> str:
    """A canonical, URL-safe representation of a project name

    Note, that this representation is similar to RFC 1123, but normalizes
    the name using underscores instead of dashes."""

    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9\-.]", "-", name)
    name = name.strip("-")
    name = name.replace("-", "_")
    name = name[:253]

    return name
