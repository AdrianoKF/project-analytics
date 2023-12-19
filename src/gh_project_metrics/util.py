from pathlib import Path

import pandas as pd


def combine(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    sort_kwargs: dict | None = None,
) -> pd.DataFrame:
    if isinstance(df1.index, pd.MultiIndex):
        if not (df1.index.names == df2.index.names and all(df1.index.dtypes == df2.index.dtypes)):
            raise ValueError("DataFrames must have equally named and typed indexes")
    else:
        if not (df1.index.name == df2.index.name and df1.index.dtype == df2.index.dtype):
            raise ValueError("DataFrames must have equally named and typed indexes")
    if any(df1.columns != df2.columns):
        raise ValueError("DataFrames must matching columns")

    return pd.concat([df2, df1.loc[df1.index.difference(df2.index)]]).sort_index(
        **(sort_kwargs or {})
    )


def combine_csv(
    df: pd.DataFrame,
    outfile: Path,
    sort_kwargs: dict | None = None,
    read_kwargs: dict | None = None,
    write_kwargs: dict | None = None,
) -> None:
    """Write a DataFrame to a CSV file, combining with existing data, if any."""
    write_df = df.copy()
    if outfile.exists():
        old_df = pd.read_csv(
            outfile, parse_dates=True, index_col=write_df.index.names, **(read_kwargs or {})
        )
        write_df = combine(write_df, old_df, sort_kwargs=sort_kwargs)
    write_df.to_csv(outfile, index=True, **(write_kwargs or {}))
