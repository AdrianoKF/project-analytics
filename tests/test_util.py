import pandas as pd
import pytest

from gh_project_metrics.util import combine


@pytest.mark.parametrize(
    "df1, df2, expected",
    [
        (  # Happy case, normal index
            pd.DataFrame(
                {
                    "a": [0, 1, 2],
                    "b": [2, 3, 4],
                },
            ).set_index("a"),
            pd.DataFrame(
                {
                    "a": [2, 3],
                    "b": [2, 3],
                },
            ).set_index("a"),
            pd.DataFrame(
                {
                    "a": [0, 1, 2, 3],
                    "b": [2, 3, 2, 3],
                },
            ).set_index("a"),
        ),
        (  # Happy case, multi index
            pd.DataFrame(
                {
                    "a": [0, 1, 2],
                    "b": [2, 3, 4],
                    "c": [5, 6, 7],
                },
            ).set_index(["a", "b"]),
            pd.DataFrame(
                {
                    "a": [2, 3],
                    "b": [4, 5],
                    "c": [8, 8],
                },
            ).set_index(["a", "b"]),
            pd.DataFrame(
                {
                    "a": [0, 1, 2, 3],
                    "b": [2, 3, 4, 5],
                    "c": [5, 6, 8, 8],
                },
            ).set_index(["a", "b"]),
        ),
    ],
)
def test_combine(df1: pd.DataFrame, df2: pd.DataFrame, expected: pd.DataFrame) -> None:
    actual = combine(df1, df2)
    pd.testing.assert_frame_equal(actual, expected)
