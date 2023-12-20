import datetime
from argparse import ArgumentParser, BooleanOptionalAction

_parser = ArgumentParser()
_parser.add_argument(
    "-d",
    "--date",
    required=False,
)
_parser.add_argument("--pypi", help="Fetch PyPI metrics", action=BooleanOptionalAction)
_parser.add_argument("--github", help="Fetch GitHub metrics", action=BooleanOptionalAction)

_parser.set_defaults(
    date=datetime.datetime.now().strftime("%Y-%m-%d"),
    pypi=True,
    github=True,
)

args = _parser.parse_args()
