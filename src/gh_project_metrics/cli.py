import datetime
from argparse import ArgumentParser, BooleanOptionalAction

_parser = ArgumentParser()
_parser.add_argument(
    "-d",
    "--date",
    required=False,
    type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
)
_parser.add_argument("--pypi", help="Fetch PyPI metrics", action=BooleanOptionalAction)
_parser.add_argument("--github", help="Fetch GitHub metrics", action=BooleanOptionalAction)

_parser.set_defaults(
    date=datetime.datetime.utcnow(),
    pypi=True,
    github=True,
)

args = _parser.parse_args()
