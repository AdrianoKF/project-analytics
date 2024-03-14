import datetime
import os
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
_parser.add_argument(
    "name", help="GitHub project name (will infer PyPI package name automatically)"
)
_parser.add_argument(
    "--supabase",
    help="Log to Supabase (need to specify SUPABASE_URL and SUPABASE_ENV env vars)",
    action=BooleanOptionalAction,
)

_parser.set_defaults(
    date=datetime.datetime.now(tz=datetime.UTC),
    pypi=True,
    github=True,
    supabase="SUPABASE_URL" in os.environ and "SUPABASE_KEY" in os.environ,
)

args = _parser.parse_args()
