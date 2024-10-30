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
_parser.add_argument(
    "name", help="GitHub project name (will infer PyPI package name automatically)"
)
_parser.add_argument(
    "--supabase",
    help="Log to Supabase (need to specify SUPABASE_URL and SUPABASE_ENV env vars)",
    action=BooleanOptionalAction,
)
_parser.add_argument(
    "--gcp-project-id",
    help="Google Cloud project ID (omit to use gcloud CLI default project)",
)
_parser.add_argument(
    "--bigquery-dataset-prefix",
    help="Name prefix for datasets created in BigQuery",
)
_parser.add_argument(
    "--bigquery",
    help="Log to Google BigQuery (need to authenticate first)",
    action=BooleanOptionalAction,
)

_parser.add_argument(
    "--pypi-package-name",
    help="PyPI package name (if different from GitHub project name)",
)

_parser.set_defaults(
    date=datetime.datetime.now(tz=datetime.UTC),
    pypi=True,
    github=True,
    supabase=False,
    gcp_project_id=None,
    bigquery=False,
    bigquery_dataset_prefix="",
)

args = _parser.parse_args()
