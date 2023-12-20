import datetime
from argparse import ArgumentParser

_parser = ArgumentParser()
_parser.add_argument(
    "-d",
    "--date",
    required=False,
    default=datetime.datetime.now().strftime("%Y-%m-%d"),
)

args = _parser.parse_args()
