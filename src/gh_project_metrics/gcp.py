import argparse
import os


def get_gcp_project_id(args: argparse.Namespace) -> str | None:
    if "gcp_project_id" in args and (id := args.gcp_project_id):
        return id
    return os.getenv("GCP_PROJECT")
