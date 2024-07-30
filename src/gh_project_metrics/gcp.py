import argparse
import urllib.parse

import google.auth

DEFAULT_LOOKERSTUDIO_TEMPLATE_REPORT_ID = "ec974853-24a1-4bf7-a23c-e6dcc7e0fbc2"


def get_gcp_project_id(args: argparse.Namespace) -> str | None:
    if "gcp_project_id" in args and (id := args.gcp_project_id):
        return id
    _, project = google.auth.default()
    return project


def create_lookerstudio_report_url(
    template_report_id: str, project_name: str, gcp_project_id: str, bq_dataset: str
) -> str:
    tables = ["referrers", "views", "stars", "downloads", "views", "clones"]
    params = {
        "c.reportId": template_report_id,
        "r.reportName": f"Project metrics for {project_name}",
    }

    for table in tables:
        params |= {
            f"ds.{table}.connector": "bigQuery",
            f"ds.{table}.type": "TABLE",
            f"ds.{table}.projectId": gcp_project_id,
            f"ds.{table}.datasetId": bq_dataset,
            f"ds.{table}.tableId": table,
        }
    return "https://lookerstudio.google.com/reporting/create?" + urllib.parse.urlencode(params)
