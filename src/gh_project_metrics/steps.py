import os

from github import Github

from gh_project_metrics.metrics.github import GithubMetrics, MetricsConfig
from gh_project_metrics.metrics.pypi import PyPIMetrics


def github_metrics(
    repo_name: str,
) -> GithubMetrics:
    gh = Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN"))
    repo = gh.get_repo(repo_name)
    config = MetricsConfig(aggregate_time="D")
    metrics = GithubMetrics(repo, config)
    return metrics


def pypi_metrics(
    package_name: str,
    gcp_project_id: str | None = None,
) -> PyPIMetrics:
    metrics = PyPIMetrics(package_name=package_name, gcp_project_id=gcp_project_id)
    return metrics
