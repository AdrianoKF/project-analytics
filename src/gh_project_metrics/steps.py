import os

from github import Github

from gh_project_metrics.metrics.github import GithubMetrics, MetricsConfig
from gh_project_metrics.metrics.pypi import Config, PyPIMetrics


def github_metrics(
    repo_name: str,
) -> GithubMetrics:
    gh = Github(login_or_token=os.getenv("GITHUB_ACCESS_TOKEN"))
    repo = gh.get_repo(repo_name)
    config = MetricsConfig(repo=repo, aggregate_time="D")
    metrics = GithubMetrics(config)
    return metrics


def pypi_metrics(
    package_name: str,
    gcp_project_id: str | None = None,
) -> PyPIMetrics:
    config = Config(package_name=package_name, gcp_project_id=gcp_project_id)
    metrics = PyPIMetrics(config)
    return metrics
