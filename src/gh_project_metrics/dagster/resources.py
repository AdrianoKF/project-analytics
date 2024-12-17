from dagster import ConfigurableResource


class GoogleCloud(ConfigurableResource):
    project_id: str | None
