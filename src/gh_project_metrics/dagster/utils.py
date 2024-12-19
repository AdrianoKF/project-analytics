from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass
class Partition:
    date: datetime
    project: str


def parse_partition_key(partition_key: str) -> Partition:
    [date_str, project] = partition_key.split("|", 1)
    date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
    return Partition(date=date, project=project)


def plot_date_range(partition: Partition) -> tuple[datetime, datetime]:
    """Calculate the start and end date for plotting a given partition."""
    start_date = partition.date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end_date = partition.date.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start_date, end_date
