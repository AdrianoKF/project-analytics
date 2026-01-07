from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


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

    LAST_MONTH_CUTOFF_DAY = 15

    # If we're on the first days of the month, plot the previous month
    current_date = partition.date
    if partition.date.day < LAST_MONTH_CUTOFF_DAY:
        # Roll back to the previous month
        current_date = partition.date.replace(day=1) - timedelta(microseconds=1)

    start_date = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # Include all data up to midnight of the end date
    end_date = current_date.replace(hour=23, minute=59, second=59, microsecond=999999)

    return start_date, end_date
