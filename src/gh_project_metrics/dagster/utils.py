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

    LAST_MONTH_CUTOFF_DAY = 15

    # If we're on the first days of the month, plot the previous month
    current_date = partition.date
    if partition.date.day < LAST_MONTH_CUTOFF_DAY:
        current_date = partition.date.replace(
            month=partition.date.month - 1,
        )

    start_date = current_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # If we're on the last days of the month, plot until the end of the month
    end_date = current_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    if partition.date.day < LAST_MONTH_CUTOFF_DAY:
        end_date = end_date.replace(
            day=1, month=end_date.month + 1, hour=0, minute=0, second=0, microsecond=0
        )
    return start_date, end_date
