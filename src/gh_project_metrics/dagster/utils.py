from dataclasses import dataclass
from datetime import datetime


@dataclass
class Partition:
    start_date: datetime
    project: str


def parse_partition_key(partition_key: str) -> Partition:
    [date_str, project] = partition_key.split("|", 1)
    start_date = datetime.strptime(date_str, "%Y-%m-%d")

    return Partition(start_date=start_date, project=project)
