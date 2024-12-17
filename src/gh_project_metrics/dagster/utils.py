from dataclasses import dataclass
from datetime import datetime


@dataclass
class Partition:
    date: datetime
    project: str


def parse_partition_key(partition_key: str) -> Partition:
    [date_str, project] = partition_key.split("|", 1)
    date = datetime.strptime(date_str, "%Y-%m-%d")

    return Partition(date=date, project=project)
