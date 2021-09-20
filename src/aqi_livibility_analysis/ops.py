import gzip
import warnings
from typing import Any, List

from dagster import ExperimentalWarning, FileHandle, op

# ignore warnings related to using new API with op
warnings.filterwarnings("ignore", category=ExperimentalWarning)


@op(
    config_schema={"target_date": str},
    required_resource_keys={"airnow", "fs"},
)
def download_hourly_data(context: Any) -> List[FileHandle]:
    target_date = context.op_config["target_date"]
    airnow = context.resources.airnow
    fs = context.resources.fs

    paths: List[FileHandle] = []

    for hour in range(24):
        data = airnow.get_hourly_data(date=target_date, hour=hour)
        compressed_data = gzip.compress(data)
        path = fs.write_data(data=compressed_data, ext="gzip")

        paths.append(path)

    return paths
