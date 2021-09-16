import gzip
from typing import Any, List

from dagster import op


@op
def download_hourly_data(context: Any) -> List[str]:
    target_date = context.op_config["target_date"]
    should_overwrite = context.op_config["should_overwrite"]
    airnow = context.resources.airnow
    fs = context.resources.fs

    paths: List[str] = []

    for hour in range(24):
        path = fs.get_path_from_key(f"hourly-{target_date}-{hour:02}.dat.gzip")
        if should_overwrite or not fs.exists(path):
            data = airnow.get_hourly_data(date=target_date, hour=hour)
            compressed_data = gzip.compress(data)
            fs.save_buffer(buffer=compressed_data, path=path)

        paths.append(path)

    return paths
