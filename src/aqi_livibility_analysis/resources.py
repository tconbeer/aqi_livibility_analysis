from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dagster import resource


class AirNow:
    def build_hourly_url(self, date: str, hour: int) -> str:
        dt = datetime.fromisoformat(date)
        BASE_URL = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"
        date_url = (
            f"{dt.year}/{dt.year}{dt.month:02}{dt.day:02}/"
            f"HourlyData_{dt.year}{dt.month:02}{dt.day:02}{hour:02}"
        )
        SUFFIX = ".dat"
        url = BASE_URL + date_url + SUFFIX
        return url

    def get_hourly_data(self, date: str, hour: int) -> bytes:
        url = self.build_hourly_url(date, hour)
        response = requests.get(url, allow_redirects=True)
        return response.content


@resource
def airnow_resource() -> AirNow:
    return AirNow()


class LocalFilesystem:
    def __init__(self, storage_dir: str) -> None:
        self.storage_dir: Path = Path(storage_dir)

    def get_path_from_key(self, key: str) -> str:
        return str(self.storage_dir / key)

    def write_data(self, data: bytes, path: str) -> None:
        p = Path(path)
        p.parent.mkdir(exist_ok=True, parents=True)
        with open(p, "wb") as f:
            f.write(data)

    def exists(self, path: str) -> bool:
        p = Path(path)
        return p.exists()


@resource
def local_filesystem_resource(init_context: Any) -> LocalFilesystem:
    return LocalFilesystem(storage_dir=init_context.instance.storage_directory())
