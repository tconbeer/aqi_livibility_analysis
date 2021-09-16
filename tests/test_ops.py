from pathlib import Path
from typing import List

import pytest
from dagster import build_solid_context

from aqi_livibility_analysis.ops import download_hourly_data
from aqi_livibility_analysis.resources import airnow_resource, local_filesystem_resource


@pytest.mark.slow
def test_download_hourly_data() -> None:

    target_date: str = "2021-09-01"
    context = build_solid_context(
        resources={"airnow": airnow_resource, "fs": local_filesystem_resource},
        config={"target_date": target_date, "should_overwrite": True},
    )
    paths_list: List[str] = download_hourly_data(context)

    assert paths_list
    assert len(paths_list) == 24

    for p in [Path(path) for path in paths_list]:
        assert p.exists()
        assert p.suffix == ".gzip"
