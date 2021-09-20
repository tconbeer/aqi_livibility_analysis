from pathlib import Path
from typing import List

import pytest
from dagster import FileHandle, build_solid_context, local_file_manager
from dagster_gcp import GCSFileHandle, gcs_file_manager

from aqi_livibility_analysis.ops import download_hourly_data
from aqi_livibility_analysis.resources import airnow_resource


@pytest.mark.slow
def test_local_download_hourly_data() -> None:

    target_date: str = "2021-09-01"
    context = build_solid_context(
        resources={"airnow": airnow_resource, "fs": local_file_manager},
        config={"target_date": target_date},
    )
    handles: List[FileHandle] = download_hourly_data(context)

    assert handles
    assert len(handles) == 24

    for p in [Path(h.path) for h in handles]:
        assert p.exists()
        assert p.suffix == ".gzip"


@pytest.mark.slow
@pytest.mark.gcp
def test_gcs_download_hourly_data() -> None:

    target_date: str = "2020-09-01"
    context = build_solid_context(
        resources={
            "airnow": airnow_resource,
            "fs": gcs_file_manager.configured(
                {
                    "project": "aqi-livibility-analysis",
                    "gcs_bucket": "aqi-raw-data",
                    "gcs_prefix": "test/test_gcs_download_hourly_data",
                }
            ),
        },
        config={"target_date": target_date},
    )
    handles: List[GCSFileHandle] = download_hourly_data(context)

    assert handles
    assert len(handles) == 24

    for p in [h.gcs_path for h in handles]:
        assert p.endswith(".gzip")
