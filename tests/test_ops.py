from pathlib import Path
from typing import List

import pandas as pd
import pytest
from dagster import FileHandle, LocalFileHandle, build_solid_context, local_file_manager
from dagster_gcp import GCSFileHandle, gcs_file_manager

from aqi_livibility_analysis.ops import (
    _read_hourly_file_to_dataframe,
    download_hourly_data,
    transform_hourly_data,
)
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


class TestTransform:
    @pytest.fixture
    def raw_file_path(self) -> str:
        test_dir = Path(__file__).parent
        p = test_dir / "data" / "HourlyData_2019011713.dat.gzip"
        return str(p)

    def test_read_hourly_file_to_dataframe(self, raw_file_path: str) -> None:
        df = _read_hourly_file_to_dataframe(raw_file_path)

        assert df is not None
        assert df.shape == (15136, 7)
        assert "observed_at" in df.columns
        assert "parameter_name" in df.columns
        assert "value" in df.columns

    def test_local_transform_hourly_data(self, raw_file_path: str) -> None:
        h = LocalFileHandle(path=raw_file_path)
        context = build_solid_context(resources={"fs": local_file_manager})
        transformed_handles = transform_hourly_data(context, raw_files=[h])

        assert transformed_handles is not None
        assert len(transformed_handles) == 1

        transformed_file_path = transformed_handles[0].path
        df = pd.read_parquet(transformed_file_path)

        assert df is not None
        assert df.shape == (15136, 7)
        assert "observed_at" in df.columns
        assert "parameter_name" in df.columns
        assert "value" in df.columns

    @pytest.mark.slow
    @pytest.mark.gcp
    @pytest.mark.filterwarnings("ignore:The loop argument:DeprecationWarning")
    @pytest.mark.filterwarnings(
        "ignore:coroutine 'noop' was never awaited:RuntimeWarning"
    )
    def test_gcs_transform_hourly_data(self, raw_file_path: str) -> None:
        h = LocalFileHandle(path=raw_file_path)
        context = build_solid_context(
            resources={
                "fs": gcs_file_manager.configured(
                    {
                        "project": "aqi-livibility-analysis",
                        "gcs_bucket": "aqi-raw-data",
                        "gcs_prefix": "test/test_gcs_transform_hourly_data",
                    }
                )
            }
        )
        transformed_handles = transform_hourly_data(context, raw_files=[h])

        assert transformed_handles is not None
        assert len(transformed_handles) == 1

        transformed_file_path = transformed_handles[0].gcs_path
        df = pd.read_parquet(transformed_file_path)

        assert df is not None
        assert df.shape == (15136, 7)
        assert "observed_at" in df.columns
        assert "parameter_name" in df.columns
        assert "value" in df.columns
