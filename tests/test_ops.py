from pathlib import Path
from typing import List

import pandas as pd
import pytest
from dagster import FileHandle, LocalFileHandle, build_solid_context, local_file_manager
from dagster_gcp import (
    GCSFileHandle,
    bigquery_resource,
    gcs_file_manager,
    import_gcs_paths_to_bq,
)

from aqi_livibility_analysis import GCP_PROJECT
from aqi_livibility_analysis.ops import (
    _read_hourly_file_to_dataframe,
    _read_site_file_to_dataframe,
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

    for p in [Path(h.path_desc) for h in handles]:
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

    for p in [h.path_desc for h in handles]:
        assert p.endswith(".gzip")


class TestTransform:
    @pytest.fixture
    def raw_file_path(self) -> str:
        test_dir = Path(__file__).parent
        p = test_dir / "data" / "HourlyData_2019011713.dat.gzip"
        return str(p)

    @pytest.fixture
    def expected_columns(self) -> List[str]:
        columns = [
            "observed_at",
            "site_id",
            "site_name",
            "gmt_offset",
            "parameter_name",
            "reporting_units",
            "value",
            "data_source_agency",
        ]
        return columns

    def test_read_hourly_file_to_dataframe(
        self, raw_file_path: str, expected_columns: List[str]
    ) -> None:
        df = _read_hourly_file_to_dataframe(raw_file_path)

        assert df is not None
        assert df.shape == (15136, 8)
        for col in expected_columns:
            assert col in df.columns

    def test_local_transform_hourly_data(
        self, raw_file_path: str, expected_columns: List[str]
    ) -> None:
        h = LocalFileHandle(path=raw_file_path)
        context = build_solid_context(resources={"fs": local_file_manager})
        transformed_paths = transform_hourly_data(context, raw_files=[h])

        assert transformed_paths is not None
        assert len(transformed_paths) == 1

        transformed_file_path = transformed_paths[0]
        df = pd.read_parquet(transformed_file_path)

        assert df is not None
        assert df.shape == (15136, 8)
        for col in expected_columns:
            assert col in df.columns

    @pytest.mark.slow
    @pytest.mark.gcp
    @pytest.mark.filterwarnings("ignore:The loop argument:DeprecationWarning")
    @pytest.mark.filterwarnings(
        "ignore:coroutine 'noop' was never awaited:RuntimeWarning"
    )
    def test_gcs_transform_hourly_data(
        self, raw_file_path: str, expected_columns: List[str]
    ) -> None:
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
        transformed_paths = transform_hourly_data(context, raw_files=[h])

        assert transformed_paths is not None
        assert len(transformed_paths) == 1

        transformed_file_path = transformed_paths[0]
        df = pd.read_parquet(transformed_file_path)

        assert df is not None
        assert df.shape == (15136, 8)
        for col in expected_columns:
            assert col in df.columns


@pytest.mark.slow
@pytest.mark.gcp
def test_import_gcs_paths_to_bq() -> None:
    uri = (
        "gs://aqi-raw-data/test/test_import_gcs_paths_to_bq/"
        "7f901271-7d76-4d9d-85f6-42188a361196.parquet"
    )
    context = build_solid_context(
        config={
            "destination": f"{GCP_PROJECT}.test.test_import_gcs_paths_to_bq",
            "load_job_config": {
                "source_format": "PARQUET",
                "write_disposition": "WRITE_TRUNCATE",
            },
        },
        resources={"bigquery": bigquery_resource.configured({"project": GCP_PROJECT})},
    )
    import_gcs_paths_to_bq(context, [uri])


def test_read_site_file_to_dataframe() -> None:
    test_dir = Path(__file__).parent
    p = test_dir / "data" / "monitoring_site_locations.dat.gzip"

    df = _read_site_file_to_dataframe(str(p))

    assert df is not None
    assert df.shape == (18755, 21)

    columns = [
        "site_id",
        "parameter_name",
        "site_code",
        "site_name",
        "status",
        "data_source_id",
        "data_source_agency",
        "epa_region",
        "latitude",
        "longitude",
        "elevation",
        "gmt_offset",
        "country_code",
        "blank1",
        "blank2",
        "msa_code",
        "msa_name",
        "state_code",
        "state_name",
        "county_code",
        "county_name",
    ]

    for col in columns:
        assert col in df.columns
