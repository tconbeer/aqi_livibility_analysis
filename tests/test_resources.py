from typing import Any

from dagster import build_init_resource_context
from dagster_gcp import gcs_file_manager

from aqi_livibility_analysis.resources import AirNow, airnow_resource


def test_airnow_resource() -> None:
    resource = airnow_resource()
    assert resource is not None
    assert isinstance(resource, AirNow)


def test_airnow_build_hourly_url() -> None:
    resource = airnow_resource()
    date = "2021-01-15"
    hour = 14
    expected_url = (
        "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"
        "2021/20210115/HourlyData_2021011514.dat"
    )
    url = resource._build_hourly_url(date=date, hour=hour)
    assert url == expected_url


# this uses the requests_mock pytest plugin
def test_airnow_get_hourly_data(requests_mock: Any) -> None:
    resource = airnow_resource()

    fake_data = b"abc123"
    date = "2020-09-05"
    hour = 1

    url = resource._build_hourly_url(date, hour)
    requests_mock.get(url, content=fake_data)

    data = resource.get_hourly_data(date=date, hour=hour)

    assert data == fake_data


def test_gcs_file_manager_resource() -> None:
    init_context = build_init_resource_context(
        config={
            "project": "aqi-livibility-analysis",
            "gcs_bucket": "aqi-raw-data",
            "gcs_prefix": "test/test_gcs_file_manager_resource",
        }
    )
    resource = gcs_file_manager(init_context)
    assert resource is not None
