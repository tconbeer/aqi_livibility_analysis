import pytest
from dagster import local_file_manager

from aqi_livibility_analysis.graphs import simple_download_graph
from aqi_livibility_analysis.resources import airnow_resource


@pytest.mark.slow
def test_simple_download_graph() -> None:
    simple_download_graph.execute_in_process(
        config={"download_hourly_data": {"config": {"target_date": "2021-07-01"}}},
        resources={"airnow": airnow_resource, "fs": local_file_manager},
    )
