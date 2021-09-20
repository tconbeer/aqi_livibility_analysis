import pytest
from dagster import local_file_manager
from dagster_gcp import gcs_file_manager

from aqi_livibility_analysis.graphs import gcp_etl_graph, simple_download_graph
from aqi_livibility_analysis.resources import airnow_resource


@pytest.mark.slow
def test_simple_download_graph() -> None:
    simple_download_graph.execute_in_process(
        config={"download_hourly_data": {"config": {"target_date": "2021-07-01"}}},
        resources={"airnow": airnow_resource, "fs": local_file_manager},
    )


@pytest.mark.slow
@pytest.mark.gcp
@pytest.mark.filterwarnings("ignore:The loop argument:DeprecationWarning")
@pytest.mark.filterwarnings("ignore:coroutine 'noop' was never awaited:RuntimeWarning")
def test_gcp_etl_graph() -> None:
    gcp_etl_graph.execute_in_process(
        config={"download_hourly_data": {"config": {"target_date": "2021-08-01"}}},
        resources={
            "airnow": airnow_resource,
            "fs": gcs_file_manager.configured(
                {
                    "project": "aqi-livibility-analysis",
                    "gcs_bucket": "aqi-raw-data",
                    "gcs_prefix": "test/test_gcp_etl_graph",
                }
            ),
        },
    )
