import pytest
from dagster_gcp import bigquery_resource, gcs_file_manager

from aqi_livibility_analysis.graphs import gcp_etl_graph
from aqi_livibility_analysis.resources import airnow_resource


@pytest.mark.gcp
def test_gcp_etl_graph() -> None:
    gcp_etl_graph.execute_in_process(
        config={"download_hourly_data": {"config": {"target_date": "2021-07-01"}}},
        resources={
            "airnow": airnow_resource,
            "fs": gcs_file_manager,
            "bigquery": bigquery_resource,
        },
    )
