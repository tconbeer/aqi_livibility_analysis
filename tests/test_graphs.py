import pytest

from aqi_livibility_analysis.graphs import hourly_etl
from aqi_livibility_analysis.resources import airnow_resource, local_filesystem_resource


@pytest.mark.slow
def test_hourly_etl_graph() -> None:
    hourly_etl.execute_in_process(
        config={
            "download_hourly_data": {
                "config": {"should_overwrite": True, "target_date": "2021-07-01"}
            }
        },
        resources={"airnow": airnow_resource, "fs": local_filesystem_resource},
    )
