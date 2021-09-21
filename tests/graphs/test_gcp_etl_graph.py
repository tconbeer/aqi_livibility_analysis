import pytest
from dagster_gcp import bigquery_resource, gcs_file_manager

from aqi_livibility_analysis import GCP_PROJECT
from aqi_livibility_analysis.graphs import gcp_etl_graph
from aqi_livibility_analysis.resources import airnow_resource


@pytest.mark.slow
@pytest.mark.gcp
@pytest.mark.filterwarnings("ignore:The loop argument:DeprecationWarning")
@pytest.mark.filterwarnings("ignore:coroutine 'noop' was never awaited:RuntimeWarning")
def test_gcp_etl_graph() -> None:
    gcp_etl_graph.execute_in_process(
        config={
            "download_hourly_data": {"config": {"target_date": "2021-08-01"}},
            "import_gcs_paths_to_bq": {
                "config": {
                    "destination": f"{GCP_PROJECT}.test.test_gcp_etl_graph",
                    "load_job_config": {
                        "source_format": "PARQUET",
                        "write_disposition": "WRITE_TRUNCATE",
                    },
                }
            },
        },
        resources={
            "airnow": airnow_resource,
            "fs": gcs_file_manager.configured(
                {
                    "project": GCP_PROJECT,
                    "gcs_bucket": "aqi-raw-data",
                    "gcs_prefix": "test/test_gcp_etl_graph",
                }
            ),
            "bigquery": bigquery_resource.configured({"project": GCP_PROJECT}),
        },
    )
