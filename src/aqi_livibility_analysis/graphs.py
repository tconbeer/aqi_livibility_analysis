import warnings
from typing import Any, Dict, List

from dagster import (
    ExperimentalWarning,
    daily_partitioned_config,
    graph,
    local_file_manager,
    repository,
)
from dagster_gcp import gcs_file_manager, import_gcs_paths_to_bq

from aqi_livibility_analysis.ops import download_hourly_data
from aqi_livibility_analysis.resources import airnow_resource

# ignore warnings related to using new API with graph, etc.
warnings.filterwarnings("ignore", category=ExperimentalWarning)


@graph
def simple_download_graph() -> None:
    download_hourly_data()


@graph
def gcp_etl_graph() -> None:
    import_gcs_paths_to_bq(download_hourly_data())


@daily_partitioned_config(start_date="2018-01-01")
def hourly_etl_config(start: Any, _end: Any) -> Dict[str, Dict[str, Dict[str, Any]]]:
    config = {
        "ops": {
            "download_hourly_data": {
                "config": {"should_overwrite": False, "target_date": str(start)}
            }
        }
    }
    return config


local_download_job = simple_download_graph.to_job(
    config=hourly_etl_config,
    resource_defs={"airnow": airnow_resource, "fs": local_file_manager},
)

gcp_etl_job = gcp_etl_graph.to_job(
    config=hourly_etl_config,
    resource_defs={
        "airnow": airnow_resource,
        "fs": gcs_file_manager.configured(
            {
                "project": "aqi-livibility-analysis",
                "gcs_bucket": "aqi-raw-data",
                "gcs_prefix": "prod",
            }
        ),
    },
)


@repository
def local_jobs() -> List[Any]:
    return [local_download_job]


@repository
def gcp_jobs() -> List[Any]:
    return [gcp_etl_job]
