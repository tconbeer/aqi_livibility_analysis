import warnings
from typing import Any, Dict, List

from dagster import ExperimentalWarning, daily_partitioned_config, graph, repository

from aqi_livibility_analysis.ops import download_hourly_data
from aqi_livibility_analysis.resources import airnow_resource, local_filesystem_resource

# ignore warnings related to using new API with graph, etc.
warnings.filterwarnings("ignore", category=ExperimentalWarning)


@graph
def hourly_etl() -> None:
    download_hourly_data()


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


local_hourly_etl_job = hourly_etl.to_job(
    config=hourly_etl_config,
    resource_defs={"airnow": airnow_resource, "fs": local_filesystem_resource},
)


@repository
def local_jobs() -> List[Any]:
    return [local_hourly_etl_job]
