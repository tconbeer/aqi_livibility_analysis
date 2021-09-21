import gzip
import warnings
from typing import Any, BinaryIO, List, TextIO, Union

import numpy as np
import pandas as pd
from dagster import ExperimentalWarning, FileHandle, op

# ignore warnings related to using new API with op
warnings.filterwarnings("ignore", category=ExperimentalWarning)


@op(
    config_schema={"target_date": str},
    required_resource_keys={"airnow", "fs"},
)
def download_hourly_data(context: Any) -> List[FileHandle]:
    target_date = context.op_config["target_date"]
    airnow = context.resources.airnow
    fs = context.resources.fs

    paths: List[FileHandle] = []

    for hour in range(24):
        data = airnow.get_hourly_data(date=target_date, hour=hour)
        compressed_data = gzip.compress(data)
        path = fs.write_data(data=compressed_data, ext="gzip")
        paths.append(path)

    return paths


def _read_hourly_file_to_dataframe(
    filepath_or_buffer: Union[str, BinaryIO, TextIO]
) -> pd.DataFrame:
    df = pd.read_csv(
        filepath_or_buffer,
        sep="|",
        names=[
            "observed_date",
            "observed_time",
            "site_id",
            "site_name",
            "gmt_offset",
            "parameter_name",
            "reporting_units",
            "value",
            "data_source_agency",
        ],
        index_col=False,
        dtype={
            "observed_date": str,
            "observed_time": str,
            "site_id": str,
            "site_name": str,
            "gmt_offset": np.float64,
            "parameter_name": str,
            "reporting_units": str,
            "value": np.float64,
            "data_source_agency": str,
        },
        parse_dates={"observed_at": [0, 1]},
        infer_datetime_format=True,
        cache_dates=True,
        compression="gzip",
    )

    return df


def _read_site_file_to_dataframe(
    filepath_or_buffer: Union[str, BinaryIO, TextIO]
) -> pd.DataFrame:
    df = pd.read_csv(
        filepath_or_buffer,
        sep="|",
        names=[
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
        ],
        index_col=False,
        dtype={
            "site_id": str,
            "parameter_name": str,
            "site_code": str,
            "site_name": str,
            "status": str,
            "data_source_id": str,
            "data_source_agency": str,
            "epa_region": str,
            "latitude": np.float64,
            "longitude": np.float64,
            "elevation": str,
            "gmt_offset": np.float64,
            "country_code": str,
            "blank1": str,
            "blank2": str,
            "msa_code": str,
            "msa_name": str,
            "state_code": str,
            "state_name": str,
            "county_code": str,
            "county_name": str,
        },
        compression="gzip",
        encoding_errors="ignore",
    )

    return df


@op(required_resource_keys={"fs"})
def transform_hourly_data(context: Any, raw_files: List[FileHandle]) -> List[str]:
    transformed_files: List[FileHandle] = []
    fs = context.resources.fs

    for p in [h.path_desc for h in raw_files]:
        raw_df = _read_hourly_file_to_dataframe(p)
        transformed_bytes = raw_df.to_parquet(compression=None, index=False)
        handle = fs.write_data(data=transformed_bytes, ext="parquet")
        transformed_files.append(handle.path_desc)

    return transformed_files
