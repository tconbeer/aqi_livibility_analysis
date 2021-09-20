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
        index_col="site_id",
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


@op(required_resource_keys={"fs"})
def transform_hourly_data(
    context: Any, raw_files: List[FileHandle]
) -> List[FileHandle]:
    transformed_files: List[FileHandle] = []
    fs = context.resources.fs

    for p in [h.path_desc for h in raw_files]:
        raw_df = _read_hourly_file_to_dataframe(p)

        transformed_bytes = raw_df.to_parquet(compression=None, index=False)

        path = fs.write_data(data=transformed_bytes, ext="parquet")

        transformed_files.append(path)

    return transformed_files
