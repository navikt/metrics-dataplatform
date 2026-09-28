import os

from source_stage import run_source_stage
from stage_to_dp import run_stage_to_dp
from util import determine_dates_to_upload, to_time_range

if __name__ == "__main__":
    # One day at a time keeps memory bounded
    for day in determine_dates_to_upload(os.environ["STAGE_TABLE"]):
        print(f"Uploading stage data for {day}")
        run_source_stage(to_time_range(day))

    for day in determine_dates_to_upload(os.environ["DATAPRODUCTS_TABLE"]):
        print(f"Uploading dataproducts data for {day}")
        run_stage_to_dp(to_time_range(day))
