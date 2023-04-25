import os
from source_stage import run_source_stage
from stage_to_dp import run_stage_to_dp
from util import determine_time_range


if __name__ == "__main__":
    stage_time_range = determine_time_range(os.environ["STAGE_TABLE"])
    if stage_time_range:
        print(f"Uploading stage data between {stage_time_range}")
        run_source_stage(stage_time_range)

    dp_time_range = determine_time_range(os.environ["DATAPRODUCTS_TABLE"])
    if dp_time_range:
        print(f"Uploading dataproducts data between {dp_time_range}")
        run_stage_to_dp(dp_time_range)
