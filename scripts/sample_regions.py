import os
import pandas as pd
from LLRunner.config import results_dir, pipeline_run_history_path
from pathlib import Path

results_dir = "/media/hdd3/neo/results_dir"
save_dir = "/media/hdd3/neo/sampled_regions"
num_regions = 25

# get all the subdirectories in the results_dir
subdirs = [f.path for f in os.scandir(results_dir) if f.is_dir()]

def has_error(result_dir):
    """ Check if the result_dir contains an error file.
    """
    
    # open the pipeline_run_history_path file
    df = pd.read_csv(pipeline_run_history_path)

    # first get the slide name by getting the last part of the result_dir
    slide_name = Path(result_dir).name

    processed_date = slide_name.split("_")[-1]

    # find the row in df where datetime_processed is processed_date
    row = df[df["datetime_processed"] == processed_date]

    return bool(row["error"].values[0])

# get all the subdirectories in the results_dir starting with BMA-diff such that not has_error
subdirs = [f.path for f in os.scandir(results_dir) if f.is_dir() and f.name.startswith("BMA-diff") and not has_error(f.path)]

# only keep the directories where the focus_regions/high_mag_unannotated subfolder exists
subdirs = [f for f in subdirs if os.path.exists(os.path.join(f, "focus_regions/high_mag_unannotated"))]

all_regions_paths = []

for subdir in subdirs:
    # get the focus_regions/high_mag_unannotated subfolder
    high_mag_unannotated = os.path.join(subdir, "focus_regions/high_mag_unannotated")

    # get all the regions in the high_mag_unannotated subfolder
    regions = os.listdir(high_mag_unannotated)

    # get the full path to each region
    regions = [os.path.join(high_mag_unannotated, region) for region in regions]

    all_regions_paths.extend(regions)

print(len(all_regions_paths))