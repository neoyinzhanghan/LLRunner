import os
import pandas as pd
from LLRunner.config import results_dir, pipeline_run_history_path
from pathlib import Path

results_dir = "/media/hdd3/neo/results_dir"

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


for subdir in subdirs:
    print(has_error(subdir))