import os
import pandas as pd
import random
import shutil
from LLRunner.config import results_dir, pipeline_run_history_path, slide_metadata_path
from pathlib import Path
from tqdm import tqdm

results_dir = "/media/hdd3/neo/results_dir"
save_dir = "/media/hdd3/neo/AML_examples"
num_regions = 20

# if the save_dir does not exist, create it
if not os.path.exists(save_dir):
    os.mkdir(save_dir)

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

def get_Dx(result_dir):
    """ Get the Dx
    """

    # open the pipeline_run_history_path file
    df = pd.read_csv(pipeline_run_history_path)

    # first get the slide name by getting the last part of the result_dir
    slide_name = Path(result_dir).name

    processed_date = slide_name.split("_")[-1]

    # find the row in df where datetime_processed is processed_date
    row = df[df["datetime_processed"] == processed_date]

    slide_name = row["wsi_name"].values[0]

    # open the slide_metadata_path file
    slide_md = pd.read_csv(slide_metadata_path)

    # find the row in slide_md where wsi_name is slide_name
    row = slide_md[slide_md["wsi_name"] == slide_name]

    return row["Dx"].values[0]
    
# get all the subdirectories in the results_dir starting with BMA-diff such that not has_error
subdirs = [f.path for f in os.scandir(results_dir) if f.is_dir() and f.name.startswith("BMA-diff") and not has_error(f.path)]

# only keep the directories such that the Dx is "AML"
subdirs = [f for f in subdirs if get_Dx(f) == "AML"]

print(f"Found {len(subdirs)} AML slides without errors.")

sampled_subdirs = random.sample(subdirs, num_regions)

for subdir in tqdm(sampled_subdirs, desc="Copying regions to save_dir"):
    
    # subdir is a directory in results_dir so make sure to shutil copy the contents to save_dir
    shutil.copytree(subdir, os.path.join(save_dir, Path(subdir).name))
