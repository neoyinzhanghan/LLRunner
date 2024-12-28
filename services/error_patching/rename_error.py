import os
import pandas as pd
from tqdm import tqdm

slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"
topview_save_dir = "/media/hdd2/neo/tmp_slides_dir/topview"

# find all the subdirectories of the LLBMA_results_dir that start with ERROR_
subdirs = [
    subdir
    for subdir in os.listdir(LLBMA_results_dir)
    if os.path.isdir(os.path.join(LLBMA_results_dir, subdir))
    and subdir.startswith("ERROR_")
]

for error_dir in tqdm(subdirs, desc="Finding error directories"):
    result_dir_path = os.path.join(LLBMA_results_dir, error_dir)

    error_txt_path = os.path.join(result_dir_path, "error.txt")

    # get the error message from the error.txt file as a string
    with open(error_txt_path, "r") as f:
        slide_error_message = f.read()

    if (
        "Number of cells passed is less than 200. This error is written posthoc"
        in slide_error_message
    ):

        # add a "Too few good candidates found error. " message before the error message and overwrite the error.txt file
        with open(error_txt_path, "w") as f:
            f.write("Too few good candidates found. " + slide_error_message)

print("Finished adding error message to error directories")
