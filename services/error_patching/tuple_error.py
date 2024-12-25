import os
import shutil
import pandas as pd
from tqdm import tqdm


slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"
topview_save_dir = "/media/hdd2/neo/tmp_slides_dir/topview"

error_message = "'<' not supported between instances of 'tuple' and 'int'"

# get the list of subdirectories of the LLBMA_results_dir
subdirs = [
    subdir
    for subdir in os.listdir(LLBMA_results_dir)
    if os.path.isdir(os.path.join(LLBMA_results_dir, subdir))
    and subdir.startswith("ERROR_")
]

print(f"Found {len(subdirs)} error directories")

specific_errors_dir = []

for subdir in tqdm(subdirs, desc="Finding error directories"):
    result_dir_path = os.path.join(LLBMA_results_dir, subdir)

    error_txt_path = os.path.join(result_dir_path, "error.txt")

    # get the error message from the error.txt file as a string
    with open(error_txt_path, "r") as f:
        slide_error_message = f.read()

    if error_message in slide_error_message:
        specific_errors_dir.append(subdir)
        print(f"Error message found in {subdir}")

# open the slide metadata file
metadata_df = pd.read_csv(metadata_path)
num_metadata_rows_before_deletion = len(metadata_df)

num_result_dirs_deleted = 0

for subdir in tqdm(specific_errors_dir, desc="Deleting h5 dzsave files"):
    dzsave_h5_path = os.path.join(dzsave_dir, subdir.replace(".ndpi", ".h5"))

    if os.path.exists(dzsave_h5_path):
        os.remove(dzsave_h5_path)
    else:
        print(f"{dzsave_h5_path} does not exist")
        raise FileNotFoundError(f"{dzsave_h5_path} does not exist")


# first delete all the directories with the specified error message
for subdir in tqdm(specific_errors_dir, desc="Deleting error directories"):
    result_dir_path = os.path.join(LLBMA_results_dir, subdir)

    shutil.rmtree(result_dir_path)
    num_result_dirs_deleted += 1

    # remove the subdir from the metadata_df
    metadata_df = metadata_df[metadata_df["wsi_name"] != subdir]

metadata_df.to_csv(metadata_path, index=False)


print(f"Found {len(specific_errors_dir)} directories with the specified error message")
print(f"Deleted {num_result_dirs_deleted} directories")
print(
    f"Number of rows in metadata before deletion: {num_metadata_rows_before_deletion - len(metadata_df)}"
)
