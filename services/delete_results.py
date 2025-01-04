import os
import pandas as pd
import shutil
from tqdm import tqdm

cutoffdatetime = "2025-01-03 00:00:00"
metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"
slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"
topview_save_dir = "/media/hdd2/neo/tmp_slides_dir/topview"


def get_slide_datetime(slide_name):
    try:
        name = slide_name.split(".ndpi")[0]
        datetime = name.split(" - ")[-1]

        # convert the datetime to a datetime object
        datetime = pd.to_datetime(datetime, format="%Y-%m-%d %H.%M.%S")
    except Exception as e:
        print(f"Error getting datetime for {slide_name}: {e}")
        raise e
    return datetime


# get the wsi_name column of the metadata as a list of strings
metadata = pd.read_csv(metadata_path)

wsi_names_after_cutoff = []
unprocessed_slides = []

# traverse through the rows of the metadata
for index, row in tqdm(metadata.iterrows(), total=metadata.shape[0]):
    slide_name = row["wsi_name"]
    slide_datetime = get_slide_datetime(slide_name)
    h5_name = slide_name.replace(".ndpi", ".h5")
    h5_path = os.path.join(dzsave_dir, h5_name)

    tmp_slide_path = os.path.join(tmp_slide_dir, slide_name)

    # check if the slide_datetime is less than the cutoffdatetime
    if slide_datetime > pd.to_datetime(cutoffdatetime):

        result_folder_path = os.path.join(
            LLBMA_results_dir, slide_name.split(".ndpi")[0]
        )
        error_result_folder_path = os.path.join(
            LLBMA_results_dir, "ERROR_" + slide_name.split(".ndpi")[0]
        )

        if os.path.exists(h5_path):
            print(f"Removing {h5_path}")
            os.remove(h5_path)

        if os.path.exists(tmp_slide_path):
            print(f"Removing {tmp_slide_path}")
            os.remove(tmp_slide_path)

        if os.path.exists(result_folder_path):
            print(f"Removing {result_folder_path}")
            shutil.rmtree(result_folder_path)

        if os.path.exists(error_result_folder_path):
            print(f"Removing {error_result_folder_path}")
            shutil.rmtree(error_result_folder_path)

        wsi_names_after_cutoff.append(slide_name)

    if not os.path.exists(h5_path):
        unprocessed_slides.append(slide_name)


print(f"Found {len(unprocessed_slides)} unprocessed slides")
print(f"Found {len(wsi_names_after_cutoff)} slides after {cutoffdatetime}")

# delete all the rows in the metadata that have a wsi_name in wsi_names_after_cutoff
metadata = metadata[~metadata["wsi_name"].isin(wsi_names_after_cutoff)]

# delete all the rows in the metadata that have a wsi_name in unprocessed_slides
metadata = metadata[~metadata["wsi_name"].isin(unprocessed_slides)]

# save the metadata to the same path
metadata.to_csv(metadata_path, index=False)
