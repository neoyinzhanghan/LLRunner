import os
import pandas as pd
from tqdm import tqdm

cutoffdatetime = "2025-01-03 00:00:00"
metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"


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

# traverse through the rows of the metadata
for index, row in tqdm(metadata.iterrows(), total=metadata.shape[0]):
    slide_name = row["wsi_name"]
    slide_datetime = get_slide_datetime(slide_name)

    # check if the slide_datetime is less than the cutoffdatetime
    if slide_datetime > pd.to_datetime(cutoffdatetime):
        wsi_names_after_cutoff.append(slide_name)


print(f"Found {len(wsi_names_after_cutoff)} slides after {cutoffdatetime}")
