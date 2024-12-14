import os
import pandas as pd

CUTOFFDATETIME = "2022-12-10 00:00:00"
# convert the cutoff datetime to a datetime object
CUTOFFDATETIME = pd.to_datetime(CUTOFFDATETIME, format="%Y-%m-%d %H:%M:%S")
HEADERS = ["H24", "H26", "H27"]

slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
metdata_path = "/media/hdd2/neo/SameDayDzsave/same_day_processing_metadata.csv"

# the same_day_processing_metadata.csv should have the following columns
# wsi_name, result_dir_name, datetime_processed, pipeline, specimen_type, datetime_dzsaved, dzsave_error, pipeline_error

metadata_df = pd.read_csv(metdata_path)

# get the list of all the .ndpi files in the slide_source_dir with name starting with something from the HEADERS
all_slide_names = os.listdir(slide_source_dir)
for header in HEADERS:
    slide_names = [
        slide_name
        for slide_name in os.listdir(slide_source_dir)
        if slide_name.startswith(header) and slide_name.endswith(".ndpi")
    ]

    all_slide_names.extend(slide_names)

print(f"Found a total of {len(all_slide_names)} slides.")

def get_slide_datetime(slide_name):
    name = slide_name.split(".ndpi")[0]
    datetime = name.split(" - ")[-1]
    return datetime

example_slide = all_slide_names[0]
example_datetime = get_slide_datetime(example_slide)
# conver the datetime to a datetime object
example_datetime = pd.to_datetime(example_datetime, format="%Y-%m-%d %H.%M.%S")
print(f"Example slide name: {example_slide}, datetime: {example_datetime}")
print(f"The slide is after the cutoff datetime: {example_datetime > CUTOFFDATETIME}")