import os
import time
import shutil
import datetime
import pandas as pd
from LLRunner.slide_processing.dzsave_h5 import dzsave_h5

cutoffdatetime = "2024-12-10 00:00:00"
# convert the cutoff datetime to a datetime object
cutoffdatetime = pd.to_datetime(cutoffdatetime, format="%Y-%m-%d %H:%M:%S")
headers = ["H24", "H25", "H26"]

slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
metdata_path = "/media/hdd2/neo/SameDayDzsave/same_day_processing_metadata.csv"

# the same_day_processing_metadata.csv should have the following columns
# wsi_name, result_dir_name, datetime_processed, pipeline, specimen_type, datetime_dzsaved, dzsave_error, pipeline_error, slide_copy_time, dzsave_time, pipeline_time

metadata_df = pd.read_csv(metdata_path)

# get the list of all the .ndpi files in the slide_source_dir with name starting with something from the HEADERS
all_slide_names = []
for header in headers:
    slide_names = [
        slide_name
        for slide_name in os.listdir(slide_source_dir)
        if slide_name.startswith(header) and slide_name.endswith(".ndpi")
        and os.path.isfile(os.path.join(slide_source_dir, slide_name))
    ]

    all_slide_names.extend(slide_names)

print(f"Found a total of {len(all_slide_names)} slides.")

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

# get the list of all the slides that are newer than the CUTOFFDATETIME
newer_slides = []

for slide_name in all_slide_names:
    slide_datetime = get_slide_datetime(slide_name)
    if slide_datetime > cutoffdatetime:
        newer_slides.append(slide_name)

print(f"Found a total of {len(newer_slides)} slides newer than the cutoff datetime.")

def process_slide(slide_name):
    # first copy the slide to the tmp_slide_dir
    slide_path = os.path.join(slide_source_dir, slide_name)
    tmp_slide_path = os.path.join(tmp_slide_dir, slide_name)
    dzsave_h5_path = os.path.join(dzsave_dir, slide_name.replace(".ndpi", ".h5"))

    new_metadata_row_dict = {
        "wsi_name": slide_name,
        "result_dir_name": None,
        "datetime_processed": None,
        "pipeline": None,
        "specimen_type": None,
        "datetime_dzsaved": None,
        "slide_copy_error": None,
        "dzsave_error": None,
        "pipeline_error": None,
        "slide_copy_time": None,
        "dzsave_time": None,
        "pipeline_time": None,
    }

    print(f"Copying slide from {slide_name} to {tmp_slide_path}")
    copy_start_time = time.time()
    try:
        # shutil.copy(slide_path, tmp_slide_path)
        print("already copied")
    except Exception as e:
        print(f"Error copying slide {slide_name}: {e}")
        new_metadata_row_dict["slide_copy_error"] = str(e)
    slide_copy_time = time.time() - copy_start_time
    new_metadata_row_dict["slide_copy_time"] = slide_copy_time
    print(f"Slide copy completed. Took {slide_copy_time} seconds.")

    print(f"dzsaving slide {slide_name} to {dzsave_h5_path}")
    dzsave_start_time = time.time()
    try:
        dzsave_h5(wsi_path=tmp_slide_path, h5_path=dzsave_h5_path, num_cpus=32, tile_size=512)
    except Exception as e:
        print(f"Error dzsaving slide {slide_name}: {e}")
        new_metadata_row_dict["dzsave_error"] = str(e)
    dzsave_time = time.time() - dzsave_start_time
    new_metadata_row_dict["dzsave_time"] = dzsave_time
    new_metadata_row_dict["datetime_dzsaved"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(new_metadata_row_dict)

process_slide(all_slide_names[0])
