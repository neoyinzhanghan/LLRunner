import os
import time
import shutil
import datetime
import openslide    
import pandas as pd
from LLBMA.front_end.api import analyse_bma
from LLRunner.slide_processing.dzsave_h5 import dzsave_h5
from LLRunner.slide_processing.specimen_clf import get_topview_bma_score, get_topview_pbs_score

cutoffdatetime = "2024-12-13 14:00:00"
# convert the cutoff datetime to a datetime object
cutoffdatetime = pd.to_datetime(cutoffdatetime, format="%Y-%m-%d %H:%M:%S")
headers = ["H24", "H25", "H26"]

slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"

# the same_day_processing_metadata.csv should have the following columns
# wsi_name, result_dir_name, dzsave_h5_path, datetime_processed, pipeline, datetime_dzsaved, slide_copy_error, dzsave_error, pipeline_error, slide_copy_time, dzsave_time, pipeline_time, bma_score, pbs_score, is_bma, is_pbs
metadata_df = pd.read_csv(metadata_path)

# get the wsi_name column of the metadata_df as a list of strings
wsi_names = metadata_df["wsi_name"].tolist()

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
print(f"This many has already been processed: {len(wsi_names)}")

# only keep the slides that have not been processed
all_slide_names = [slide_name for slide_name in all_slide_names if slide_name not in wsi_names]
print(f"Found a total of {len(all_slide_names)} slides to process.")


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

def process_slide(slide_name, metadata_df):
    # first copy the slide to the tmp_slide_dir
    slide_path = os.path.join(slide_source_dir, slide_name)
    tmp_slide_path = os.path.join(tmp_slide_dir, slide_name)
    dzsave_h5_path = os.path.join(dzsave_dir, slide_name.replace(".ndpi", ".h5"))

    new_metadata_row_dict = {
        "wsi_name": slide_name,
        "result_dir_name": None,
        "dzsave_h5_path": dzsave_h5_path,
        "datetime_processed": None,
        "pipeline": None,
        "datetime_dzsaved": None,
        "slide_copy_error": None,
        "dzsave_error": None,
        "pipeline_error": None,
        "slide_copy_time": None,
        "dzsave_time": None,
        "pipeline_time": None,
        "bma_score": None,
        "pbs_score": None,
        "is_bma": None,
        "is_pbs": None,
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

    print(f"Performing specimen classification on slide {slide_name}")
    try:
        wsi = openslide.OpenSlide(tmp_slide_path)
        # topview is the entire level 7 image
        topview = wsi.read_region((0, 0), 7, wsi.level_dimensions[7])
        # if RGBA then convert to RGB
        if topview.mode == "RGBA":
            topview = topview.convert("RGB")
    
        bma_score = get_topview_bma_score(topview)
        pbs_score = get_topview_pbs_score(topview)
        new_metadata_row_dict["bma_score"] = bma_score
        new_metadata_row_dict["pbs_score"] = pbs_score
        is_bma = bma_score >= 0.5
        is_pbs = pbs_score >= 0.5
        new_metadata_row_dict["is_bma"] = is_bma
        new_metadata_row_dict["is_pbs"] = is_pbs

        print(f"Specimen BMA classification score for {slide_name}: {bma_score}")
        print(f"Specimen PBS classification score for {slide_name}: {pbs_score}")
    
    except Exception as e:
        print(f"Error performing specimen classification on slide {slide_name}: {e}")
        new_metadata_row_dict["pipeline_error"] = str(e)

    if not is_bma:
        print(f"dzsaving slide {slide_name} to {dzsave_h5_path}")
        dzsave_start_time = time.time()
        try:
            dzsave_h5(wsi_path=tmp_slide_path, h5_path=dzsave_h5_path, num_cpus=32, tile_size=512)
        except Exception as e:
            print(f"Error dzsaving slide {slide_name}: {e}")
            new_metadata_row_dict["dzsave_error"] = str(e)
        dzsave_time = time.time() - dzsave_start_time
        new_metadata_row_dict["dzsave_time"] = dzsave_time
        new_metadata_row_dict["dzsave_h5_path"] = dzsave_h5_path
        new_metadata_row_dict["datetime_dzsaved"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    else:
        print(f"We will run the LLBMA pipeline on slide {slide_name}")
        try:
            pipeline_start_time = time.time()
            # Run the heme_analyze function
            analyse_bma(
                slide_path=slide_path,
                dump_dir=LLBMA_results_dir,
                hoarding=True,
                extra_hoarding=False,
                continue_on_error=True,
                do_extract_features=False,
                check_specimen_clf=False,
                pretiled_h5_path=None,
            )
            pipeline_time = time.time() - pipeline_start_time
            new_metadata_row_dict["pipeline"] = "BMA-diff"
            new_metadata_row_dict["pipeline_time"] = pipeline_time
            new_metadata_row_dict["datetime_processed"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            slide_name_no_ext = slide_name.split(".ndpi")[0]
            pipeline_slide_h5_path = os.path.join(LLBMA_results_dir, slide_name_no_ext, "slide.h5")

            # copying the slide.h5 to the dzsave_dir
            print(f"Copying slide.h5 from {pipeline_slide_h5_path} to {dzsave_h5_path}")
            dzsave_start_time = time.time()
            shutil.copy(pipeline_slide_h5_path, dzsave_h5_path)
            dzsave_time = time.time() - dzsave_start_time

            new_metadata_row_dict["result_dir_name"] = os.path.join(LLBMA_results_dir, slide_name_no_ext)
            new_metadata_row_dict["dzsave_h5_path"] = dzsave_h5_path
            new_metadata_row_dict["dzsave_time"] = dzsave_time  


        except Exception as e:
            print(f"Error running LLBMA pipeline on slide {slide_name}: {e}")
            new_metadata_row_dict["pipeline_error"] = str(e)

            print(f"Due to pipeline error, we are dzsaving slide {slide_name} to {dzsave_h5_path}")
            dzsave_start_time = time.time()
            try:
                dzsave_h5(wsi_path=tmp_slide_path, h5_path=dzsave_h5_path, num_cpus=32, tile_size=512)
            except Exception as e:
                print(f"Error dzsaving slide {slide_name}: {e}")
                new_metadata_row_dict["dzsave_error"] = str(e)
            dzsave_time = time.time() - dzsave_start_time
            new_metadata_row_dict["dzsave_time"] = dzsave_time
            new_metadata_row_dict["dzsave_h5_path"] = dzsave_h5_path
            new_metadata_row_dict["datetime_dzsaved"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(new_metadata_row_dict)

    # concat the new metadata row to the metadata_df
    new_metadata_row_df = pd.DataFrame([new_metadata_row_dict])
    metadata_df = pd.concat([metadata_df, new_metadata_row_df], ignore_index=True)

    # save the metadata_df back to the metadata_path
    metadata_df.to_csv(metadata_path, index=False)

for slide in all_slide_names:
    process_slide(slide, metadata_df)
    break
