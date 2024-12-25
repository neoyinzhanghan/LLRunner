import os
import time
import shutil
import datetime
import openslide
import pandas as pd
from argparse import ArgumentParser
from tqdm import tqdm
from LLBMA.front_end.api import analyse_bma
from LLRunner.slide_processing.dzsave_h5 import dzsave_h5
from LLRunner.slide_processing.specimen_clf import (
    get_topview_bma_score,
    get_topview_pbs_score,
)
from sample_N_cells import sample_N_cells

# initialize slide_name as an argparse argument
parser = ArgumentParser()

parser.add_argument("--slide_name", type=str, help="Name of the slide to process")

args = parser.parse_args()

slide_name = args.slide_name
slide_source_dir = "/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
LLBMA_results_dir = "/media/hdd2/neo/debug/SameDayLLBMAResults"
dzsave_dir = "/media/hdd2/neo/debug/SameDayDzsave"
topview_save_dir = "/media/hdd2/neo/tmp_slides_dir/topview"

# remove the LLBMA_results_dir and dzsave_dir if they exist
if os.path.exists(LLBMA_results_dir):
    shutil.rmtree(LLBMA_results_dir)

if os.path.exists(dzsave_dir):
    shutil.rmtree(dzsave_dir)

# create the LLBMA_results_dir and dzsave_dir
os.makedirs(LLBMA_results_dir, exist_ok=True)
os.makedirs(dzsave_dir, exist_ok=True)

# first copy the slide to the tmp_slide_dir
slide_path = os.path.join(slide_source_dir, slide_name)
tmp_slide_path = os.path.join(tmp_slide_dir, slide_name)
dzsave_h5_path = os.path.join(dzsave_dir, slide_name.replace(".ndpi", ".h5"))


print(f"Copying slide from {slide_name} to {tmp_slide_path}")
copy_start_time = time.time()

if not os.path.exists(tmp_slide_path):
    shutil.copy(slide_path, tmp_slide_path)
else:
    print("already copied")
# print("already copied")

slide_copy_time = time.time() - copy_start_time

wsi = openslide.OpenSlide(tmp_slide_path)
# topview is the entire level 7 image
topview = wsi.read_region((0, 0), 7, wsi.level_dimensions[7])
# if RGBA then convert to RGB
if topview.mode == "RGBA":
    topview = topview.convert("RGB")

# save the topview image
topview_path = os.path.join(topview_save_dir, slide_name.replace(".ndpi", ".jpg"))
topview.save(topview_path)

bma_score = get_topview_bma_score(topview)
pbs_score = get_topview_pbs_score(topview)
is_bma = bma_score >= 0.5
is_pbs = pbs_score >= 0.5

print(f"Specimen BMA classification score for {slide_name}: {bma_score}")
print(f"Specimen PBS classification score for {slide_name}: {pbs_score}")

if not is_bma:
    print(f"dzsaving slide {slide_name} to {dzsave_h5_path}")
    dzsave_start_time = time.time()
    dzsave_h5(
        wsi_path=tmp_slide_path,
        h5_path=dzsave_h5_path,
        num_cpus=32,
        tile_size=512,
    )
    dzsave_time = time.time() - dzsave_start_time

else:
    print(f"We will run the LLBMA pipeline on slide {slide_name}")

    pipeline_start_time = time.time()

    # Run the heme_analyze function
    save_path, has_error = analyse_bma(
        slide_path=tmp_slide_path,
        dump_dir=LLBMA_results_dir,
        hoarding=True,
        extra_hoarding=False,
        continue_on_error=False,
        do_extract_features=False,
        check_specimen_clf=False,
        pretiled_h5_path=None,
    )
    pipeline_time = time.time() - pipeline_start_time

    slide_name_no_ext = slide_name.split(".ndpi")[0]
    pipeline_slide_h5_path = os.path.join(LLBMA_results_dir, save_path, "slide.h5")

    # Moving the slide.h5 to the dzsave_dir
    print(f"Moving slide.h5 from {pipeline_slide_h5_path} to {dzsave_h5_path}")
    dzsave_start_time = time.time()
    shutil.move(pipeline_slide_h5_path, dzsave_h5_path)
    dzsave_time = time.time() - dzsave_start_time

    sample_N_cells(os.path.join(LLBMA_results_dir, save_path), N=200)
