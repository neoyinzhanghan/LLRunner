import os
from pathlib import Path
from LLRunner.slide_transfer.slides_management import copy_slide_to_tmp
from LLRunner.slide_transfer.metadata_management import add_bma_diff_metadata_row
from LLRunner.config import *
from tqdm import tqdm

# if the tmp_slide_dir does not exist, then create it
if not os.path.exists(tmp_slide_dir):
    os.makedirs(tmp_slide_dir)

# create the topview subdir if it does not exist
topview_dir = os.path.join(tmp_slide_dir, "topview")
if not os.path.exists(topview_dir):
    os.makedirs(topview_dir)

# create the slide_metadata_path if it does not exist (which is a csv file)
if not os.path.exists(slide_metadata_path):
    with open(slide_metadata_path, "w") as f:
        f.write("wsi_name,accession_number,wsi_extension,part_description,reported_BMA,reported_PB,metadata_last_updated,Dx,sub_Dx,slide_last_updated,in_tmp_slide_dir,level_0_mpp,topview_error,level_0_mpp_error\n")

if not os.path.exists(bma_diff_metadata_path):
    with open(bma_diff_metadata_path, "w") as f:
        f.write("wsi_name,accession_number,wsi_extension,diff_last_updated,blasts,blast-equivalents,promyelocytes,myelocytes,metamyelocytes,neutrophils/bands,monocytes,eosinophils,erythroid precursors,lymphocytes,plasma cells\n")

# get the list of slides in the tmp_slide_dir
slides = os.listdir(tmp_slide_dir)
# only keep the slides that have an extension in allowed_extensions, usse Path
slides = [slide for slide in slides if Path(slide).suffix in allowed_extensions]

# for each slide in the tmp_slide_dir, run copy_slide_to_tmp with overwrite=False and overwrite_topview=True
num_slides = len(slides)
for slide in tqdm(slides, desc="Initialize slides metadata and topviews for tmp_slide_dir)", total=num_slides):
    copy_slide_to_tmp(slide, overwrite=False, overwrite_topview=True)
    add_bma_diff_metadata_row(slide)