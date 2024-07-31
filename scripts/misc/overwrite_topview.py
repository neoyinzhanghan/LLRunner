import os
import pandas as pd
from tqdm import tqdm
from LLRunner.slide_transfer.slides_management import copy_slide_to_tmp
from LLRunner.config import slide_metadata_path

# open the slide metadata
slide_metadata = pd.read_csv(slide_metadata_path)

for wsi_name in tqdm(slide_metadata["wsi_name"]):
    copy_slide_to_tmp(wsi_name, overwrite=False, overwrite_topview=True)
