import os
import time
import openslide
import numpy as np
from LLRunner.slide_processing.dzsave_h5 import dzsave_wsi_name_h5, retrieve_tile_h5
from LLRunner.config import tmp_slide_dir
from tqdm import tqdm

slide_name = "H19-5749;S10;MSKI - 2023-05-24 21.38.53.ndpi"
h5_path = "/dmpisilon_tools/neo/dzsave_bm/6825083.h5"

wsi = openslide.OpenSlide(os.path.join(tmp_slide_dir, slide_name))
width, height = wsi.dimensions
tile_size = 256

num_to_retrieve = 1000

start_time = time.time()
for i in tqdm(range(num_to_retrieve), desc="Retrieving tiles from h5"):
    # find a random level from 0, 1, ... 18
    random_level = np.random.randint(13, 19)
    downsample_factor = 2 ** (18 - random_level)

    # find a random x and y coordinate
    random_x = np.random.randint(0, max((width / downsample_factor) // tile_size, 1))
    random_y = np.random.randint(0, max((height / downsample_factor) // tile_size, 1))

    tile = retrieve_tile_h5(h5_path, random_level, random_x, random_y)

retrieval_time_h5 = time.time() - start_time

print("Retrieval time for h5:", retrieval_time_h5)
