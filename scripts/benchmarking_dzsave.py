import os
import time
import openslide
import numpy as np
from LLRunner.config import tmp_slide_dir
from LLRunner.slide_processing.dzsave import dzsave_wsi_name, retrieve_tile
from LLRunner.slide_processing.dzsave_h5 import dzsave_wsi_name_h5, retrieve_tile_h5

slide_name = "H19-5749;S10;MSKI - 2023-05-24 21.38.53.ndpi"
isilon_dir = "/dmpisilon_tools/neo/dzsave_bm"

h5_path = "/media/hdd3/neo/dzsave_dir/H19-5749;S10;MSKI - 2023-05-24 21.38.53.h5"
dzsave_dir = "/media/hdd3/neo/dzsave_dir/H19-5749;S10;MSKI - 2023-05-24 21.38.53"

example_img_dir = "/media/hdd3/neo/example_img_dir"
dzsave_metadata_path = os.path.join("/media/hdd3/neo/dzsave_dir", "dzsave_metadata.csv")


def clear_metadata():
    ## remove all rows but keep the header for the columns
    with open(dzsave_metadata_path, "r") as f:
        lines = f.readlines()

    with open(dzsave_metadata_path, "w") as f:
        f.write(lines[0])


os.makedirs(dzsave_dir, exist_ok=True)
os.makedirs(os.path.join(dzsave_dir, "h5"), exist_ok=True)
os.makedirs(os.path.join(dzsave_dir, "dzi"), exist_ok=True)

wsi = openslide.OpenSlide(os.path.join(tmp_slide_dir, slide_name))
width, height = wsi.dimensions
tile_size = 2048

start_time = time.time()
# print("DZSaving slide as H5...")
# clear_metadata()
# dzsave_wsi_name_h5(
#     slide_name,
#     tile_size=2048,
#     num_cpus=128,
#     region_cropping_batch_size=256,
# )

dzsave_h5_time = time.time() - start_time

start_time = time.time()
# print("DZSaving slide as DZI...")
# clear_metadata()
# dzsave_wsi_name(
#     slide_name,
#     tile_size=2048,
#     num_cpus=128,
#     region_cropping_batch_size=256,
# )

dzsave_time = time.time() - start_time

h5_file_size = os.path.getsize(h5_path)
dzsave_dir_size = sum(
    os.path.getsize(os.path.join(dzsave_dir, f))
    for f in os.listdir(dzsave_dir)
    if f.endswith(".dzi") or f.endswith(".jpeg")
)

start_time = time.time()
# print("Rsyncing h5 file to isilon...")
# os.system(f"sudo rsync -av '{h5_path}' {isilon_dir}")
rsync_h5_time = time.time() - start_time

start_time = time.time()
# print("Rsyncing dzsave dir to isilon...")
# os.system(f"sudo rsync -av '{dzsave_dir}' {isilon_dir}")
rsync_dzsave_time = time.time() - start_time


retrieval_time_h5 = 0
retrieval_time_dzsave = 0
num_to_retrieve = 100

for i in range(num_to_retrieve):
    # find a random level from 0, 1, ... 18
    random_level = np.random.randint(0, 19)
    downsample_factor = 2 ** (18 - random_level)

    # find a random x and y coordinate
    random_x = np.random.randint(0, max((width / downsample_factor) // tile_size, 1))
    random_y = np.random.randint(0, max((height / downsample_factor) // tile_size, 1))

    start_time = time.time()
    h5_tile = retrieve_tile_h5(h5_path, random_level, random_x, random_y)
    retrieval_time_h5 += time.time() - start_time

    # save the tile as a jpeg image in the example_img_dir/h5
    h5_tile.save(os.path.join(example_img_dir, "h5", f"{random_x}_{random_y}.jpeg"))

    start_time = time.time()
    tile_path = retrieve_tile(dzsave_dir, random_level, random_x, random_y)
    retrieval_time_dzsave += time.time() - start_time

    # save the tile as a jpeg image in the example_img_dir/dzsave
    tile_path.save(
        os.path.join(example_img_dir, "dzsave", f"{random_x}_{random_y}.jpeg")
    )


h5_size_mb = h5_file_size / 1e6
dzsave_size_mb = dzsave_dir_size / 1e6

print(f"DZSave H5 time: {dzsave_h5_time}")
print(f"DZSave time: {dzsave_time}")
print(f"Rsync H5 time: {rsync_h5_time} for {h5_size_mb} MB")
print(f"Rsync DZSave time: {rsync_dzsave_time} for {dzsave_size_mb} MB")
print(
    f"Retrieval H5 time on isilon: {retrieval_time_h5} for {num_to_retrieve} random tiles"
)
print(
    f"Retrieval DZSave time on isilon: {retrieval_time_dzsave} for {num_to_retrieve} random tiles"
)
