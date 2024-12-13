import os
from LLRunner.slide_processing.dzsave_h5 import dzsave_h5

test_slide_ndpi_path = "/media/hdd3/neo/test_slide_2.ndpi"
save_dir = "/media/hdd3/neo/test_slide_2_dzsave"
os.makedirs(save_dir, exist_ok=True)
h5_path_256 = "test_slide_2_256.h5"
h5_path_512 = "test_slide_2_512.h5"
h5_path_1024 = "test_slide_2_1024.h5"

print("Dzsaving at tile size 256")
dzsave_h5(
    wsi_path=test_slide_ndpi_path,
    h5_path=os.path.join(save_dir, h5_path_256),
    tile_size=256,
    num_cpus=32,
    region_cropping_batch_size=256,
)

print("Dzsaving at tile size 512")
dzsave_h5(
    wsi_path=test_slide_ndpi_path,
    h5_path=os.path.join(save_dir, h5_path_512),
    tile_size=512,
    num_cpus=32,
    region_cropping_batch_size=256,
)

print("Dzsaving at tile size 1024")
dzsave_h5(
    wsi_path=test_slide_ndpi_path,
    h5_path=os.path.join(save_dir, h5_path_1024),
    tile_size=1024,
    num_cpus=32,
    region_cropping_batch_size=256,
)