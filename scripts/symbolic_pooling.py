import os
from tqdm import tqdm

source_data_dirs = [
    "/media/hdd3/neo/BMA_PCM",
    "/media/hdd3/neo/BMA_AML",
    "/media/hdd3/neo/BMA_Normal",
]

save_dir = "/media/hdd3/neo/tmp_slide_dir"

ndpi_files = []
# get the path to all the NDPI files in the source_data_dirs
for source_data_dir in source_data_dirs:
    for root, dirs, files in os.walk(source_data_dir):
        for file in files:
            if file.endswith(".ndpi"):
                ndpi_files.append(os.path.join(root, file))


for ndpi_file in tqdm(ndpi_files, desc="Copying NDPI files to tmp_slide_dir"):
    # use symbolic links to copy the NDPI files to the save_dir
    os.symlink(ndpi_file, os.path.join(save_dir, os.path.basename(ndpi_file)))