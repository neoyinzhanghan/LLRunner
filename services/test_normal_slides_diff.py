import os
from get_copath_data import get_path_data, get_diff

normal_slides_dir = "/media/hdd2/neo/BMA_Normal_lite"
normal_slides_names = os.listdir(normal_slides_dir)

accession_numbers = [slide_name.split(";")[0] for slide_name in normal_slides_names]

copath_data = get_path_data(accession_numbers)

diff_data = get_diff(copath_data)

print(f"Number of slides: {len(normal_slides_names)}")
print(f"Number of diffs: {len(diff_data)}")