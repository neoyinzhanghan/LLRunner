import os
from tqdm import tqdm

LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"

# find the paths to all the subdirectories in the LLBMA_results_dir that start with ERROR_
error_subdirs = [
    subdir
    for subdir in os.listdir(LLBMA_results_dir)
    if os.path.isdir(os.path.join(LLBMA_results_dir, subdir))
    and subdir.startswith("ERROR_")
]

error_names = []

for error_subdir in tqdm(error_subdirs, desc="Finding error messages"):
    # open the error.txt file in the error_subdir as a string
    error_txt_path = os.path.join(LLBMA_results_dir, error_subdir, "error.txt")

    with open(error_txt_path, "r") as f:
        error_message = f.read()

    if (
        error_message not in error_names
        and "Too few focus regions found" not in error_message
        and "Too few candidates found" not in error_message
    ):
        error_names.append(error_message)

print(f"Number of distinct error messages: {len(error_names)}")
print("Distinct error messages:")
for error_name in error_names:
    print(error_name)
