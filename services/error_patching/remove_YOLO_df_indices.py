import os
import pandas as pd
from tqdm import tqdm

LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"

# find all the subdirectories of the LLBMA_results_dir
subdirs = [
    subdir
    for subdir in os.listdir(LLBMA_results_dir)
    if os.path.isdir(os.path.join(LLBMA_results_dir, subdir))
]

YOLO_df_paths = []

for result_dir in tqdm(subdirs, desc="Finding YOLO df paths", total=len(subdirs)):
    result_dir_path = os.path.join(LLBMA_results_dir, result_dir)

    YOLO_df_dir_path = os.path.join(result_dir_path, "focus_regions", "YOLO_df")

    # find the paths to all the csv files in the YOLO_df_dir_path
    YOLO_df_paths += [
        os.path.join(YOLO_df_dir_path, csv_file)
        for csv_file in os.listdir(YOLO_df_dir_path)
        if csv_file.endswith(".csv")
    ]

print(f"Found {len(YOLO_df_paths)} YOLO dataframes")
