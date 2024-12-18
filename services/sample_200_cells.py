import os
import pandas as pd

result_dir_path = "/media/hdd2/neo/test_slide_result_dir"

high_mag_region_result_path = os.path.join(
    result_dir_path, "focus_regions", "high_mag_focus_regions_info.csv"
)

low_mag_region_result_path = os.path.join(
    result_dir_path, "focus_regions", "focus_regions_info.csv"
)

cell_info_path = os.path.join(result_dir_path, "cells", "cells_info.csv")


# open all these paths as dataframes
high_mag_region_result_df = pd.read_csv(high_mag_region_result_path)
low_mag_region_result_df = pd.read_csv(low_mag_region_result_path)
cell_info_df = pd.read_csv(cell_info_path)

# print the columns for all these dataframes
print("High Mag Region Result Columns:")
print(high_mag_region_result_df.columns)

print("\nLow Mag Region Result Columns:")
print(low_mag_region_result_df.columns)

print("\nCell Info Columns:")
print(cell_info_df.columns)

# sort the high_mag_region_result_df by the confidence score in descending order
high_mag_region_result_df = high_mag_region_result_df.sort_values(
    by="adequate_confidence_score_high_mag", ascending=False
)

import sys

sys.exit()

# iterate through the rows of the high_mag_region_result_df
for idx, row in high_mag_region_result_df.iterrows():
    print(row["adequate_confidence_score_high_mag"])
    focus_region_idx = row["idx"]

    # look for all the rows in the the cells df that have the focus_region_idx column equal to the focus_region_idx
    cell_info_df_filtered = cell_info_df.loc[
        cell_info_df["focus_region_idx"] == focus_region_idx
    ]

    # print the number of cells in this focus region
    print(
        f"Number of cells in focus region {focus_region_idx}: {cell_info_df_filtered.shape[0]}"
    )
