import os
import pandas as pd

classes_to_remove = ["U1", "PL2", "PL3", "ER5", "ER6", "U4", "B1", "B2"]

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

regions_to_keep = []
num_cells = 0
num_cells_pre_removal = 0

# iterate through the rows of the high_mag_region_result_df
for idx, row in high_mag_region_result_df.iterrows():
    print(row["adequate_confidence_score_high_mag"])
    focus_region_idx = row["idx"]

    # look for all the rows in the the cells df that have the focus_region_idx column equal to the focus_region_idx
    cell_info_df_filtered = cell_info_df.loc[
        cell_info_df["focus_region_idx"] == focus_region_idx
    ]

    before_removal = cell_info_df_filtered.shape[0]

    # print the number of cells in this focus region
    print(
        f"Number of cells in focus region {focus_region_idx}: {cell_info_df_filtered.shape[0]}"
    )

    # remove all the rows where label is in classes_to_remove
    cell_info_df_filtered = cell_info_df_filtered.loc[
        ~cell_info_df_filtered["label"].isin(classes_to_remove)
    ]

    print(
        f"Number of cells in focus region {focus_region_idx} after removing classes to remove: {cell_info_df_filtered.shape[0]}"
    )

    if cell_info_df_filtered.shape[0] > 0:
        regions_to_keep.append(focus_region_idx)

        num_cells += cell_info_df_filtered.shape[0]
        num_cells_pre_removal += before_removal

    if num_cells > 200:
        break

print(f"Number of regions to keep: {len(regions_to_keep)}")
print(f"Number of cells: {num_cells}")
print(f"Number of cells before removal: {num_cells_pre_removal}")
