import os
import pandas as pd
from tqdm import tqdm
from LLRunner.slide_processing.concurrent_processing import main_concurrent_processing


error_results_df_path = "/home/greg/Documents/neo/LL-Eval/pipeline_error_df.csv"
error_results_df = pd.read_csv(error_results_df_path)

pipeline_run_history_path = (
    "/media/hdd3/neo/glv3_results_dir_batched/pipeline_run_history.csv"
)

# only keep the rows where specimen_type is BMA
error_results_df = error_results_df[error_results_df["specimen_type"] == "BMA"]

# ge the result_dir_name column as a list of strings
result_dir_names = error_results_df["result_dir_name"].tolist()

# get the pipeline_run_history_df
pipeline_run_history_df = pd.read_csv(pipeline_run_history_path)

list_of_slide_names = []

for result_dir_name in tqdm(result_dir_names, desc="Getting slide names"):
    pipeline, datetime_processed = result_dir_name.split("_")

    # look for the row in the pipeline_run_history_df with pipeline and datetime_processed matching
    matching_row = pipeline_run_history_df[
        (pipeline_run_history_df["pipeline"] == pipeline)
        & (pipeline_run_history_df["datetime_processed"] == datetime_processed)
    ]

    # if no row found raise an error
    if len(matching_row) == 0:
        raise ValueError(f"No matching row found for {result_dir_name}")

    # get the wsi_name from the matching row
    wsi_name = matching_row["wsi_name"].values[0]

    list_of_slide_names.append(wsi_name)


list_of_slide_names = list_of_slide_names[
    :5
]  # TODO remove this line after debugging is complete


print(f"Number of slide names found: {len(list_of_slide_names)}")


def error_slide_name_filter(wsi_name):
    return wsi_name in list_of_slide_names


def identity_filter(pipeline_history_df):
    return pipeline_history_df


note = "Rerunning error slide with V3 pipeline. Started on December 11th, 2024."

main_concurrent_processing(
    wsi_name_filter_func=error_slide_name_filter,
    processing_filter_func=identity_filter,
    num_rsync_workers=4,
    note=note,
    delete_slide=True,
)
