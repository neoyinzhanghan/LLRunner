import os
import pandas as pd

columns = [
    "wsi_name",
    "result_dir_name",
    "dzsave_h5_path",
    "datetime_processed",
    "pipeline",
    "datetime_dzsaved",
    "slide_copy_error",
    "dzsave_error",
    "pipeline_error",
    "slide_copy_time",
    "dzsave_time",
    "pipeline_time",
    "bma_score",
    "pbs_score",
    "is_bma",
    "is_pbs",
]


def initialize_minimal_servcice_csv_file(csv_file_path):
    # if a file already exists, raise an error
    if os.path.exists(csv_file_path):
        raise FileExistsError(f"File {csv_file_path} already exists.")

    # create a new empty dataframe with the columns
    df = pd.DataFrame(columns=columns)

    # save the dataframe to the csv file
    df.to_csv(csv_file_path, index=False)
    print(f"Created new csv file at {csv_file_path}")