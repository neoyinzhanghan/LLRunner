import os
import pandas as pd
from LLRunner.config import dzsave_dir, dzsave_metadata_path
from tqdm import tqdm

archive_dir = "/dmpisilon_tools/neo/test_archive"
os.makedirs(archive_dir, exist_ok=True)

# find all the subdirectories in dzsave_dir
subdirs = [f.path for f in os.scandir(dzsave_dir) if f.is_dir()]

# first clean the directory, load the metadata
dzsave_metadata_df = pd.read_csv(dzsave_metadata_path)

# find all the rows in the metadata where the error column is not None or empty string
error_rows = dzsave_metadata_df[
    (dzsave_metadata_df["error"].notnull()) & (dzsave_metadata_df["error"] != "")
]

# print the number of rows with errors
print(f"Number of rows with errors: {len(error_rows)}")

