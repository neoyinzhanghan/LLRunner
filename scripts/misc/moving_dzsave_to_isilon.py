import os
import time
import pandas as pd
import shutil
import zipfile
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

profile_metadata = {
    "subdir": [],
    "glv_zipping_time": [],
    "rsync_time": [],
    "isilon_unzipping_time": [],
}

for subdir in tqdm(subdirs, desc="Profilling dzsave archiving"):
    starttime = time.time()
    
    # Create a zip file using shutil
    zip_file_path = f"{subdir}.zip"
    shutil.make_archive(subdir, 'zip', subdir)
    glv_zipping_time = time.time() - starttime

    starttime = time.time()
    # Use sudo rsync command for transferring the zip file
    os.system(f"sudo rsync -av {zip_file_path} {archive_dir}")
    rsync_time = time.time() - starttime
    
    # Unzip the file on the isilon using zipfile
    starttime = time.time()
    with zipfile.ZipFile(os.path.join(archive_dir, f"{os.path.basename(subdir)}.zip"), 'r') as zip_ref:
        zip_ref.extractall(os.path.join(archive_dir, os.path.basename(subdir)))
    isilon_unzipping_time = time.time() - starttime

    profile_metadata["subdir"].append(subdir)
    profile_metadata["glv_zipping_time"].append(glv_zipping_time)
    profile_metadata["rsync_time"].append(rsync_time)
    profile_metadata["isilon_unzipping_time"].append(isilon_unzipping_time)

# Save the profiling metadata
profile_metadata_df = pd.DataFrame(profile_metadata)
profile_metadata_df.to_csv(os.path.join(dzsave_dir, "archive_profile_metadata.csv"), index=False)
