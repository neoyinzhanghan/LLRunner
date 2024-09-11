import os
import time
import pandas as pd
from pathlib import Path
from LLRunner.config import dzsave_dir, dzsave_metadata_path
from tqdm import tqdm

archive_dir = "/dmpisilon_tools/neo/test_archive"
os.makedirs(archive_dir, exist_ok=True)

# Get the list of subdirectories in the dzsave directory
subdir_names = os.listdir(dzsave_dir)

# first clean the directory, load the metadata
dzsave_metadata_df = pd.read_csv(dzsave_metadata_path)

# find all the rows in the metadata where the error column is not None or empty string
error_rows = dzsave_metadata_df[
    (dzsave_metadata_df["error"].notnull()) & (dzsave_metadata_df["error"] != "")
]

# find the list the wsi_name column of the error rows
error_wsi_names = error_rows["wsi_name"].tolist()

# remove the .ndpi at the end of the wsi_name
error_wsi_names = [wsi_name[:-5] for wsi_name in error_wsi_names]

# for each error wsi_name, remove the corresponding subdirectory
subdirs_names = [subdir_name for subdir_name in subdir_names if subdir_name not in error_wsi_names]

# now check if the subdir is actually a directory
subdirs = [os.path.join(dzsave_dir, subdir_name) for subdir_name in subdirs_names if os.path.isdir(os.path.join(dzsave_dir, subdir_name))]

subdirs = [subdirs[0]]

# print the number of rows with errors
print(f"Number of rows with errors: {len(error_rows)}")

profile_metadata = {
    "subdir": [],
    "glv_zipping_time": [],
    "rsync_time": [],
    "isilon_unzipping_time": [],
    "direct_rsync_time": [],
    "folder_size": [],
}

subdirs = [subdirs[0]]

for subdir in tqdm(subdirs, desc="Profilling dzsave archiving"):
    subdir_path = Path(subdir)
    subdir_name = subdir_path.name

    starttime = time.time()
    print(f"Zipping {subdir}")
    # Use sudo zip command for zipping the directory
    os.system(f"sudo zip -r \'{subdir}.zip\' \'{subdir}\'")
    glv_zipping_time = time.time() - starttime

    starttime = time.time()
    print(f"Transferring {subdir}.zip")
    # Use sudo rsync command for transferring the zip file
    os.system(f"sudo rsync -av \'{subdir}.zip\' \'{archive_dir}\'")
    rsync_time = time.time() - starttime
    
    # Unzip the file on the isilon using sudo unzip
    starttime = time.time()
    print(f"Unzipping {subdir}.zip") 
    os.system(f"sudo unzip \'{os.path.join(archive_dir, os.path.basename(subdir))}.zip\' -d \'{archive_dir}\'")
    isilon_unzipping_time = time.time() - starttime


    # now sudo remove the zip file and the unzipped directory
    os.system(f"sudo rm \'{subdir}.zip\'")
    os.system(f"sudo rm \'{os.path.join(archive_dir, subdir_name)}.zip\'")

    starttime = time.time()
    print(f"Directly transferring {subdir}")
    # Use sudo rsync command for transferring the directory
    os.system(f"sudo rsync -av \'{subdir}\' \'{archive_dir}\'")
    direct_rsync_time = time.time() - starttime

    # get the size of subdir in Mb
    folder_size = sum(
        sum(os.path.getsize(os.path.join(dirpath, filename)) for filename in filenames)
        for dirpath, dirnames, filenames in os.walk(subdir)
    )
    folder_size = folder_size / (1024 * 1024)

    profile_metadata["subdir"].append(subdir)
    profile_metadata["glv_zipping_time"].append(glv_zipping_time)
    profile_metadata["rsync_time"].append(rsync_time)
    profile_metadata["isilon_unzipping_time"].append(isilon_unzipping_time)
    profile_metadata["direct_rsync_time"].append(direct_rsync_time)
    profile_metadata["folder_size"].append(folder_size)

    print(f"Zipping time: {glv_zipping_time}")
    print(f"Rsync time: {rsync_time}")
    print(f"Isilon unzipping time: {isilon_unzipping_time}")
    print(f"Direct rsync time: {direct_rsync_time}")
    print(f"Folder size: {folder_size} Mb")


# Save the profiling metadata
profile_metadata_df = pd.DataFrame(profile_metadata)
profile_metadata_df.to_csv(os.path.join(dzsave_dir, "archive_profile_metadata.csv"), index=False)
