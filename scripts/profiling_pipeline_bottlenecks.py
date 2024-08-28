import time
import random
import os
import zipfile
import subprocess
import pyvips
import pandas as pd
from LLRunner.slide_transfer.sshos import SSHOS
from LLRunner.config import (
    slide_source_dir,
    slide_source_hostname,
    slide_source_username,
)
from tqdm import tqdm

# Configuration and parameters
num_slides = 3
tmp_test_dir = "/media/hdd3/neo/tmp_test_dir"
dzsave_dir = "/media/hdd3/neo/tmp_dzsave_dir"
archive_dir = "/dmpisilon_tools/neo/test"

profile_metadata = {
    "slide_name": [],
    "slide_file_size": [],
    "moving_to_tmp_time": [],
    "moving_to_tmp_num_attempts": [],
    "dzsave_time": [],
    "zipping_time": [],
    "moving_to_archive_time": [],
    "unzipping_time": [],
}

with SSHOS() as sshos:
    # Find all the .ndpi files in the slide source directory
    slide_files = [
        f for f in sshos.sftp.listdir(slide_source_dir) if f.endswith(".ndpi")
    ]

    # Only keep slides that start with H
    slide_files = [f for f in slide_files if f.startswith("H")]

    # Randomly select num_slides slides
    slide_files = random.sample(slide_files, num_slides)

    for slide_file in tqdm(slide_files, desc="Processing slides"):
        slide_path = os.path.join(slide_source_dir, slide_file)
        local_path = os.path.join(tmp_test_dir, slide_file)
        dzsave_output_path = os.path.join(dzsave_dir, slide_file.split(".ndpi")[0])
        zip_output_path = f"{dzsave_output_path}.zip"
        archive_path = os.path.join(archive_dir, os.path.basename(zip_output_path))

        profile_metadata["slide_name"].append(slide_file)

        # Step 1: Rsync file to tmp_test_dir using SSHOS
        start_time = time.time()
        print("Rsyncing to tmp")
        num_attempts = sshos.rsync_file(slide_path, tmp_test_dir)
        moving_to_tmp_time = time.time() - start_time
        print(
            f"Completed rsyncing to tmp in {moving_to_tmp_time} seconds with {num_attempts} attempts"
        )
        profile_metadata["moving_to_tmp_time"].append(moving_to_tmp_time)
        profile_metadata["moving_to_tmp_num_attempts"].append(num_attempts)

        # Step 2: PyVips dzsave to dzsave_tmp_dir
        start_time = time.time()
        print("Running dzsave")
        image = pyvips.Image.new_from_file(local_path)
        image.dzsave(dzsave_output_path, tile_size=512, overlap=0)
        dzsave_time = time.time() - start_time
        print(f"Completed dzsave in {dzsave_time} seconds")
        profile_metadata["dzsave_time"].append(dzsave_time)

        # Step 3: Zip the dzsave output
        start_time = time.time()
        print("Zipping")
        with zipfile.ZipFile(zip_output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(dzsave_output_path):
                for file in files:
                    zipf.write(
                        os.path.join(root, file),
                        os.path.relpath(os.path.join(root, file), dzsave_output_path),
                    )
        zipping_time = time.time() - start_time
        print(f"Completed zipping in {zipping_time} seconds")
        profile_metadata["zipping_time"].append(zipping_time)

        # Step 4: Rsync the zip file to archive_dir using ordinary rsync
        start_time = time.time()
        print("Rsyncing to archive")
        rsync_command = ["rsync", "-avz", zip_output_path, archive_dir]
        subprocess.run(rsync_command, check=True)
        moving_to_archive_time = time.time() - start_time
        print(f"Completed rsyncing to archive in {moving_to_archive_time} seconds")
        profile_metadata["moving_to_archive_time"].append(moving_to_archive_time)

        # Step 5: Unzip the file in archive_dir
        start_time = time.time()
        print("Unzipping")
        with zipfile.ZipFile(archive_path, "r") as zipf:
            zipf.extractall(archive_dir)
        unzipping_time = time.time() - start_time
        print(f"Completed unzipping in {unzipping_time} seconds")
        profile_metadata["unzipping_time"].append(unzipping_time)

# Convert profile_metadata to a DataFrame and save it
df = pd.DataFrame(profile_metadata)
df.to_csv(os.path.join(tmp_test_dir, "profile_metadata.csv"), index=False)
