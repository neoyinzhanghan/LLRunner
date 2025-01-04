import os
import subprocess

# Configuration
remote_ip = "172.28.164.44"
remote_user = "greg"
remote_backup_dir = "/media/hdd2"
storage_dir = "/media/hdd2"
matching_subdirs = ["SameDayDzsave", "SameDayLLBMAResults"]

# rsync options for incremental backup
rsync_options = [
    "-avz",  # Archive mode, verbose, compress
    "--update",  # Skip files that are newer on the receiver
    "--progress",  # Show progress during transfer
    "--ignore-existing",  # Ignore files already on the remote
]

# Iterate through matching subdirectories
for subdir in matching_subdirs:
    local_path = os.path.join(storage_dir, subdir)
    remote_path = f"{remote_user}@{remote_ip}:{remote_backup_dir}/{subdir}"

    # Ensure the local path exists
    if os.path.exists(local_path):
        # Construct rsync command
        command = ["rsync"] + rsync_options + [local_path + "/", remote_path]
        print(f"Syncing {local_path} to {remote_path}...")

        try:
            # Run the rsync command and wait for it to complete
            subprocess.run(command, check=True)
            print(f"Successfully synced {local_path} to {remote_path}.")
        except subprocess.CalledProcessError as e:
            print(f"Error syncing {local_path} to {remote_path}: {e}")
    else:
        print(f"Local directory {local_path} does not exist, skipping.")
