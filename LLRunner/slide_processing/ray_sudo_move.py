import os
import ray
import signal
import sys
from tqdm import tqdm
import shlex

# Define the source and destination directories
directory_to_copy = "/media/hdd3/neo/dzsave_dir/H24-1514;S15;MSKW - 2024-07-10 23.01.33"
save_dir = "/dmpisilon_tools/neo/test_archive"

# Ensure the destination directory exists
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

# Escape the directory path to handle special characters
escaped_directory_to_copy = shlex.quote(directory_to_copy)
escaped_save_dir = shlex.quote(save_dir)

# Ensure the source directory is not empty before creating the tarball
if len(os.listdir(directory_to_copy)) == 0:
    print("The source directory is empty. Exiting.")
    sys.exit(1)

# Archive all the files into a single tarball
tarball_path = "/tmp/temp_archive.tar"
os.system(f"tar -cf {tarball_path} -C {escaped_directory_to_copy} .")  # Create the tarball

# Use Ray to parallelize the copying of the archive
ray.init(num_cpus=128)

@ray.remote
def copy_tarball(tarball, destination):
    # Escape the paths
    tarball = shlex.quote(tarball)
    destination = shlex.quote(destination)
    # Use cp to copy the tarball to the destination
    os.system(f"sudo cp {tarball} {destination}")
    return tarball

# Function to handle KeyboardInterrupt
def signal_handler(signal, frame):
    print("\nKeyboardInterrupt detected! Shutting down all Ray tasks.")
    ray.shutdown()
    sys.exit(0)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

# Perform the file copy operation
try:
    future = copy_tarball.remote(tarball_path, save_dir)
    with tqdm(total=1, desc="Copying tarball to destination") as pbar:
        ray.get(future)
        pbar.update(1)

except KeyboardInterrupt:
    print("\nKeyboardInterrupt caught. Canceling remaining tasks...")
    ray.shutdown()
    sys.exit(0)

# Shutdown Ray and extract the tarball at the destination
ray.shutdown()
os.system(f"tar -xf {os.path.join(save_dir, 'temp_archive.tar')} -C {escaped_save_dir}")
