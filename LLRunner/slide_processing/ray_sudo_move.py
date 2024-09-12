import os
from tqdm import tqdm
import ray
import signal
import sys
import shlex  # For safely escaping file paths

# Define the source and destination directories
directory_to_copy = "/media/hdd3/neo/dzsave_dir/H24-1514;S15;MSKW - 2024-07-10 23.01.33"  # Directory to move to Isilon
save_dir = "/dmpisilon_tools/neo/test_archive"  # Directory to move to
batch_size = 20  # Adjust based on system capacity and optimal performance

# Ensure the destination directory exists
if not os.path.exists(save_dir):
    os.makedirs(save_dir)

print(f"Copying files from {directory_to_copy} to {save_dir}")

# Collect all files in the directory
files = []
for (dirpath, dirnames, filenames) in os.walk(directory_to_copy):
    files += [os.path.join(dirpath, file) for file in filenames]

print(f"Number of files found: {len(files)}")

# Initialize Ray with the available number of CPUs
ray.init(num_cpus=128)  # Adjust based on available CPUs

# Function to copy a batch of files using rsync for better performance
@ray.remote
def copy_files(files, save_dir):
    # Escape and join the file paths for the rsync command
    files = ' '.join([shlex.quote(file) for file in files])
    # Use rsync to copy files efficiently
    os.system(f"sudo rsync -av {files} {save_dir}")
    return files  # Return the batch of files copied

# Handle KeyboardInterrupt to stop Ray properly
def signal_handler(signal, frame):
    print("\nKeyboardInterrupt detected! Shutting down all Ray tasks.")
    ray.shutdown()  # Properly shut down Ray
    sys.exit(0)  # Exit the program

# Register the signal handler for KeyboardInterrupt
signal.signal(signal.SIGINT, signal_handler)

# Create batches of files for more efficient copying
file_batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

try:
    # Create futures for each batch of files
    futures = [copy_files.remote(batch, save_dir) for batch in file_batches]

    # Progress bar with Ray futures to track the copying process
    with tqdm(total=len(futures), desc="Copying files to Isilon") as pbar:
        # Iterate through futures as they complete
        while futures:
            done, futures = ray.wait(futures, num_returns=1, timeout=None)
            pbar.update(len(done))  # Update progress bar as tasks complete

except KeyboardInterrupt:
    print("\nKeyboardInterrupt caught. Canceling remaining tasks...")
    # Cancel all remaining futures
    ray.cancel(futures)
    ray.shutdown()  # Shutdown Ray gracefully
    sys.exit(0)

# Shutdown Ray after successful completion
ray.shutdown()
