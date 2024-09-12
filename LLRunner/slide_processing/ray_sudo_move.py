import os
from tqdm import tqdm
import ray
import signal
import sys
import shlex  # For safely escaping file paths

# Initialize Ray
ray.init(num_cpus=128)  # Adjust based on the number of CPUs available

directory_to_copy = "/media/hdd3/neo/dzsave_dir/H24-1514;S15;MSKW - 2024-07-10 23.01.33"  # Directory to move to Isilon
save_dir = "/dmpisilon_tools/neo/test_archive"  # Directory to move to

# Escape file paths for safe usage in shell commands

print(f"Copying files from {directory_to_copy} to {save_dir}")

files = []
for (dirpath, dirnames, filenames) in os.walk(directory_to_copy):
    print(f"Traversing directory: {dirpath}")  # Print directories
    print(f"Files: {filenames}")  # Print filenames
    files += [os.path.join(dirpath, file) for file in filenames]

print(f"Number of files found: {len(files)}")  # Output total number of files found

@ray.remote
def copy_file(file, save_dir):
    # Escape file paths for the system command
    file = shlex.quote(file)
    os.system(f"sudo cp {file} {save_dir}")
    return file  # Return file for progress tracking

# Function to handle KeyboardInterrupt and stop Ray properly
def signal_handler(signal, frame):
    print("\nKeyboardInterrupt detected! Shutting down all Ray tasks.")
    ray.shutdown()  # Properly shutdown Ray
    sys.exit(0)  # Exit the program

# Register the signal handler for KeyboardInterrupt
signal.signal(signal.SIGINT, signal_handler)

try:
    # Create futures for file copying
    futures = [copy_file.remote(file, save_dir) for file in files]

    # Progress bar with Ray futures
    with tqdm(total=len(futures), desc="Copying files to Isilon") as pbar:
        # Iterate through futures as they complete
        while futures:
            done, futures = ray.wait(futures, num_returns=1, timeout=None)
            pbar.update(len(done))  # Update the progress bar as tasks complete

except KeyboardInterrupt:
    print("\nKeyboardInterrupt caught. Canceling remaining tasks...")
    # Cancel all remaining futures
    ray.cancel(futures)
    ray.shutdown()  # Shutdown Ray gracefully
    sys.exit(0)

# Shutdown Ray after successful completion
ray.shutdown()
