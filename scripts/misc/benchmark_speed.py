import numpy as np
import h5py
import os
import time
from tqdm import tqdm
from PIL import Image

hdd_h5_path = "/media/hdd3/neo/random_data.h5"
hdd_folder_path = "/media/hdd3/neo/random_images"
isilon_h5_path = "/dmpisilon_tools/neo/dzsave_test/random_data.h5"
isilon_folder_path = "/dmpisilon_tools/neo/dzsave_test/random_images"


def create_random_h5_file(h5_path, rows, columns, patch_size):
    # initialize the h5 file with a dataset named "data", which is an array of shape (rows, columns, patch_size, patch_size, 3)
    with h5py.File(h5_path, "w") as h5_file:
        data = h5_file.create_dataset(
            "data", (rows, columns, patch_size, patch_size, 3), dtype="uint8"
        )
        # fill the dataset with random data
        for i in tqdm(range(rows), desc="Creating random h5 file"):
            for j in tqdm(range(columns), leave=False):
                data[i, j] = np.random.randint(0, 256, (patch_size, patch_size, 3))


starttime = time.time()
# create a random h5 file with 1000 rows, 1000 columns, and 32x32 patches
create_random_h5_file(hdd_h5_path, 400, 400, 32)
h5_creation_time = time.time() - starttime


def create_random_image_folder(folder_path, rows, columns, patch_size):
    # Ensure patch_size is valid
    if patch_size < 2:
        raise ValueError("patch_size must be at least 2.")

    # Create the folder if it does not exist
    os.makedirs(folder_path, exist_ok=True)

    # Fill the folder with random images
    for i in tqdm(range(rows), desc="Creating random image folder"):
        for j in tqdm(range(columns), leave=False):
            image = np.random.randint(
                0, 256, (patch_size, patch_size, 3), dtype=np.uint8
            )
            image_path = os.path.join(folder_path, f"image_{i}_{j}.png")
            Image.fromarray(image).save(image_path)


starttime = time.time()
create_random_image_folder(hdd_folder_path, 400, 400, 32)
folder_creation_time = time.time() - starttime


def benchmark_loading_h5(h5_path, nums_to_load):
    start = time.time()
    with h5py.File(h5_path, "r") as h5_file:
        for i in tqdm(range(nums_to_load), desc="Loading from h5"):
            # randomly sample a row and column
            row = np.random.randint(0, h5_file["data"].shape[0])
            column = np.random.randint(0, h5_file["data"].shape[1])

            # load the image
            image = h5_file["data"][row, column]
    return time.time() - start


def benchmark_loading_folder(folder_path, nums_to_load):
    start = time.time()
    for i in tqdm(range(nums_to_load), desc="Loading from folder"):
        # randomly sample a row and column
        row = np.random.randint(0, 400)
        column = np.random.randint(0, 400)

        # load the image
        image_path = os.path.join(folder_path, f"image_{row}_{column}.png")
        image = np.array(Image.open(image_path))
    return time.time() - start


h5_local_loading_time = benchmark_loading_h5(hdd_h5_path, 10000)
folder_local_loading_time = benchmark_loading_folder(hdd_folder_path, 10000)

# sudo rsync -av the h5 file and the image folder to /dmpisilon_tools/neo/dzsave_test
# and then benchmark how long it takes to load the h5 file vs the image folder from the network
starttime = time.time()
os.system(f"sudo rsync -av {hdd_h5_path} {isilon_h5_path}")
h5_network_copy_time = time.time() - starttime

starttime = time.time()
os.system(f"sudo rsync -av {hdd_folder_path} {"/dmpisilon_tools/neo/dzsave_test"}")
folder_network_copy_time = time.time() - starttime

h5_network_loading_time = benchmark_loading_h5(isilon_h5_path, 10000)
folder_network_loading_time = benchmark_loading_folder(isilon_folder_path, 10000)


print(f"Creating h5 file took {h5_creation_time} seconds")
print(f"Creating image folder took {folder_creation_time} seconds")
print(f"Copying h5 file to network took {h5_network_copy_time} seconds")
print(f"Copying image folder to network took {folder_network_copy_time} seconds")
print(f"Loading from h5 from hdd took {h5_local_loading_time} seconds")
print(f"Loading from folder from hdd took {folder_local_loading_time} seconds")
print(f"Loading from h5 from network took {h5_network_loading_time} seconds")
print(f"Loading from folder from network took {folder_network_loading_time} seconds")
