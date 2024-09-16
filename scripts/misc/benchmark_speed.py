import numpy as np
import h5py
import os
import imageio
from tqdm import tqdm


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


# create a random h5 file with 1000 rows, 1000 columns, and 32x32 patches
create_random_h5_file("random_data.h5", 400, 400, 32)


def create_random_image_folder(folder_path, rows, columns, patch_size):
    # create the folder if it does not exist
    os.makedirs(folder_path, exist_ok=True)
    # fill the folder with random images
    for i in tqdm(range(rows), desc="Creating random image folder"):
        for j in tqdm(range(columns), leave=False):
            image = np.random.randint(0, 256, (patch_size, patch_size, 3))
            image_path = os.path.join(folder_path, f"image_{i}_{j}.png")
            imageio.imwrite(image_path, image)

create_random_image_folder("random_images", 400, 400, 32)

# now benchmark how long it takes to load 1000 random images from the h5 file vs the image folder
import time


def benchmark_loading_h5(h5_path, rows, columns):
    start = time.time()
    with h5py.File(h5_path, "r") as h5_file:
        for i in tqdm(range(rows), desc="Benchmarking loading h5"):
            for j in tqdm(range(columns), leave=False):
                image = h5_file["data"][i, j]
    return time.time() - start


def benchmark_loading_folder(folder_path, rows, columns):
    start = time.time()
    for i in tqdm(range(rows), desc="Benchmarking loading folder"):
        for j in tqdm(range(columns), leave=False):
            image_path = os.path.join(folder_path, f"image_{i}_{j}.png")
            image = imageio.imread(image_path)
    return time.time() - start


h5_time = benchmark_loading_h5("random_data.h5", 400, 400)
folder_time = benchmark_loading_folder("random_images", 400, 400)
print(f"Loading from h5 took {h5_time:.2f} seconds")
print(f"Loading from folder took {folder_time:.2f} seconds")
