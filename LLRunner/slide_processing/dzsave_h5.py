import os
import ray
import time
import h5py
import openslide
import pandas as pd
import numpy as np
from PIL import Image
from pathlib import Path
from tqdm import tqdm
from LLRunner.config import dzsave_dir, dzsave_metadata_path, tmp_slide_dir


def create_list_of_batches_from_list(list, batch_size):
    """
    This function creates a list of batches from a list.

    :param list: a list
    :param batch_size: the size of each batch
    :return: a list of batches

    >>> create_list_of_batches_from_list([1, 2, 3, 4, 5], 2)
    [[1, 2], [3, 4], [5]]
    >>> create_list_of_batches_from_list([1, 2, 3, 4, 5, 6], 3)
    [[1, 2, 3], [4, 5, 6]]
    >>> create_list_of_batches_from_list([], 3)
    []
    >>> create_list_of_batches_from_list([1, 2], 3)
    [[1, 2]]
    """

    list_of_batches = []

    for i in range(0, len(list), batch_size):
        batch = list[i : i + batch_size]
        list_of_batches.append(batch)

    return list_of_batches


def initialize_h5py_file(h5_path, img_height, img_width, patch_size=256):
    """
    Create an HDF5 file with a dataset that stores tiles, indexed by row and column.
    
    Parameters:
        h5_path (str): Path where the HDF5 file will be created.
        image_shape (tuple): Shape of the full image (height, width, channels).
        patch_size (int): The size of each image patch (default: 256).
    
    Raises:
        AssertionError: If the file already exists at h5_path.
    """
    assert not os.path.exists(h5_path), f"Error: {h5_path} already exists."
    
    
    # Calculate the number of rows and columns of tiles
    num_tile_rows = int(np.ceil(img_height / patch_size))
    num_tile_columns = int(np.ceil(img_width / patch_size))
    
    # Create the HDF5 file and dataset
    with h5py.File(h5_path, "w") as f:
        # Create dataset with shape (num_tile_rows, num_tile_columns, patch_size, patch_size, 3)
        f.create_dataset(
            "tiles", 
            shape=(num_tile_rows, num_tile_columns, patch_size, patch_size, 3), 
            dtype='uint8'
        )

    # create dataset Heights, Widths, TileSize, Overlap, Format
    with h5py.File(h5_path, "a") as f:
        f.create_dataset("Heights", data=img_height)
        f.create_dataset("Widths", data=img_width)
        f.create_dataset("TileSize", data=patch_size)
        f.create_dataset("Overlap", data=0)
        f.create_dataset("Format", data="jpeg")
    
    print(f"Initialized HDF5 file at {h5_path} with shape {num_tile_rows} x {num_tile_columns} for tiles.")

def add_patch_to_h5py(h5_path, level, patch, row, column):
    """
    Add a patch to an HDF5 file at the specified row and column at the dataste named str(level).
    """
    with h5py.File(h5_path, "a") as f:
        f[f"{level}"][row, column] = patch

@ray.remote
class WSICropManager:
    """
    A class representing a manager that crops WSIs.
    Each Manager object is assigned with a single CPU core and is responsible for cropping a subset of the coordinates from a given WSI.

    Attributes:
    wsi_path: str: The path to the WSI.
    wsi: openslide.OpenSlide: The WSI object.

    """

    def __init__(self, wsi_path) -> None:
        self.wsi_path = wsi_path
        self.wsi = None

    def open_slide(self):
        """Open the WSI."""
        self.wsi = openslide.OpenSlide(self.wsi_path)

    def close_slide(self):
        """Close the WSI."""
        self.wsi.close()
        self.wsi = None

    def get_level_0_dimensions(self):
        """Get dimensions of the slide at level 0."""
        if self.wsi is None:
            self.open_slide()
        return self.wsi.dimensions

    def get_level_N_dimensions(self, level):
        """Get dimensions of the slide at level N."""
        if self.wsi is None:
            self.open_slide()
        return self.wsi.level_dimensions[level]

    def get_tile_coordinate_level_pairs(self, tile_size=256, level=0):
        """Generate a list of coordinates_leve for 256x256 disjoint patches."""
        if self.wsi is None:
            self.open_slide()

        width, height = self.get_level_N_dimensions(level)
        coordinates = []

        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                # Ensure that the patch is within the image boundaries

                coordinates.append(
                    (
                        (x, y, min(x + tile_size, width), min(y + tile_size, height)),
                        level,
                    )
                )

        return coordinates

    def crop(self, coords, level=0):
        """Crop the WSI at the specified level of magnification."""
        if self.wsi is None:
            self.open_slide()

        coords_level_0 = (
            coords[0] * (2**level),
            coords[1] * (2**level),
            coords[2] * (2**level),
            coords[3] * (2**level),
        )

        image = self.wsi.read_region(
            (coords_level_0[0], coords_level_0[1]),
            level,
            (coords[2] - coords[0], coords[3] - coords[1]),
        )

        image = image.convert("RGB")
        return image

    def async_get_bma_focus_region_level_pair_batch(
        self, focus_region_coords_level_pairs, h5_path, crop_size=256
    ):
        """Save a list of focus regions."""
        for focus_region_coord_level_pair in focus_region_coords_level_pairs:
            focus_region_coord, level = focus_region_coord_level_pair

            image = self.crop(focus_region_coord, level=level)
            # Save the image to a .jpeg file in save_dir

            padded_image = padding_image(image, patch_size=crop_size)

            x, y = int(focus_region_coord[0] // crop_size), int(focus_region_coord[1] // crop_size)

            add_patch_to_h5py(
                h5_path,
                level,
                np.array(padded_image),
                x,
                y
            )

        return len(focus_region_coords_level_pairs)


def padding_image(image, patch_size=256):
    """First check that both dim of the image is <= patch_size, if not raise an error.
    Then pad the image to make it patch_size x patch_size by adding black pixels to the right and bottom.
    """

    image_width = image.width
    image_height = image.height
    assert image_width <= patch_size, f"Error: Image width {image.width} is greater than patch_size {patch_size}."
    assert image_height <= patch_size, f"Error: Image height {image.height} is greater than patch_size {patch_size}."

    if image_height == patch_size and image_width == patch_size:
        return image
    else:
        new_image = Image.new("RGB", (patch_size, patch_size), (0, 0, 0))
        new_image.paste(image, (0, 0))
    return new_image

def crop_wsi_images_all_levels(
    wsi_path,
    h5_path,
    region_cropping_batch_size,
    crop_size=256,
    verbose=True,
    num_cpus=96,
):
    num_croppers = num_cpus  # Number of croppers is the same as num_cpus

    if verbose:
        print("Initializing WSICropManager")

    manager = WSICropManager.remote(wsi_path)

    # Get all the coordinates for 256x256 patches
    focus_regions_coordinates = []

    for level in range(0, 8):
        focus_regions_coordinates.extend(
            ray.get(
                manager.get_tile_coordinate_level_pairs.remote(
                    tile_size=crop_size, level=level
                )
            )
        )
    list_of_batches = create_list_of_batches_from_list(
        focus_regions_coordinates, region_cropping_batch_size
    )

    task_managers = [WSICropManager.remote(wsi_path) for _ in range(num_croppers)]

    tasks = {}

    for i, batch in enumerate(list_of_batches):
        manager = task_managers[i % num_croppers]
        task = manager.async_get_bma_focus_region_level_pair_batch.remote(
            batch, h5_path, crop_size=crop_size
        )
        tasks[task] = batch

    with tqdm(
        total=len(focus_regions_coordinates), desc="Cropping focus regions"
    ) as pbar:
        while tasks:
            done_ids, _ = ray.wait(list(tasks.keys()))

            for done_id in done_ids:
                try:
                    batch = ray.get(done_id)
                    pbar.update(batch)

                except ray.exceptions.RayTaskError as e:
                    print(f"Task for batch {tasks[done_id]} failed with error: {e}")

                del tasks[done_id]


def get_depth_from_0_to_11(wsi_path, save_dir, tile_size=256):
    # the depth 11 image the the level 7 image from the slide
    # each depth decrease is a downsample by factor of 2

    # get the depth_11 image
    wsi = openslide.OpenSlide(wsi_path)
    level_7_dimensions = wsi.level_dimensions[7]
    image = wsi.read_region((0, 0), 7, level_7_dimensions)
    image = image.convert("RGB")

    current_image = image
    for depth in range(10, -1, -1):
        # downsample the image by a factor of 2
        current_image = current_image.resize(
            (max(current_image.width // 2, 1), max(current_image.height // 2, 1))
        )

        # print("Range debugging")
        # print(len(range(0, current_image.height, tile_size)))
        # print(len(range(0, current_image.width, tile_size)))

        # crop 256x256 patches from the downsampled image (don't overlap, dont leave out any boundary patches)
        for y in range(0, current_image.height, tile_size):
            for x in range(0, current_image.width, tile_size):
                # Calculate the right and bottom coordinates ensuring they are within the image boundaries
                right = min(x + tile_size, current_image.width)
                bottom = min(y + tile_size, current_image.height)

                # Crop the patch from the image starting at (x, y) to (right, bottom)
                patch = current_image.crop((x, y, right, bottom))


                padded_patch = padding_image(patch, patch_size=tile_size)

                x, y = int(x // tile_size), int(y // tile_size)

                add_patch_to_h5py(
                    os.path.join(save_dir, f"{depth}.h5"),
                    np.array(padded_patch),
                    x,
                    y
                )

def dzsave(
    wsi_path,
    save_dir,
    h5_name,
    tile_size=256,
    num_cpus=96,
    region_cropping_batch_size=256,
):
    """
    Create a DeepZoom image pyramid from a WSI.
    Save the dz folder structure at save_dir/folder_name_files
    Save the .dzi file at save_dir/folder_name.dzi
    """

    wsi = openslide.OpenSlide(wsi_path)
    height, width = wsi.dimensions

    print(f"Width: {width}, Height: {height}")

    starttime = time.time()

    h5_path = os.path.join(save_dir, h5_name)   
    initialize_h5py_file(
        h5_path=h5_path,
        img_height=height,
        img_width=width,
        patch_size=tile_size,
    )

    print("Cropping from NDPI")
    crop_wsi_images_all_levels(
        wsi_path,
        h5_path,
        region_cropping_batch_size=region_cropping_batch_size,
        crop_size=tile_size,
        num_cpus=num_cpus,
    )
    print("Cropping Lower Resolution Levels")
    get_depth_from_0_to_11(wsi_path, h5_path, tile_size=tile_size)
    time_taken = time.time() - starttime

    return time_taken


def dzsave_wsi_name(wsi_name, tile_size=256):
    """Check if the wsi_name is in the dzsave_metadata_path. If not then create a subfolder in dzsave_dir with the wsi_name with out the extention
    The in that folder save the wsi_name_files folder and the wsi_name.dzi file.
    Add the processing time and datetime processed to the dzsave_metadata_path.
    """
    wsi_name_path = Path(wsi_name)
    wsi_path = os.path.join(tmp_slide_dir, wsi_name)

    assert os.path.exists(wsi_path), f"Error: {wsi_path} does not exist."
    wsi_name_no_ext = wsi_name_path.stem

    dzsave_subdir = os.path.join(dzsave_dir, wsi_name_no_ext)

    # check if the dzsave_subdir exists, if not then create it
    if os.path.exists(dzsave_subdir):
        print(
            f"UserWarning: {dzsave_subdir} already exists. Skipping. This should not be intended behavior and is a sign that something could be wrong."
        )
    else:
        os.makedirs(dzsave_subdir, exist_ok=True)

        folder_name = wsi_name_no_ext
        save_dir = dzsave_subdir

        starttime = time.time()

        try:
            dzsave(
                wsi_path=wsi_path,
                save_dir=save_dir,
                folder_name=folder_name,
                tile_size=tile_size,
                num_cpus=128,
                region_cropping_batch_size=256,
            )
            error = None

        except Exception as e:
            error = str(e)
            print(
                f"UserWarningL: Error: {error} occurred while processing {wsi_name}. While continue on error is on, the error should not be ignored if it happens to too many slides."
            )

        processing_time = time.time() - starttime
        datetime_processed = time.strftime("%Y-%m-%d %H:%M:%S")

        dzsave_metadata_df = pd.read_csv(dzsave_metadata_path)

        new_row = {
            "wsi_name": wsi_name,
            "tile_size": tile_size,
            "processing_time": processing_time,
            "datetime_processed": datetime_processed,
            "error": error,
        }

        new_row_df = pd.DataFrame([new_row])

        # Add a row to the dataframe
        dzsave_metadata_df = pd.concat(
            [dzsave_metadata_df, new_row_df], ignore_index=True
        )

        # Save the dataframe back to the dzsave_metadata_path
        dzsave_metadata_df.to_csv(dzsave_metadata_path, index=False)


def initialize_dzsave_dir():
    os.makedirs(dzsave_dir, exist_ok=True)
    # if the dzsave_metadata_path does not exist, then create it
    if not os.path.exists(dzsave_metadata_path):
        with open(dzsave_metadata_path, "w") as f:
            f.write("wsi_name,tile_size,processing_time,datetime_processed,error\n")


if __name__ == "__main__":
    initialize_dzsave_dir()
    
    dzsave_wsi_name('H22-9925;S15;MSK8 - 2023-06-12 18.11.56.ndpi', tile_size=256)