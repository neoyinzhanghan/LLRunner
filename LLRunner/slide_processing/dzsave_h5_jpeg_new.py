import io
import os
import ray
import h5py
import base64
import openslide
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from PIL import Image
from LLRunner.config import (
    dzsave_dir,
    dzsave_metadata_path,
    tmp_slide_dir,
)


def image_to_jpeg_string(image):
    # Create an in-memory bytes buffer
    buffer = io.BytesIO()
    try:
        # Save the image in JPEG format to the buffer
        image.save(buffer, format="JPEG")
        jpeg_string = buffer.getvalue()  # Get the byte data
    finally:
        buffer.close()  # Explicitly close the buffer to free memory

    return jpeg_string


def jpeg_string_to_image(jpeg_string):
    # Create a BytesIO object from the JPEG string (byte data)
    jpeg_string = bytes(jpeg_string)
    buffer = io.BytesIO(jpeg_string)

    # Open the image from the buffer
    image = Image.open(buffer)

    return image


def encode_image_to_base64(jpeg_string):
    return base64.b64encode(jpeg_string)


def decode_image_from_base64(encoded_string):
    return base64.b64decode(encoded_string)


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


def initialize_final_h5py_file(
    h5_path, image_width, image_height, num_levels=18, patch_size=256
):
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

    # Create the HDF5 file and dataset
    with h5py.File(h5_path, "w") as f:
        # Create dataset with shape (num_tile_rows, num_tile_columns, patch_size, patch_size, 3)
        for level in range(num_levels + 1):
            level_image_height = image_height // (2 ** (num_levels - level))
            level_image_width = image_width // (2 ** (num_levels - level))

            dt = h5py.special_dtype(vlen=bytes)

            f.create_dataset(
                str(level),
                shape=(
                    level_image_width // patch_size,
                    level_image_height // patch_size,
                ),
                dtype=dt,
            )

        # also track the image width and height
        f.create_dataset(
            "level_0_width",
            shape=(1,),
            dtype="int",
        )

        f.create_dataset(
            "level_0_height",
            shape=(1,),
            dtype="int",
        )

        # also track the patch size
        f.create_dataset(
            "patch_size",
            shape=(1,),
            dtype="int",
        )

        # also track the number of levels
        f.create_dataset(
            "num_levels",
            shape=(1,),
            dtype="int",
        )

        f["level_0_width"][0] = image_width
        f["level_0_height"][0] = image_height
        f["patch_size"][0] = patch_size
        f["num_levels"][0] = num_levels


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

        for y in range(height // tile_size):
            for x in range(width // tile_size):
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
        self, focus_region_coords_level_pairs, crop_size=256
    ):
        """Save a list of focus regions."""

        indices_to_jpeg_dict = {}
        for focus_region_coord_level_pair in focus_region_coords_level_pairs:
            focus_region_coord, level = focus_region_coord_level_pair

            image = self.crop(focus_region_coord, level=level)

            indices_level = (
                focus_region_coord[0] // crop_size,
                focus_region_coord[1] // crop_size,
                level,
            )

            jpeg_string = image_to_jpeg_string(image)
            jpeg_string = encode_image_to_base64(jpeg_string)

            indices_to_jpeg_dict[indices_level] = jpeg_string

        return indices_to_jpeg_dict


def crop_wsi_images_all_levels(
    wsi_path,
    h5_path,
    region_cropping_batch_size,
    crop_size=256,
    verbose=True,
    num_cpus=32,
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
            batch, crop_size=crop_size
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

                    keys = list(batch.keys())
                    for indices in keys:
                        # Save the jpeg_string to the h5 file
                        jpeg_string = batch[indices]
                        level = 18 - indices[2]
                        with h5py.File(h5_path, "a") as f:
                            f[str(level)][indices[0], indices[1]] = jpeg_string

                        pbar.update()

                except ray.exceptions.RayTaskError as e:
                    print(f"Task for batch {tasks[done_id]} failed with error: {e}")

                del tasks[done_id]


def get_depth_from_0_to_11(wsi_path, h5_path, tile_size=256):
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

                # make sure patch is in RGB mode and a PIL image
                patch = patch.convert("RGB")

                indices = (x // tile_size, y // tile_size)
                level = str(depth)

                # Save the patch to the h5 file
                with h5py.File(h5_path, "a") as f:
                    jpeg_string = image_to_jpeg_string(patch)
                    jpeg_string = encode_image_to_base64(jpeg_string)
                    f[level][indices[0], indices[1]] = jpeg_string


def dzsave(
    wsi_path,
    h5_path,
    tile_size=256,
    num_cpus=32,
    region_cropping_batch_size=256,
):
    """
    Create a DeepZoom image pyramid from a WSI.
    """

    wsi = openslide.OpenSlide(wsi_path)
    height, width = wsi.dimensions

    print(f"Width: {width}, Height: {height}")

    starttime = time.time()

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

    h5_path = os.path.join(dzsave_dir, wsi_name_no_ext) + ".h5"

    wsi = openslide.OpenSlide(wsi_path)
    image_width, image_height = wsi.dimensions

    initialize_final_h5py_file(
        h5_path,
        image_width=image_width,
        image_height=image_height,
        patch_size=tile_size,
    )

    starttime = time.time()

    try:
        dzsave(
            wsi_path=wsi_path,
            h5_path=h5_path,
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

        raise e

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
    dzsave_metadata_df = pd.concat([dzsave_metadata_df, new_row_df], ignore_index=True)

    # Save the dataframe back to the dzsave_metadata_path
    dzsave_metadata_df.to_csv(dzsave_metadata_path, index=False)


def initialize_dzsave_dir():
    os.makedirs(dzsave_dir, exist_ok=True)
    # if the dzsave_metadata_path does not exist, then create it
    if not os.path.exists(dzsave_metadata_path):
        with open(dzsave_metadata_path, "w") as f:
            f.write("wsi_name,tile_size,processing_time,datetime_processed,error\n")


if __name__ == "__main__":
    import time

    # start_time = time.time()
    # print("Rsyncing slide")
    # original_slide_path = (
    #     "/pesgisipth/NDPI/H19-5749;S10;MSKI - 2023-05-24 21.38.53.ndpi"
    # )

    # # run sudo rsync -av the slide from original_slide_path to slide_path
    # save_path = "/media/hdd3/neo/"

    slide_name = "H19-5749;S10;MSKI - 2023-05-24 21.38.53.ndpi"
    # slide_path = os.path.join(save_path, slide_name)

    # # copy the slide from original_slide_path to slide_path
    # os.system(f"sudo rsync -av {original_slide_path} {slide_path}")
    # rsync_slide_time = time.time() - start_time

    start_time = time.time()
    print("DZSaving slide")
    initialize_dzsave_dir()
    dzsave_wsi_name(
        slide_name,
        tile_size=256,
    )

    dzsave_time = time.time() - start_time

    print(f"DZSave time: {dzsave_time}")
