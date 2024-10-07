import io
import os
import ray
import time
import zarr
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
from LLRunner.slide_processing.dzsave_h5 import initialize_dzsave_dir
import zarr
from numcodecs import VLenBytes  # Import the codec for variable-length bytes

def image_to_jpeg_string(image):
    buffer = io.BytesIO()
    try:
        image.save(buffer, format="JPEG")
        jpeg_string = buffer.getvalue()
    finally:
        buffer.close()
    return jpeg_string


def jpeg_string_to_image(jpeg_string):
    buffer = io.BytesIO(jpeg_string)
    image = Image.open(buffer)
    image.load()
    return image


def encode_image_to_base64(jpeg_string):
    return base64.b64encode(jpeg_string)


def decode_image_from_base64(encoded_string):
    return base64.b64decode(encoded_string)


def create_list_of_batches_from_list(list, batch_size):
    list_of_batches = [
        list[i : i + batch_size] for i in range(0, len(list), batch_size)
    ]
    return list_of_batches



def initialize_final_zarr_file(
    zarr_path, image_width, image_height, num_levels=18, patch_size=256
):
    """
    Initialize a Zarr file for storing image tiles.
    """
    if os.path.exists(zarr_path):
        # Remove existing Zarr file
        os.remove(zarr_path)

    # Initialize the Zarr group
    root = zarr.open(zarr_path, mode='w')

    # Define the codec for storing variable-length bytes
    object_codec = VLenBytes()

    for level in range(num_levels + 1):
        level_image_height = image_height // (2 ** (num_levels - level))
        level_image_width = image_width // (2 ** (num_levels - level))

        # Create datasets for each level with the specified codec
        root.create_dataset(
            str(level),
            shape=(
                max(level_image_width // patch_size + 1, 1),
                max(level_image_height // patch_size + 1, 1),
            ),
            dtype=object,  # Use object dtype for variable-length data
            chunks=(1, 1),  # Adjust chunk size as needed
            object_codec=object_codec  # Specify the codec for variable-length bytes
        )

    # Store metadata such as image dimensions, patch size, etc.
    root.create_dataset("level_0_width", shape=(1,), dtype="int")[0] = image_width
    root.create_dataset("level_0_height", shape=(1,), dtype="int")[0] = image_height
    root.create_dataset("patch_size", shape=(1,), dtype="int")[0] = patch_size
    root.create_dataset("num_levels", shape=(1,), dtype="int")[0] = num_levels
    root.create_dataset("overlap", shape=(1,), dtype="int")[0] = 0


@ray.remote
class WSICropManager:
    def __init__(self, wsi_path) -> None:
        self.wsi_path = wsi_path
        self.wsi = None

    def open_slide(self):
        self.wsi = openslide.OpenSlide(self.wsi_path)

    def close_slide(self):
        self.wsi.close()
        self.wsi = None

    def get_level_N_dimensions(self, wsi_level):
        if self.wsi is None:
            self.open_slide()
        return self.wsi.level_dimensions[wsi_level]

    def get_tile_coordinate_level_pairs(self, tile_size=256, wsi_level=0):
        if self.wsi is None:
            self.open_slide()
        width, height = self.get_level_N_dimensions(wsi_level)
        coordinates = [
            (
                (x, y, min(x + tile_size, width), min(y + tile_size, height)),
                wsi_level,
            )
            for y in range(0, height, tile_size)
            for x in range(0, width, tile_size)
        ]
        return coordinates

    def crop(self, coords, wsi_level=0):
        if self.wsi is None:
            self.open_slide()
        coords_level_0 = (
            coords[0] * (2**wsi_level),
            coords[1] * (2**wsi_level),
            coords[2] * (2**wsi_level),
            coords[3] * (2**wsi_level),
        )
        image = self.wsi.read_region(
            (coords_level_0[0], coords_level_0[1]),
            wsi_level,
            (coords[2] - coords[0], coords[3] - coords[1]),
        )
        image = image.convert("RGB")
        return image

    def async_get_bma_focus_region_level_pair_batch(
        self, focus_region_coords_level_pairs, crop_size=256
    ):
        indices_to_jpeg = []
        for focus_region_coord_level_pair in focus_region_coords_level_pairs:
            focus_region_coord, wsi_level = focus_region_coord_level_pair
            image = self.crop(focus_region_coord, wsi_level=wsi_level)
            jpeg_string = image_to_jpeg_string(image)
            jpeg_string = encode_image_to_base64(jpeg_string)
            indices_level_jpeg = (
                focus_region_coord[0] // crop_size,
                focus_region_coord[1] // crop_size,
                wsi_level,
                jpeg_string,
            )
            indices_to_jpeg.append(indices_level_jpeg)
        return indices_to_jpeg


def crop_wsi_images_all_levels(
    wsi_path,
    zarr_path,
    region_cropping_batch_size,
    crop_size=256,
    verbose=True,
    num_cpus=32,
):
    num_croppers = num_cpus

    if verbose:
        print("Initializing WSICropManager")

    manager = WSICropManager.remote(wsi_path)

    focus_regions_coordinates = []

    for level in range(0, 8):
        focus_regions_coordinates.extend(
            ray.get(
                manager.get_tile_coordinate_level_pairs.remote(
                    tile_size=crop_size, wsi_level=level
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
                    for indices_jpeg in batch:
                        x, y, wsi_level, jpeg_string = indices_jpeg
                        level = int(18 - wsi_level)
                        with zarr.open(zarr_path, mode="a") as root:
                            root[str(level)][x, y] = jpeg_string

                    pbar.update(len(batch))

                except ray.exceptions.RayTaskError as e:
                    print(f"Task for batch {tasks[done_id]} failed with error: {e}")

                del tasks[done_id]


def retrieve_tile_zarr(zarr_path, level, row, col):
    with zarr.open(zarr_path, mode="r") as root:
        try:
            jpeg_string = root[str(level)][row, col]
            jpeg_string = decode_image_from_base64(jpeg_string)
            image = jpeg_string_to_image(jpeg_string)
        except Exception as e:
            print(
                f"Error: {e} occurred while retrieving tile at level: {level}, row: {row}, col: {col} from {zarr_path}"
            )
            raise e
        return image


def get_depth_from_0_to_11(wsi_path, zarr_path, tile_size=256):
    """
    Generate depth images from level 7 to level 0, each downsampled by a factor of 2,
    and store them as tiles in a Zarr file.
    """
    wsi = openslide.OpenSlide(wsi_path)
    level_7_dimensions = wsi.level_dimensions[7]
    image = wsi.read_region((0, 0), 7, level_7_dimensions)
    image = image.convert("RGB")

    current_image = image
    for depth in range(10, -1, -1):
        # Downsample the image by a factor of 2 for each depth level
        current_image = image.resize(
            (
                max(image.width // (2 ** (11 - depth)), 1),
                max(image.height // (2 ** (11 - depth)), 1),
            )
        )

        # Crop 256x256 patches from the downsampled image (no overlap)
        for y in range(0, current_image.height, tile_size):
            for x in range(0, current_image.width, tile_size):
                # Calculate the right and bottom coordinates ensuring they are within image boundaries
                right = min(x + tile_size, current_image.width)
                bottom = min(y + tile_size, current_image.height)

                # Crop the patch from the image starting at (x, y) to (right, bottom)
                patch = current_image.crop((x, y, right, bottom))
                patch = patch.convert("RGB")

                # Store the patch in Zarr
                level = str(depth)
                with zarr.open(zarr_path, mode="a") as root:
                    jpeg_string = image_to_jpeg_string(patch)
                    jpeg_string = encode_image_to_base64(jpeg_string)
                    try:
                        root[level][
                            int(x // tile_size), int(y // tile_size)
                        ] = jpeg_string
                    except Exception as e:
                        print(
                            f"Error: {e} occurred while saving patch at level: {level}, x: {x}, y: {y} to {zarr_path}"
                        )


def dzsave_zarr(
    wsi_path,
    zarr_path,
    tile_size=256,
    num_cpus=32,
    region_cropping_batch_size=256,
):
    """
    Create a DeepZoom image pyramid from a WSI using Zarr.
    """
    wsi = openslide.OpenSlide(wsi_path)
    width, height = wsi.dimensions

    print(f"Width: {width}, Height: {height}")

    starttime = time.time()

    print("Cropping from NDPI")
    crop_wsi_images_all_levels(
        wsi_path,
        zarr_path,
        region_cropping_batch_size=region_cropping_batch_size,
        crop_size=tile_size,
        num_cpus=num_cpus,
    )
    print("Cropping Lower Resolution Levels")
    get_depth_from_0_to_11(wsi_path, zarr_path, tile_size=tile_size)
    time_taken = time.time() - starttime

    return time_taken


def dzsave_wsi_name_zarr(
    wsi_name, tile_size=256, num_cpus=32, region_cropping_batch_size=256
):
    """
    Create a Zarr pyramid from a WSI and store it.
    """
    wsi_name_path = Path(wsi_name)
    wsi_path = os.path.join(tmp_slide_dir, wsi_name)

    assert os.path.exists(wsi_path), f"Error: {wsi_path} does not exist."
    wsi_name_no_ext = wsi_name_path.stem

    zarr_path = os.path.join(dzsave_dir, wsi_name_no_ext) + ".zarr"

    wsi = openslide.OpenSlide(wsi_path)
    image_width, image_height = wsi.dimensions

    initialize_final_zarr_file(
        zarr_path,
        image_width=image_width,
        image_height=image_height,
        patch_size=tile_size,
    )

    starttime = time.time()

    try:
        dzsave_zarr(
            wsi_path=wsi_path,
            zarr_path=zarr_path,
            tile_size=tile_size,
            num_cpus=num_cpus,
            region_cropping_batch_size=region_cropping_batch_size,
        )
        error = None

    except Exception as e:
        error = str(e)
        print(
            f"UserWarning: Error: {error} occurred while processing {wsi_name}. While continue on error is on, the error should not be ignored if it happens to too many slides."
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
    if not os.path.exists(dzsave_metadata_path):
        with open(dzsave_metadata_path, "w") as f:
            f.write("wsi_name,tile_size,processing_time,datetime_processed,error\n")


def retrieve_tile_zarr(zarr_path, level, row, col):
    with zarr.open(zarr_path, mode="r") as root:
        try:
            jpeg_string = root[str(level)][row, col]
            jpeg_string = decode_image_from_base64(jpeg_string)
            image = jpeg_string_to_image(jpeg_string)
        except Exception as e:
            print(
                f"Error: {e} occurred while retrieving tile at level: {level}, row: {row}, col: {col} from {zarr_path}"
            )
            raise e
        return image


if __name__ == "__main__":
    slide_name = "H19-5749;S10;MSKI - 2023-05-24 21.38.53.ndpi"
    initialize_dzsave_dir()
    dzsave_wsi_name_zarr(
        slide_name,
        tile_size=256,
        num_cpus=128,
        region_cropping_batch_size=2048,
    )

    zarr_path = (
        "/media/hdd3/neo/dzsave_dir/H19-5749;S10;MSKI - 2023-05-24 21.38.53.zarr"
    )

    with zarr.open(zarr_path, "r") as root:
        print(root.keys())
        print(root["18"].shape)
        image = retrieve_tile_zarr(zarr_path, level=18, row=10, col=10)
        image.save("/media/hdd3/neo/my_test.jpeg")
