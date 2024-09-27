import io
import os
import h5py
import time
import openslide
import numpy as np
from tqdm import tqdm


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


def initialize_final_h5py_file(h5_path, image_width, image_height, patch_size=256):
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
        level = 0
        level_image_width = image_width
        level_image_height = image_height

        dt = h5py.special_dtype(vlen=np.dtype("uint8"))  # Variable-length binary data

        f.create_dataset(
            str(level),
            shape=(
                level_image_height // patch_size,
                level_image_width // patch_size,
            ),
            dtype=dt,
        )


def tile_wsi(wsi_path, h5_path, tile_size=256):
    """Tile the WSI and save the tiles to an HDF5 file."""

    cropping_time = 0
    compression_time = 0
    writing_time = 0

    wsi = openslide.OpenSlide(wsi_path)

    image_width, image_height = wsi.dimensions

    for i in tqdm(range(0, image_height, tile_size), desc="Tiling WSI X Range"):
        for j in tqdm(range(0, image_width, tile_size), desc="Tiling WSI Y Range"):

            start_time = time.time()
            tile = wsi.read_region((j, i), 0, (tile_size, tile_size))
            cropping_time += time.time() - start_time

            start_time = time.time()
            tile = tile.convert("RGB")
            jpeg_string = image_to_jpeg_string(tile)
            compression_time += time.time() - start_time

            start_time = time.time()
            with h5py.File(h5_path, "a") as f:
                jpeg_string = np.void(jpeg_string)
                f["0"][i // tile_size, j // tile_size] = jpeg_string
            writing_time += time.time() - start_time

    print(f"Cropping time: {cropping_time:.2f} seconds")
    print(f"Compression time: {compression_time:.2f} seconds")
    print(f"Writing time: {writing_time:.2f} seconds")


if __name__ == "__main__":
    wsi_path = "/media/hdd3/neo/BMA_AML/H19-3767;S10;MSKG - 2023-09-05 16.27.28.ndpi"
    h5_path = "/media/hdd3/neo/test.h5"
    wsi = openslide.OpenSlide(wsi_path)
    image_width, image_height = wsi.dimensions
    initialize_final_h5py_file(h5_path, image_width, image_height)
    tile_wsi(wsi_path, h5_path)
