import os
import ray
import h5py
import base64
import openslide
import numpy as np
from tqdm import tqdm
from PIL import Image
import io


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


def initialize_h5py_file(h5_path, batch_width, level, patch_size=256):
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
        # Create dataset with shape (num_tile_rows, num_tile_columns)

        dt = h5py.special_dtype(vlen=bytes)

        f.create_dataset(
            str(level),
            shape=(1, batch_width),
            dtype=dt,
        )


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
        for level in range(num_levels):
            level_image_height = image_height // (2 ** (num_levels - level))
            level_image_width = image_width // (2 ** (num_levels - level))

            dt = h5py.special_dtype(vlen=bytes)

            f.create_dataset(
                str(level),
                shape=(
                    level_image_height // patch_size,
                    level_image_width // patch_size,
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


def combine_tmp_h5_files(tmp_h5_dir, h5_save_path):
    # first get a list of all the h5 files in the directory
    h5_files = [
        os.path.join(tmp_h5_dir, f) for f in os.listdir(tmp_h5_dir) if f.endswith(".h5")
    ]

    # open the h5_save_path
    with h5py.File(h5_save_path, "r") as f:
        level_0_width = f["level_0_width"][0]
        level_0_height = f["level_0_height"][0]
        patch_size = f["patch_size"][0]
        num_levels = f["num_levels"][0]

    # open the h5_save_path in write mode
    with h5py.File(h5_save_path, "a") as f:
        for h5_file in tqdm(h5_files, desc="Combining temporary h5 files..."):

            print(f"Adding {h5_file} to {h5_save_path}...")

            # the h5_file filename is of the form level_row.h5, parse it to get the level and row
            level = int(h5_file.split("/")[-1].split("_")[0])
            row = int(h5_file.split("/")[-1].split("_")[1].split(".")[0])

            print(f"Adding level {level} row {row}...")
            with h5py.File(h5_file, "r") as tmp_f:
                level_image_height = level_0_height // (2 ** (num_levels - level))
                level_image_width = level_0_width // (2 ** (num_levels - level))

                for row in range(level_image_height // patch_size):
                    for column in range(level_image_width // patch_size):
                        f[f"{level}"][row, column] = tmp_f[f"{level}"][
                            0, column
                        ]  # TODO check the integrity of the images before and after added to the final h5 file
                        # TODO also check the statistical distribution of pixels in the final h5 file at each level


def add_patch_to_h5py(h5_path, level, patch, row, column):
    """
    Add a patch to an HDF5 file at the specified row and column at the dataste named str(level).
    Patch is a PIL image.
    """

    # assert that the patch is a PIL image
    assert isinstance(patch, Image.Image), "Error: patch is not a PIL image."
    # apply jpeg compression to the image
    patch_string = image_to_jpeg_string(patch)

    patch_raw_bytes = encode_image_to_base64(patch_string)

    with h5py.File(h5_path, "a") as f:
        f[f"{level}"][row, column] = patch_raw_bytes


@ray.remote
class WSIH5CropManager:
    """
    A class representing a manager that crops WSIs.
    Each Manager object is assigned with a single CPU core and is responsible for cropping a subset of the coordinates from a given WSI.

    Attributes:
    wsi_path: str: The path to the WSI.
    wsi: openslide.OpenSlide: The WSI object.
    manager_id: int: The ID of the manager.
    root_tmp_dir: str: The root temporary directory where the cropped patches will be saved.
    image_width: int: The width of the WSI at level 0.
    image_height: int: The height of the WSI at level 0.
    """

    def __init__(self, wsi_path, manager_id, root_tmp_dir) -> None:
        self.wsi_path = wsi_path
        self.wsi = None

        image_width, image_height = self.get_level_0_dimensions()

        self.image_width = image_width
        self.image_height = image_height

        self.manager_id = manager_id
        self.root_tmp_dir = root_tmp_dir

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

    def batch_all_levels(self, patch_size, num_levels=18):
        """Return a dictionary that maps (level, row_id) to a list of focus region coordinates level pairs."""

        focus_region_coords_level_pairs = {}

        with openslide.OpenSlide(self.wsi_path) as slide:
            image_width, image_height = slide.dimensions

        for level in range(num_levels + 1):
            level_image_height = image_height // (2 ** (num_levels - level))
            level_image_width = image_width // (2 ** (num_levels - level))

            for row_id in range(level_image_height // patch_size):
                focus_region_coords_level_pairs[(level, row_id)] = [
                    (
                        (
                            column_id * patch_size,
                            row_id * patch_size,
                            min((column_id + 1) * patch_size, level_image_width),
                            min((row_id + 1) * patch_size, level_image_height),
                        ),
                        level,
                    )
                    for column_id in range(level_image_width // patch_size)
                ]

        return focus_region_coords_level_pairs

    def save_h5_row(self, batch, level, row_id):
        """
        batch[i] = (focus_region_coord, level)
        """

        tmp_h5_path = os.path.join(self.root_tmp_dir, f"{str(level)}_{str(row_id)}.h5")

        # print(f"Initalizing h5 file at {tmp_h5_path}...")
        initialize_h5py_file(tmp_h5_path, batch_width=len(batch), level=level)

        for i, focus_region_coord_level_pair in enumerate(batch):
            focus_region_coord, level = focus_region_coord_level_pair
            image = self.crop(focus_region_coord, level=level)
            add_patch_to_h5py(
                h5_path=tmp_h5_path,
                level=level,
                patch=image,
                row=0,
                column=i,
            )

    def crop(self, coords, level=0, max_level=18):
        """Crop the WSI at the specified level of magnification.
        The coords here are assumed to be at the level we are cropping from."""
        if self.wsi is None:
            self.open_slide()

        wsi_level = max_level - level

        assert (
            wsi_level >= 0
        ), f"Error: Level {level} is greater than max_level {max_level}."
        if wsi_level <= self.wsi.level_count:
            coords_level_0 = (
                coords[0] * (2**wsi_level),
                coords[1] * (2**wsi_level),
                coords[2] * (2**wsi_level),
                coords[3] * (2**wsi_level),
            )

            crop_width = coords[2] - coords[0]
            crop_height = coords[3] - coords[1]

            # assert taht the crop is not out of bounds
            assert (
                coords_level_0[0] >= 0
            ), f"Error: coords_level_0[0] is less than 0: {coords_level_0[0]}."

            assert (
                coords_level_0[1] >= 0
            ), f"Error: coords_level_0[1] is less than 0: {coords_level_0[1]}."

            assert (
                coords_level_0[2] <= self.image_width
            ), f"Error: coords_level_0[2] is greater than image_width: {coords_level_0[2]}."

            assert (
                coords_level_0[3] <= self.image_height
            ), f"Error: coords_level_0[3] is greater than image_height: {coords_level_0[3]}."

            image = self.wsi.read_region(
                coords_level_0[:2],
                wsi_level,
                (crop_width, crop_height),
            )

        else:
            wsi_level_difference = wsi_level - self.wsi.level_count
            downsample_factor = 2**wsi_level_difference

            # take the whole image at the highest level using read region
            topview = self.wsi.read_region(
                (0, 0),
                self.wsi.level_count - 1,
                self.wsi.level_dimensions[self.wsi.level_count - 1],
            )

            # downsample the image to the desired level
            image = topview.resize(
                (
                    topview.width // downsample_factor,
                    topview.height // downsample_factor,
                )
            )

            # now crop the image from the downsampled image
            image = image.crop((coords[0], coords[1], coords[2], coords[3]))

        image = image.convert("RGB")
        return image


def dzsave_h5(
    wsi_path,
    save_dir,
    h5_name="test",
    tile_size=256,
    num_cpus=96,
):

    ray.shutdown()
    ray.init(num_cpus=num_cpus)

    root_tmp_dir = os.path.join(save_dir, h5_name)

    os.makedirs(root_tmp_dir, exist_ok=True)

    managers = [
        WSIH5CropManager.remote(wsi_path, i, root_tmp_dir) for i in range(num_cpus)
    ]

    print("Batching all levels...")
    focus_region_coords_level_pairs = managers[0].batch_all_levels.remote(tile_size)
    focus_region_coords_level_pairs = ray.get(focus_region_coords_level_pairs)

    # number of patches
    total = 0
    for key in focus_region_coords_level_pairs.keys():
        total += len(focus_region_coords_level_pairs[key])

    print(f"Total number of patches: {total}")

    keys = list(focus_region_coords_level_pairs.keys())

    tasks = {}

    print("Assigning Ray Tasks to Workers...")

    for i, key in enumerate(keys):
        manager = managers[i % num_cpus]
        task = manager.save_h5_row.remote(
            focus_region_coords_level_pairs[key], key[0], key[1]
        )

        tasks[task] = key

    print("Cropping regions and saving to HDF5...")

    with tqdm(
        total=len(focus_region_coords_level_pairs),
        desc="Cropping regions and saving to HDF5",
    ) as pbar:
        while tasks:
            done_ids, _ = ray.wait(list(tasks.keys()))

            for done_id in done_ids:
                try:
                    key = tasks[done_id]
                    output = ray.get(done_id)
                    pbar.update()
                except ray.exceptions.RayTaskError as e:
                    print(f"Task for batch {key} failed with error: {e}")

                del tasks[done_id]

    ray.shutdown()

    # now combine all the temporary h5 files into a single h5 file
    h5_save_path = os.path.join(save_dir, f"{h5_name}.h5")

    # get the image_width and image_height from wsi_path
    with openslide.OpenSlide(wsi_path) as slide:
        image_width, image_height = slide.dimensions

    # print("Combining temporary h5 files... initializing final h5 file...")
    # initialize_final_h5py_file(
    #     h5_save_path,
    #     image_width=image_width,
    #     image_height=image_height,
    #     num_levels=18,
    #     patch_size=256,
    # )

    # print("Combining temporary h5 files... adding patches to final h5 file...")
    # combine_tmp_h5_files(root_tmp_dir, h5_save_path)

    # # print("Combining temporary h5 files... deleting temporary h5 files...")
    # # os.system(f"rm -r {root_tmp_dir}")

    # print("Done!")


if __name__ == "__main__":
    slide_path = "/media/hdd3/neo/BMA_AML/H19-3767;S10;MSKG - 2023-09-05 16.27.28.ndpi"
    save_dir = "/media/hdd3/neo/test_dzsave_h5"
    network_location = "/dmpisilon_tools/neo/dzsave_h5_rows"

    import time
    import random

    os.makedirs(save_dir, exist_ok=True)

    start_time = time.time()
    dzsave_h5(slide_path, save_dir)
    dzsave_h5_tmp_dir = os.path.join(save_dir, "test")
    dzsave_time = time.time() - start_time

    # start_time = time.time()
    # # rysnc the tmp h5 files to the save_dir
    # os.system(f'sudo rsync -av "{dzsave_h5_tmp_dir}" "{network_location}"')
    # rsync_time = time.time() - start_time

    # all_tmp_files = os.listdir(dzsave_h5_tmp_dir)
    # start_time = time.time()
    # num_to_load = 1000
    # for i in tqdm(range(num_to_load), desc="Loading random patches..."):
    #     random_file = random.choice(all_tmp_files)

    #     # open the h5 file on the network location
    #     with h5py.File(os.path.join(network_location, "test", random_file), "r") as f:
    #         level = random_file.split("_")[0]
    #         # load a random column from the h5 file
    #         column = random.randint(0, f[level].shape[1] - 1)

    #         # load the image
    #         image = f[level][0, column]

    #         # convert the image to a PIL image
    #         pil_image = Image.fromarray(image)

    # loading_time = time.time() - start_time

    # print(f"dzsave_h5 time: {dzsave_time}")
    # print(f"rsync time: {rsync_time}")
    # print(f"loading time for {num_to_load} patches: {loading_time}")
    # # dzsave_h5_path = "/media/hdd3/neo/test_dzsave_h5/test.h5"

    # # # print all the keys in the h5 file
    # # print("Keys in the h5 file:")
    # # with h5py.File(dzsave_h5_path, "r") as f:
    # #     for key in f.keys():
    # #         print(key)

    # # # print the shape of each level 0 ... 18
    # # for i in range(18):
    # #     with h5py.File(dzsave_h5_path, "r") as f:
    # #         print(f"Shape of level {i}: {f[str(i)].shape}")

    # # # get the image at (101,206) at level 18
    # # with h5py.File(dzsave_h5_path, "r") as f:
    # #     image = f["17"][101, 206]
    # #     # save the image to "/media/hdd3/neo/test_dzsave_h5/test.jpg"
    # #     Image.fromarray(image).save("/media/hdd3/neo/test_dzsave_h5/test.jpg")
    # #     print("Image saved to /media/hdd3/neo/test_dzsave_h5/test.jpg")

    # # # retrieve the same image from the tmp h5 file
    # # with h5py.File("/media/hdd3/neo/test_dzsave_h5/test/17_101.h5", "r") as f:
    # #     image = f["17"][0, 206]
    # #     # save the image to "/media/hdd3/neo/test_dzsave_h5/test_tmp.jpg"
    # #     Image.fromarray(image).save("/media/hdd3/neo/test_dzsave_h5/test_tmp.jpg")
    # #     print("Image saved to /media/hdd3/neo/test_dzsave_h5/test_tmp.jpg")

    # time how long it takes to sudo rsync the slide to the network location
    # import time

    # start_time = time.time()
    # os.system(f'sudo rsync -av "{slide_path}" "{network_location}"')
    # print("Time to rsync slide to network location:", time.time() - start_time)

    # get the total storage consumption of /media/hdd3/neo/test_dzsave_h5
    import os
    import subprocess

    # get the total storage consumption of /media/hdd3/neo/test_dzsave_h5
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(save_dir):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)

    print(f"Total size of {save_dir}: {total_size / 1e9} GB")

    # get the total storage consumption of the slide_path itself
    total_size = os.path.getsize(slide_path)

    print(f"Total size of {slide_path}: {total_size / 1e9} GB")
