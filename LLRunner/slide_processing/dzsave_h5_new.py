import os
import ray
import h5py
import openslide
import numpy as np
from tqdm import tqdm
from PIL import Image


def padding_image(image, patch_size=256):
    """First check that both dim of the image is <= patch_size, if not raise an error.
    Then pad the image to make it patch_size x patch_size by adding black pixels to the right and bottom.
    """

    image_width = image.width
    image_height = image.height
    assert (
        image_width <= patch_size
    ), f"Error: Image width {image.width} is greater than patch_size {patch_size}."
    assert (
        image_height <= patch_size
    ), f"Error: Image height {image.height} is greater than patch_size {patch_size}."

    if image_height == patch_size and image_width == patch_size:
        return image
    else:
        new_image = Image.new("RGB", (patch_size, patch_size), (0, 0, 0))
        new_image.paste(image, (0, 0))
    return new_image


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
        # Create dataset with shape (num_tile_rows, num_tile_columns, patch_size, patch_size, 3)
        f.create_dataset(
            str(level),
            shape=(1, batch_width, patch_size, patch_size, 3),
            dtype="uint8",
        )


def add_patch_to_h5py(h5_path, level, patch, row, column):
    """
    Add a patch to an HDF5 file at the specified row and column at the dataste named str(level).
    """
    with h5py.File(h5_path, "a") as f:
        f[f"{level}"][row, column] = patch


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

        for level in range(num_levels):
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

        print(f"Initalizing h5 file at {tmp_h5_path}...")
        initialize_h5py_file(tmp_h5_path, batch_width=len(batch), level=level)

        for i, focus_region_coord_level_pair in enumerate(batch):
            focus_region_coord, level = focus_region_coord_level_pair
            image = self.crop(focus_region_coord, level=level)
            padded_image = padding_image(image, patch_size=256)
            add_patch_to_h5py(
                h5_path=tmp_h5_path,
                level=level,
                patch=np.array(padded_image),
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
                coords[0] * (2**level),
                coords[1] * (2**level),
                coords[2] * (2**level),
                coords[3] * (2**level),
            )

            crop_width = coords[2] - coords[0]
            crop_height = coords[3] - coords[1]

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


if __name__ == "__main__":
    slide_path = "/media/hdd3/neo/BMA_AML/H19-3767;S10;MSKG - 2023-09-05 16.27.28.ndpi"
    save_dir = "/media/hdd3/neo/test_dzsave_h5"

    # dzsave_h5(slide_path, save_dir)
    # initialize_h5py_file("/media/hdd3/neo/test_dzsave_h5/test/0_0.h5", 1, 0)

    example_h5_tmp = "/media/hdd3/neo/test_dzsave_h5/test/17_132.h5"

    # open the h5 file and print all the keys
    with h5py.File(example_h5_tmp, "r") as f:
        print(f.keys())
        print(f["17"].shape)
        print(f["17"][0, 0].shape)

        print(f["17"][0, 0])    

        # turn it into a PIL image
        img = Image.fromarray(f["17"][0, 0])

        # save the image in the same directory
        img.save("/media/hdd3/neo/test_dzsave_h5/test/17_132_example.jpg")
