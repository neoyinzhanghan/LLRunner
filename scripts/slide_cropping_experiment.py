import openslide
import ray
from tqdm import tqdm


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


if __name__ == "__main__":
    import doctest

    doctest.testmod()


@ray.remote
class WSICropManager:
    """A class representing a manager that crops WSIs."""

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

    def get_tile_coordinates(self, tile_size=256, level=0):
        """Generate a list of coordinates for 256x256 disjoint patches."""
        if self.wsi is None:
            self.open_slide()

        width, height = self.get_level_N_dimensions(level)
        coordinates = []

        for y in range(0, height, tile_size):
            for x in range(0, width, tile_size):
                # Ensure that the patch is within the image boundaries
                if x + tile_size <= width and y + tile_size <= height:
                    coordinates.append((x, y, x + tile_size, y + tile_size))

        return coordinates

    def get_tile_coordinates_level_pair(self, tile_size=256, level=0):
        """Generate a list of coordinates for 256x256 disjoint patches."""
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

        image = self.wsi.read_region(
            (coords[0], coords[1]),
            level,
            (coords[2] - coords[0], coords[3] - coords[1]),
        )

        image = image.convert("RGB")
        return image

    # def async_get_bma_focus_region_batch(self, focus_region_coords, save_dir):
    #     """Save a list of focus regions."""
    #     for focus_region_coord in focus_region_coords:
    #         image = self.crop(focus_region_coord, level=0)
    #         # Save the image to a .jpeg file in save_dir
    #         crop_size = focus_region_coord[2] - focus_region_coord[0]
    #         path = os.path.join(
    #             save_dir,
    #             f"{int(focus_region_coord[0]//crop_size)}_{int(focus_region_coord[1]//crop_size)}.jpeg",
    #         )
    #         image.save(path)
    #     return len(focus_region_coords)

    def async_get_bma_focus_region_level_pair_batch(
        self, focus_region_coords_level_pairs, save_dir, crop_size=256
    ):
        """Save a list of focus regions."""
        for focus_region_coord_level_pair in focus_region_coords_level_pairs:
            focus_region_coord, level = focus_region_coord_level_pair

            image = self.crop(focus_region_coord, level=level)
            # Save the image to a .jpeg file in save_dir

            path = os.path.join(
                save_dir,
                str(18 - level),
                f"{int(focus_region_coord[0]//crop_size)}_{int(focus_region_coord[1]//crop_size)}.jpeg",
            )
            image.save(path)

        return len(focus_region_coords_level_pairs)


# Main processing function
def crop_wsi_images(
    wsi_path,
    save_dir,
    region_cropping_batch_size,
    crop_size=256,
    level=0,
    verbose=True,
    num_cpus=128,
):
    num_croppers = num_cpus  # Number of croppers is the same as num_cpus

    if verbose:
        print("Initializing WSICropManager")

    manager = WSICropManager.remote(wsi_path)

    # Get all the coordinates for 256x256 patches
    focus_regions_coordinates = ray.get(
        manager.get_tile_coordinates.remote(tile_size=crop_size, level=level)
    )
    list_of_batches = create_list_of_batches_from_list(
        focus_regions_coordinates, region_cropping_batch_size
    )

    task_managers = [WSICropManager.remote(wsi_path) for _ in range(num_croppers)]

    tasks = {}

    for i, batch in enumerate(list_of_batches):
        manager = task_managers[i % num_croppers]
        task = manager.async_get_bma_focus_region_batch.remote(batch, save_dir)
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

    if verbose:
        print(f"Shutting down Ray")
    ray.shutdown()


def crop_wsi_images_all_levels(
    wsi_path,
    save_dir,
    region_cropping_batch_size,
    crop_size=256,
    verbose=True,
    num_cpus=128,
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
                manager.get_tile_coordinates_level_pair.remote(
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
            batch, save_dir, crop_size=crop_size
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

                # Save the patch
                path = os.path.join(save_dir, str(depth), f"{x}_{y}.jpeg")
                patch.save(path)


def dzsave(
    wsi_path, dz_dir, tile_size=256, num_cpus=128, region_cropping_batch_size=256
):
    # create the subdirectories for the different levels 0, ..., 18
    for i in range(19):
        os.makedirs(os.path.join(dz_dir, str(i)), exist_ok=True)

    starttime = time.time()

    print("Cropping from NDPI")
    crop_wsi_images_all_levels(
        wsi_path,
        dz_dir,
        region_cropping_batch_size=region_cropping_batch_size,
        crop_size=tile_size,
        num_cpus=num_cpus,
    )

    print("Cropping Lower Resolution Levels")

    get_depth_from_0_to_11(wsi_path, dz_dir, tile_size=tile_size)

    time_taken = time.time() - starttime

    return time_taken


if __name__ == "__main__":

    import time
    import os
    import pyvips

    # Initialize Ray with the desired number of CPUs
    num_cpus = 128  # Number of CPUs for Ray
    ray.init(num_cpus=num_cpus)

    # Example usage
    starttime = time.time()
    wsi_path = "/media/hdd3/neo/BMA_AML/H19-3465;S10;MSKB - 2023-09-21 13.52.50.ndpi"
    save_dir = "/media/hdd3/neo/tmp_dump_dir"
    dz_dir = "/media/hdd3/neo/tmp_dz_dir"
    region_cropping_batch_size = 256  # Adjust batch size based on your requirements
    crop_size = 256  # Crop size in pixels

    dzsave(
        wsi_path,
        save_dir,
        tile_size=crop_size,
        num_cpus=num_cpus,
        region_cropping_batch_size=region_cropping_batch_size,
    )

    print(f"Time taken using Neo's dzsave: {time.time() - starttime} seconds")

    # test running dzsave on the slide and how long it takes ()
    starttime = time.time()

    print("Running dzsave on the slide and how long it takes")
    image = pyvips.Image.new_from_file(wsi_path)
    dzsave_output_path = dz_dir
    image.dzsave(dzsave_output_path, tile_size=256, overlap=0)

    print(f"Time taken using pyvips dzsave: {time.time() - starttime} seconds")
