import os
from tqdm import tqdm

classes_to_remove = ["U1", "PL2", "PL3", "ER5", "ER6", "U4", "B1", "B2"]


def get_number_of_regions_and_cells(result_dir_path):

    focus_regions_save_subdir = os.path.join(
        result_dir_path, "focus_regions", "high_mag_unannotated"
    )
    cells_save_subdir = os.path.join(result_dir_path, "cells")

    # num_focus_regions_passed is equal to the number of .jpg files in focus_regions_save_subdir
    num_focus_regions_passed = len(
        [
            name
            for name in os.listdir(focus_regions_save_subdir)
            if name.endswith(".jpg")
        ]
    )

    # find all the further subdirectories in cells_save_subdir
    cell_subdirs = [
        subdir
        for subdir in os.listdir(cells_save_subdir)
        if os.path.isdir(os.path.join(cells_save_subdir, subdir))
    ]

    # remove the cell_subdirs that are in classes_to_remove
    cell_subdirs = [
        subdir for subdir in cell_subdirs if subdir not in classes_to_remove
    ]

    # num_cells_passed is equal to the number of .jpg files in the cell_subdirs
    num_cells_passed = 0

    for cell_subdir in cell_subdirs:
        num_cells_passed += len(
            [
                name
                for name in os.listdir(os.path.join(cells_save_subdir, cell_subdir))
                if name.endswith(".jpg")
            ]
        )

    num_removed_cells = 0

    for cell_subdir in classes_to_remove:
        num_removed_cells += len(
            [
                name
                for name in os.listdir(os.path.join(cells_save_subdir, cell_subdir))
                if name.endswith(".jpg")
            ]
        )

    return num_focus_regions_passed, num_cells_passed, num_removed_cells


if __name__ == "__main__":
    test_result_dir = "/media/hdd2/neo/test_slide_result_dir"

    num_focus_regions_passed, num_cells_passed, num_removed_cells = (
        get_number_of_regions_and_cells(test_result_dir)
    )

    print(f"Number of focus regions passed: {num_focus_regions_passed}")
    print(f"Number of cells passed: {num_cells_passed}")
    print(f"Number of cells removed: {num_removed_cells}")

    # results_dir = "/media/hdd2/neo/SameDayLLBMAResults"

    # # find all the subdirectories of the results_dir that does not start with ERROR_
    # subdirs = [
    #     subdir
    #     for subdir in os.listdir(results_dir)
    #     if os.path.isdir(os.path.join(results_dir, subdir))
    # ]

    # non_error_subdirs = [
    #     subdir for subdir in subdirs if not subdir.startswith("ERROR_")
    # ]

    # # find all the subdirectories of the results_dir that start with ERROR_
    # error_subdirs = [subdir for subdir in subdirs if subdir.startswith("ERROR_")]

    # print(f"Number of error subdirectories: {len(error_subdirs)}")
    # print(f"Number of non-error subdirectories: {len(non_error_subdirs)}")
    # print(f"Number of total subdirectories: {len(subdirs)}")

    # for non_error_subdir in tqdm(non_error_subdirs):
    #     num_focus_regions_passed, num_cells_passed = get_number_of_regions_and_cells(
    #         os.path.join(results_dir, non_error_subdir)
    #     )

    #     print(f"Number of focus regions passed: {num_focus_regions_passed}")
    #     print(f"Number of cells passed: {num_cells_passed}")
    #     print()
