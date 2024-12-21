import os
from tqdm import tqdm
from get_service_summary import get_number_of_regions_and_cells


def has_less_than_N_cells(result_dir_path, N=200):
    (
        num_focus_regions_passed,
        num_unannotated_focus_regions,
        num_cells_passed,
        num_removed_cells,
    ) = get_number_of_regions_and_cells(result_dir_path)

    if num_cells_passed < N:
        return True
    return


if __name__ == "__main__":

    test_result_dir = "/media/hdd2/neo/test_slide_result_dir"

    if has_less_than_N_cells(test_result_dir):
        print("Number of cells passed is less than 200. :(")
    else:
        print("Number of cells passed is greater than 200. :)")

    root_dir = "/media/hdd2/neo/SameDayLLBMAResults"

    # find the list of all subdirectories in the root_dir
    subdirs = [
        subdir
        for subdir in os.listdir(root_dir)
        if os.path.isdir(os.path.join(root_dir, subdir))
    ]

    # find the subdirectories that do not start with ERROR_
    non_error_subdirs = [
        subdir for subdir in subdirs if not subdir.startswith("ERROR_")
    ]

    total_non_error_subdirs = len(non_error_subdirs)
    total_low_cells = 0
    low_cells_dirs = []

    for non_error_subdir in tqdm(non_error_subdirs, desc="Checking subdirectories"):
        result_dir_path = os.path.join(root_dir, non_error_subdir)

        if has_less_than_N_cells(result_dir_path):
            print(f"{result_dir_path} has less than 200 cells.")
            total_low_cells += 1
            low_cells_dirs.append(result_dir_path)
        else:
            print(f"{result_dir_path} has more than 200 cells.")

    print(f"Total number of subdirectories with less than 200 cells: {total_low_cells}")
    print(f"Total number of subdirectories: {total_non_error_subdirs}")

    print("Subdirectories with less than 200 cells:")
    for low_cells_dir in low_cells_dirs:
        print(low_cells_dir)

    # rename the subdirectories, adding ERROR_ to the beginning of the name
    for low_cells_dir in low_cells_dirs:
        new_dir_name = f"ERROR_{low_cells_dir.split('/')[-1]}"
        os.rename(low_cells_dir, os.path.join(root_dir, new_dir_name))
        print(f"{low_cells_dir} renamed to {new_dir_name}")

        # write an error.txt file in the renamed directory
        with open(os.path.join(root_dir, new_dir_name, "error.txt"), "w") as f:
            f.write(
                "Number of cells passed is less than 200. This error is written posthoc before the number of cells checking is internalized in to the pipeline."
            )
            print(f"error.txt written in {new_dir_name}")
