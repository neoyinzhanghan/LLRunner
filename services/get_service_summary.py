import os
import pandas as pd
from tqdm import tqdm

classes_to_remove = ["U1", "PL2", "PL3", "ER5", "ER6", "U4", "B1", "B2"]

import matplotlib.pyplot as plt
import seaborn as sns
from PIL import Image
import io


def create_hist_KDE_rug_plot(data, title, lines=[]):
    """
    This function takes a list of numbers and creates a histogram with a KDE overlay
    and a rug plot. The plot follows a dark, techno-futuristic theme with high visibility.

    :param data: list of numbers to plot.
    :param title: string, the title of the plot.
    :param lines: list of floats, positions to plot vertical lines.
    :return: PIL Image object of the plot.
    """
    # Set the dark theme with the brightest elements
    sns.set_theme(style="darkgrid")

    # Create the figure with techno theme
    plt.figure(figsize=(10, 6))

    # Create the histogram with KDE plot
    sns.histplot(
        data,
        kde=True,
        color="#606060",
        stat="count",
        edgecolor="none",
    )

    # Add rug plot
    sns.rugplot(data, color="#00FF00", height=0.05, alpha=0.5)

    # Customize the plot to match a techno futuristic theme
    plt.title(title, fontsize=15, color="#00FF00")
    plt.xlabel("Values", fontsize=12, color="#00FF00")
    plt.ylabel("Mass", fontsize=12, color="#00FF00")

    # Customize the KDE line color
    plt.setp(plt.gca().lines, color="#FFFFFF")

    # Change the axis increment numbers to white
    plt.tick_params(axis="x", colors="white")
    plt.tick_params(axis="y", colors="white")

    # Plotting vertical red lines at specified positions
    for line in lines:
        if isinstance(line, (int, float)):
            plt.axvline(x=line, color="red", linestyle="--")

    # Set the spines to a bright color
    for spine in plt.gca().spines.values():
        spine.set_edgecolor("#00FF00")

    # Set the face color of the axes
    plt.gca().set_facecolor("#121212")

    # Set the grid to a brighter color
    plt.grid(color="#777777")

    # Save the plot to a BytesIO buffer instead of saving to disk
    buf = io.BytesIO()
    plt.savefig(buf, format="png", transparent=True, facecolor="#121212")
    buf.seek(0)

    # Convert the buffer to a PIL Image
    image = Image.open(buf)

    # Close the plot to free memory
    plt.close()

    return image


def get_number_of_regions_and_cells(result_dir_path):

    focus_regions_save_subdir = os.path.join(
        result_dir_path, "focus_regions", "high_mag_unannotated"
    )
    cells_save_subdir = os.path.join(result_dir_path, "cells")

    cells_info_path = os.path.join(cells_save_subdir, "cells_info.csv")
    cells_info_df = pd.read_csv(cells_info_path)

    # num_focus_regions_passed is equal to the number of .jpg files in focus_regions_save_subdir
    num_focus_regions_passed = len(
        [
            name
            for name in os.listdir(focus_regions_save_subdir)
            if name.endswith(".jpg")
        ]
    )

    num_unannotated_focus_regions = 0

    for focus_region_name in os.listdir(focus_regions_save_subdir):
        if focus_region_name.endswith(".jpg"):
            idx = int(focus_region_name.split(".jpg")[0])

            if idx not in cells_info_df["focus_region_idx"].values:
                num_unannotated_focus_regions += 1

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
        if os.path.exists(os.path.join(cells_save_subdir, cell_subdir)):
            num_cells_passed += len(
                [
                    name
                    for name in os.listdir(os.path.join(cells_save_subdir, cell_subdir))
                    if name.endswith(".jpg")
                ]
            )

    num_removed_cells = 0

    for cell_subdir in classes_to_remove:
        if os.path.exists(os.path.join(cells_save_subdir, cell_subdir)):
            num_removed_cells += len(
                [
                    name
                    for name in os.listdir(os.path.join(cells_save_subdir, cell_subdir))
                    if name.endswith(".jpg")
                ]
            )

    return (
        num_focus_regions_passed,
        num_unannotated_focus_regions,
        num_cells_passed,
        num_removed_cells,
    )


if __name__ == "__main__":
    test_result_dir = "/media/hdd2/neo/test_slide_result_dir"

    (
        num_focus_regions_passed,
        num_unannotated_focus_regions,
        num_cells_passed,
        num_removed_cells,
    ) = get_number_of_regions_and_cells(test_result_dir)

    print(f"Number of focus regions passed: {num_focus_regions_passed}")
    print(f"Number of unannotated focus regions: {num_unannotated_focus_regions}")
    print(f"Number of cells passed: {num_cells_passed}")
    print(f"Number of cells removed: {num_removed_cells}")

    results_dir = "/media/hdd2/neo/SameDayLLBMAResults"

    # find all the subdirectories of the results_dir that does not start with ERROR_
    subdirs = [
        subdir
        for subdir in os.listdir(results_dir)
        if os.path.isdir(os.path.join(results_dir, subdir))
    ]

    non_error_subdirs = [
        subdir for subdir in subdirs if not subdir.startswith("ERROR_")
    ]

    # find all the subdirectories of the results_dir that start with ERROR_
    error_subdirs = [subdir for subdir in subdirs if subdir.startswith("ERROR_")]

    print(f"Number of error subdirectories: {len(error_subdirs)}")
    print(f"Number of non-error subdirectories: {len(non_error_subdirs)}")
    print(f"Number of total subdirectories: {len(subdirs)}")

    num_focus_regions_passed_list = []
    num_unannotated_focus_regions_list = []
    num_cells_passed_list = []
    num_removed_cells_list = []

    for non_error_subdir in tqdm(non_error_subdirs):
        result_dir_path = os.path.join(results_dir, non_error_subdir)

        (
            num_focus_regions_passed,
            num_unannotated_focus_regions,
            num_cells_passed,
            num_removed_cells,
        ) = get_number_of_regions_and_cells(result_dir_path)

        num_focus_regions_passed_list.append(num_focus_regions_passed)
        num_unannotated_focus_regions_list.append(num_unannotated_focus_regions)
        num_cells_passed_list.append(num_cells_passed)
        num_removed_cells_list.append(num_removed_cells)

    # Create a histogram with KDE overlay and rug plot for the number of focus regions passed
    num_focus_regions_plot = create_hist_KDE_rug_plot(
        num_focus_regions_passed_list, "Number of Focus Regions Passed"
    )

    # Create a histogram with KDE overlay and rug plot for the number of unannotated focus regions
    num_unannotated_focus_regions_plot = create_hist_KDE_rug_plot(
        num_unannotated_focus_regions_list, "Number of Unannotated Focus Regions"
    )

    # Create a histogram with KDE overlay and rug plot for the number of cells passed
    num_cells_passed_plot = create_hist_KDE_rug_plot(
        num_cells_passed_list, "Number of Cells Passed"
    )

    # Create a histogram with KDE overlay and rug plot for the number of removed cells
    num_removed_cells_plot = create_hist_KDE_rug_plot(
        num_removed_cells_list, "Number of Removed Cells"
    )

    save_dir = "/media/hdd2/neo/SaveDaySummaryPlots"

    # save the plots to the save_dir
    num_focus_regions_plot.save(os.path.join(save_dir, "num_focus_regions_plot.png"))
    num_unannotated_focus_regions_plot.save(
        os.path.join(save_dir, "num_unannotated_focus_regions_plot.png")
    )
    num_cells_passed_plot.save(os.path.join(save_dir, "num_cells_passed_plot.png"))
    num_removed_cells_plot.save(os.path.join(save_dir, "num_removed_cells_plot.png"))
