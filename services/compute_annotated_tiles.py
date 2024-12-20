import os
import pandas as pd

root_dir = "/media/hdd2/neo/SameDayDzsave"
results_dir = "/media/hdd2/neo/SameDayLLBMAResults"


def string_to_tuple(input_str):
    # Remove the parentheses and split by commas
    return tuple(map(int, input_str.strip("()").split(", ")))


def get_LLBMA_processing_status(slide_h5_name):

    # check to see if there is a subdir in the results_dir that is the wsi_name without .ndpi extension
    wsi_name_no_ext = slide_h5_name.split(".h5")[0]

    # check if the subdir exists
    subdir = os.path.join(results_dir, wsi_name_no_ext)

    subdir_error = os.path.join(results_dir, f"ERROR_{wsi_name_no_ext}")

    if os.path.exists(subdir):
        return "Processed"
    elif os.path.exists(subdir_error):
        return "Error"
    else:
        return "Not Processed"


def get_annotated_focus_region_indices_and_coordinates(slide_h5_name):
    """Get a list of tuples (high_mag_score, idx, row, col, coordinate, image_path) for the annotated focus regions in the slide_h5_name"""

    df_dict = {
        "high_mag_score": [],
        "idx": [],
        "row": [],
        "col": [],
        "coordinate": [],
        "image_path": [],
    }

    for level in range(19):
        df_dict[f"x_{level}"] = []
        df_dict[f"y_{level}"] = []

    if (
        get_LLBMA_processing_status(slide_h5_name=slide_h5_name) == "Error"
        or get_LLBMA_processing_status(slide_h5_name=slide_h5_name) == "Not Processed"
    ):
        raise ValueError(
            f"Cannot get annotated focus regions for {slide_h5_name}. Status: {get_LLBMA_processing_status(slide_h5_name)}"
        )

    # get the subdir for the slide_h5_name
    wsi_name_no_ext = slide_h5_name.split(".h5")[0]
    subdir = os.path.join(results_dir, wsi_name_no_ext)

    # get the high_mag_focus_regions_info.csv file from the selected_focus_regions subdir
    high_mag_focus_regions_info_path = os.path.join(
        subdir, "selected_focus_regions", "high_mag_focus_regions_info.csv"
    )

    # read the high_mag_focus_regions_info.csv file
    high_mag_focus_regions_info_df = pd.read_csv(high_mag_focus_regions_info_path)

    for i, df_row in high_mag_focus_regions_info_df.iterrows():
        high_mag_score = df_row["adequate_confidence_score_high_mag"]

        # round the high_mag_score to 3 decimal places
        high_mag_score = round(high_mag_score, 3)
        idx = df_row["idx"]
        coordinate_string = df_row["coordinate"]
        coordinate = df_row["coordinate"]

        TLx, TLy, BRx, BRy = string_to_tuple(coordinate_string)

        row = TLx // 512
        col = TLy // 512

        image_path = os.path.join(
            subdir,
            "selected_focus_regions",
            "high_mag_annotated",
            f"{idx}.jpg",
        )

        df_dict["high_mag_score"].append(high_mag_score)
        df_dict["idx"].append(idx)
        df_dict["row"].append(row)
        df_dict["col"].append(col)
        df_dict["coordinate"].append(coordinate)
        df_dict["image_path"].append(image_path)

        for level in range(19):
            downsample_level = 18 - level
            downsample_factor = 2**downsample_level

            df_dict[f"x_{level}"].append(TLx / downsample_factor)
            df_dict[f"y_{level}"].append(TLy / downsample_factor)

    return pd.DataFrame(df_dict)


if __name__ == "__main__":
    # find all the h5 files in the root_dir
    slide_h5_names = [
        slide_name for slide_name in os.listdir(root_dir) if slide_name.endswith(".h5")
    ]

    for slide_h5_name in slide_h5_names:
        print(
            f"Status for {slide_h5_name}: {get_LLBMA_processing_status(slide_h5_name)}"
        )
