import os

root_dir = "/media/hdd2/neo/SameDayDzsave"
results_dir = "/media/hdd2/neo/SameDayLLBMAResults"


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


if __name__ == "__main__":
    # find all the h5 files in the root_dir
    slide_h5_names = [
        slide_name for slide_name in os.listdir(root_dir) if slide_name.endswith(".h5")
    ]

    for slide_h5_name in slide_h5_names:
        print(
            f"Status for {slide_h5_name}: {get_LLBMA_processing_status(slide_h5_name)}"
        )
