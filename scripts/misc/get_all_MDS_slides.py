import os
import pandas as pd
from subprocess import call
from LLBMA.front_end.api import analyse_bma
from tqdm import tqdm
from LLRunner.read.SST import sst
from LLRunner.custom_errors import AccessionNumberNotFoundError

processed_slides_path = "/media/hdd3/neo/test_diff_results.csv"

# open the processed slides
df = pd.read_csv(processed_slides_path)

# get the wsi_name column as a list
wsi_names = df["wsi_name"].tolist()

MDS_wsi_names_df_dict = {"wsi_name": [], "Dx": [], "sub_Dx": []}
MDS_wsi_names = []

for wsi_name in tqdm(wsi_names, desc="Finding MDS Slides"):
    accession_number = wsi_name.split(";")[0]

    try:
        dx, subdx = sst.get_dx(accession_number)
        if (
            dx is not None
            and subdx is not None
            and "MDS" in dx
            and ("EB1" in subdx or "EB2" in subdx)
        ):
            MDS_wsi_names_df_dict["wsi_name"].append(wsi_name)
            MDS_wsi_names_df_dict["Dx"].append(dx)
            MDS_wsi_names_df_dict["sub_Dx"].append(subdx)

            MDS_wsi_names.append(wsi_name)

    except AccessionNumberNotFoundError:
        continue

print("MDS Slides Found: ", len(MDS_wsi_names_df_dict["wsi_name"]))

# create a dataframe from the dictionary
MDS_wsi_names_df = pd.DataFrame(MDS_wsi_names_df_dict)

# save the dataframe to a csv file in media/hdd3/neo
MDS_wsi_names_df.to_csv("/media/hdd3/neo/MDS_wsi_names.csv", index=False)

slide_save_dir = "/media/hdd3/neo/MDS_slides"
os.makedirs(slide_save_dir, exist_ok=True)


def rsync_slide(slide_path, destination_dir):
    """Run rsync to sync the dzsave output to the destination."""
    rsync_command = [
        "sudo",
        "rsync",
        "-a",  # Adjust options based on your needs
        slide_path,
        destination_dir,
    ]
    result = call(rsync_command)
    if result == 0:
        print(f"Successfully synced {slide_path} to {destination_dir}")
    else:
        print(f"Failed to sync {slide_path}. Error code: {result}")


slide_source_dir = "/pesgisipth/NDPI"

for wsi_name in tqdm(MDS_wsi_names, desc="Syncing MDS Slides"):
    wsi_path = os.path.join(slide_source_dir, wsi_name)
    rsync_slide(wsi_path, slide_save_dir)
