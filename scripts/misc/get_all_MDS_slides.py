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
        if dx is not None and "MDS" in dx:
            MDS_wsi_names_df_dict["wsi_name"].append(wsi_name)
            MDS_wsi_names_df_dict["Dx"].append(dx)
            MDS_wsi_names_df_dict["sub_Dx"].append(subdx)

            MDS_wsi_names.append(wsi_name)

    except AccessionNumberNotFoundError:
        continue

print("MDS Slides Found: ", len(MDS_wsi_names_df_dict["wsi_name"]))

# create a dataframe from the dictionary
MDS_wsi_names_df = pd.DataFrame(MDS_wsi_names_df_dict)

slide_save_dir = "/media/hdd3/neo/BMA_MDS"
os.makedirs(slide_save_dir, exist_ok=True)

# save the dataframe to a csv file in media/hdd3/neo
MDS_wsi_names_df.to_csv("/media/hdd3/neo/BMA_MDS/MDS_wsi_names.csv", index=False)

MDS_results_dir = "/media/hdd3/neo/MDS_results"
os.makedirs(MDS_results_dir, exist_ok=True)
