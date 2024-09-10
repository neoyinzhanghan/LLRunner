import os
import torch    
import pandas as pd
from PIL import Image
from LLRunner.config import tmp_slide_dir, BMA_specimen_clf_threshold, PBS_specimen_clf_threshold
from LLRunner.slide_processing.specimen_clf import (
    load_bma_specimen_clf_model,
    load_pbs_specimen_clf_model,
    predict_image_bma,
    predict_image_pbs,
)
from tqdm import tqdm

tmp_topview_dir = os.path.join(tmp_slide_dir, "topview")
save_dir = "/media/hdd3/neo/specimen_clf_sanity_check"

os.makedirs(save_dir, exist_ok=True)

is_bma_path = os.path.join(save_dir, "is_bma")
is_pbs_path = os.path.join(save_dir, "is_pbs")
is_not_bma_path = os.path.join(save_dir, "is_not_bma")
is_not_pbs_path = os.path.join(save_dir, "is_not_pbs")
is_both_path = os.path.join(save_dir, "is_both")

os.makedirs(is_bma_path, exist_ok=True)
os.makedirs(is_pbs_path, exist_ok=True)
os.makedirs(is_not_bma_path, exist_ok=True)
os.makedirs(is_not_pbs_path, exist_ok=True)
os.makedirs(is_both_path, exist_ok=True)

bma_model = load_bma_specimen_clf_model()
pbs_model = load_pbs_specimen_clf_model()

metadata = {
    "idx": [],
    "topview_path": [],
    "bma_score": [],
    "pbs_score": [],
}

current_idx = 0

# get the path to all the jpg images in the topview directory
topview_images = [
    os.path.join(tmp_topview_dir, f)
    for f in os.listdir(tmp_topview_dir)
    if f.endswith(".jpg")
]

for image_path in tqdm(topview_images):
    image = Image.open(image_path)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    bma_score = predict_image_bma(bma_model, image, device=device)
    pbs_score = predict_image_pbs(pbs_model, image, device=device)
    
    metadata["idx"].append(current_idx)
    metadata["topview_path"].append(image_path)
    metadata["bma_score"].append(bma_score)
    metadata["pbs_score"].append(pbs_score)

    # save name will the idx.jpg
    save_name = f"{current_idx}.jpg"
    
    if bma_score > BMA_specimen_clf_threshold:
        os.symlink(image_path, os.path.join(is_bma_path, save_name))
    else:
        os.symlink(image_path, os.path.join(is_not_bma_path, save_name))

    if pbs_score > PBS_specimen_clf_threshold:
        os.symlink(image_path, os.path.join(is_pbs_path, save_name))
    else:
        os.symlink(image_path, os.path.join(is_not_pbs_path, save_name))    
    if bma_score > BMA_specimen_clf_threshold and pbs_score > PBS_specimen_clf_threshold:
        os.symlink(image_path, os.path.join(is_both_path, save_name))

    current_idx += 1

metadata_df = pd.DataFrame(metadata)
metadata_df.to_csv(os.path.join(save_dir, "specimen_clf_metadata.csv"), index=False)

# print how many images are in each category, is_bma, is_pbs, is_both, is_neither
print(f"Number of images in is_bma: {len(os.listdir(is_bma_path))}")
print(f"Number of images in is_pbs: {len(os.listdir(is_pbs_path))}")
print(f"Number of images in is_not_bma: {len(os.listdir(is_not_bma_path))}")
print(f"Number of images in is_not_pbs: {len(os.listdir(is_not_pbs_path))}")
print(f"Number of images in is_both: {len(os.listdir(is_both_path))}")