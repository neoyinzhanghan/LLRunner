import os
import openslide
import pandas as pd
from tqdm import tqdm

slide_dir = "/media/hdd2/michael_superdock_bad_slides"
save_dir = "/media/hdd2/michael_superdock_bad_slides"

# create the save directory if it doesn't exist
if not os.path.exists(save_dir):
    os.makedirs(save_dir)
# find all the svs and ndpi files in the slide directory
slide_files = [
    x for x in os.listdir(slide_dir) if x.endswith(".svs") or x.endswith(".ndpi")
]

bad_slides_df_dict = {"slide_name": [], "problem": []}

for slide in tqdm(slide_files):
    try:
        slide_path = os.path.join(slide_dir, slide)
        slide_obj = openslide.OpenSlide(slide_path)
    except Exception as e:
        bad_slides_df_dict["slide_name"].append(slide)
        bad_slides_df_dict["problem"].append(str(e))

print(f"Number of bad slides: {len(bad_slides_df_dict['slide_name'])}")
save_path = os.path.join(slide_dir, "bad_slides.csv")

bad_slides_df = pd.DataFrame(bad_slides_df_dict)
bad_slides_df.to_csv(save_path, index=False)

# now move the bad slides to the save directory
for slide in bad_slides_df_dict["slide_name"]:
    slide_path = os.path.join(slide_dir, slide)
    save_path = os.path.join(save_dir, slide)
    os.rename(slide_path, save_path)
    print(f"Moved {slide} to {save_path}")
