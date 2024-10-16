import time
import pandas as pd
from LLRunner.slide_processing.concurrent_processing import main_concurrent_dzsave_h5

wsi_tracker_csv_path = "/media/hdd3/neo/test_diff_results.csv"

# open the WSI tracker
df = pd.read_csv(wsi_tracker_csv_path)

# get the wsi_name as a list
wsi_names = df["wsi_name"].tolist()


def wsi_name_filter(wsi_name: str) -> bool:
    """
    Simple filter function to filter out the wsi names.
    """
    return wsi_name in wsi_names


start_time = time.time()
main_concurrent_dzsave_h5(
    wsi_name_filter_func=wsi_name_filter,
    num_rsync_workers=4,
    delete_slide=True,
)
total_time = time.time() - start_time


print(f"Total time taken: {total_time:.2f} seconds for {len(wsi_names)} WSIs")
