import os

slide_source_hostname = "172.28.164.166"
slide_source_username = "greg"

slide_source_dir = "greg@172.28.164.166:/pesgisipth/NDPI"
tmp_slide_dir = "/media/hdd3/neo/tmp_slide_dir"
results_dir = "/media/hdd3/neo/results_dir"

slide_metadata_path = os.path.join(tmp_slide_dir, "tmp_slide_metadata.csv")

bma_diff_metadata_path = os.path.join(tmp_slide_dir, "bma_diff_metadata.csv")

pipeline_run_history_path = os.path.join(results_dir, "pipeline_run_history.csv")

bma_diff_results_path = os.path.join(results_dir, "bma_diff_results.csv")

available_pipelines = [
    "BMA-diff"
]

pipeline_results_path_dict = {
    "BMA-diff": os.path.join(results_dir, "BMA-diff_results.csv")
}

allowed_extensions = [".ndpi"]

topview_level = 7