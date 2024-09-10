import os

slide_source_hostname = "172.28.164.166"
slide_source_username = "greg"
slide_source_dir = "/pesgisipth/NDPI"

tmp_slide_dir = "/media/hdd3/neo/tmp_slide_dir"
results_dir = "/media/hdd3/neo/results_dir"
dzsave_dir = "/media/hdd3/neo/dzsave_dir"

ssh_config = {
    "glv2": {"hostname": "172.28.164.44", "username": "greg", "key_filename": None},
    "glv3": {"hostname": "172.28.164.114", "username": "greg", "key_filename": None},
}

available_machines = list(ssh_config.keys())

slide_metadata_path = os.path.join(tmp_slide_dir, "tmp_slide_metadata.csv")

bma_diff_metadata_path = os.path.join(tmp_slide_dir, "bma_diff_metadata.csv")
pbs_diff_metadata_path = os.path.join(tmp_slide_dir, "pbs_diff_metadata.csv")

pipeline_run_history_path = os.path.join(results_dir, "pipeline_run_history.csv")

bma_diff_results_path = os.path.join(results_dir, "bma_diff_results.csv")

dzsave_metadata_path = os.path.join(dzsave_dir, "dzsave_metadata.csv")

available_pipelines = ["BMA-diff", "PBS-diff"]

allowed_extensions = [".ndpi"]

topview_level = 7

BMA_specimen_clf_ckpt_path = "/media/hdd3/neo/MODELS/2024-09-04 Riya-Specimen-Clf-BMA/resnext-epoch=18-val_acc=0.97.ckpt"
BMA_specimen_clf_threshold = 0.5

PBS_specimen_clf_ckpt_path = "/media/hdd3/neo/MODELS/2024-09-10 Riya-Specimen-Clf-PBS/resnext-epoch=45-val_acc=0.96.ckpt"
PBS_specimen_clf_threshold = 0.5
