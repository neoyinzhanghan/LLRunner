import os
from LLRunner.slide_processing.run_one_slide import run_one_slide
from LLRunner.config import pipeline_run_history_path

slide_path = "/media/hdd3/neo/tmp_slide_dir/H22-7033;S12;MSK3 - 2023-06-06 19.17.45.ndpi"
wsi_name = "H22-7033;S12;MSK3 - 2023-06-06 19.17.45.ndpi"

# if the file pipeline_run_history_path does not exist, create it with the header
if not os.path.exists(pipeline_run_history_path):
    with open(pipeline_run_history_path, "w") as f:
        f.write("wsi_name,pipeline,datetime_processed,result_dir,error,note,kwargs\n")
        
if __name__ == "__main__":
    run_one_slide(wsi_name=wsi_name,
                  pipeline="BMA-diff",
                  note="Test",
                  hoarding=False,
                  continue_on_error=False,
                  do_extract_features=False,
                  check_specimen_clf=False)