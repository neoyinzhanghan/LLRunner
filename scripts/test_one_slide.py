import os
from LLRunner.slide_processing.run_one_slide import run_one_slide

slide_path = "/media/hdd3/neo/tmp_slide_dir/H22-7033;S12;MSK3 - 2023-06-06 19.17.45.ndpi"
wsi_name = "H22-7033;S12;MSK3 - 2023-06-06 19.17.45.ndpi"

if __name__ == "__main__":
    run_one_slide(wsi_name=wsi_name,
                  pipeline="BMA-diff",
                  note="Test",
                  hoarding=True,
                  continue_on_error=False,
                  do_extract_features=False,
                  check_specimen_clf=False)