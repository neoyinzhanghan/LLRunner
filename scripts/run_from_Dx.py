import os
from tqdm import tqdm
from LLRunner.slide_processing import run_one_slide
from LLRunner.slide_transfer.retrieve_from_tmp import find_BMA_from_Dx
from LLRunner.config import tmp_slide_dir

Dx_filters = ["AML", "Normal BMA", "Plasma cell myeloma"]

wsi_names = []

for Dx_filter in Dx_filters:
    wsi_names.extend(find_BMA_from_Dx(Dx_filter))

for wsi_name in tqdm(wsi_names, desc="Running BMA-diff pipeline", total=len(wsi_names)):
    print(f"Running BMA-diff pipeline on {wsi_name}")
    run_one_slide(wsi_name=wsi_name,
                  pipeline="BMA-diff",
                  note="LLBMA on AML, Normal BMA, and Plasma cell myeloma slides, version=0.1",
                  hoarding=True,
                  continue_on_error=True,
                  do_extract_features=False,
                  check_specimen_clf=False
                  )