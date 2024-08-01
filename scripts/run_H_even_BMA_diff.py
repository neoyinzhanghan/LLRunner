from tqdm import tqdm
from LLRunner.slide_transfer.metadata_management import (
    initialize_reported_bma_metadata,
    decide_what_to_run,
)

from LLRunner.slide_processing.run_one_slide import run_one_slide

if __name__ == "__main__":

    def H_even_filter(wsi_name):
        """Look for slides with name that starts with H**,
        where ** is an integer divisible by 2.
        """

        if not wsi_name.startswith("H"):
            return False
        else:
            year_num_str = wsi_name[1:3]

            # first check that the year_num_str is a numeric string
            if not year_num_str.isnumeric():
                return False
            else:
                year_num = int(year_num_str)
                return year_num % 2 == 0

    def identity_filter(pipeline_history_df):
        return pipeline_history_df

    initialize_reported_bma_metadata(wsi_name_filter_func=H_even_filter, overwrite=True)

    wsi_names_to_run = decide_what_to_run(
        wsi_name_filter_func=H_even_filter,
        processing_filter_func=identity_filter,
        pipeline="BMA-diff",
    )

    print(f"Found {len(wsi_names_to_run)} slides to run the BMA-diff pipeline on.")

    for wsi_name in tqdm(
        wsi_names_to_run,
        desc="Running BMA-diff pipeline on slides",
        total=len(wsi_names_to_run),
    ):
        run_one_slide(
            wsi_name,
            pipeline="BMA-diff",
            copy_slide=True,
            delete_slide=True,
            note="Running Pipeling on H-even-year slides reported as BMA in part description.",
            hoarding=True,
            continue_on_error=True,
            do_extract_features=False,
            check_specimen_clf=False,
        )
