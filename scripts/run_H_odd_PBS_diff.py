from tqdm import tqdm
from LLRunner.slide_transfer.metadata_management import (
    initialize_reported_pbs_metadata,
    decide_what_to_run,
)

from LLRunner.slide_processing.run_one_slide import run_one_slide

if __name__ == "__main__":

    def H_odd_filter(wsi_name):
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
                return year_num % 2 == 1

    def identity_filter(pipeline_history_df):
        return pipeline_history_df

    initialize_reported_pbs_metadata(wsi_name_filter_func=H_odd_filter, overwrite=False)

    wsi_names_to_run = decide_what_to_run(
        wsi_name_filter_func=H_odd_filter,
        processing_filter_func=identity_filter,
        pipeline="PBS-diff",
    )

    print(f"Found {len(wsi_names_to_run)} slides to run the PBS-diff pipeline on.")

    for wsi_name in tqdm(
        wsi_names_to_run,
        desc="Running PBS-diff pipeline on slides",
        total=len(wsi_names_to_run),
    ):
        run_one_slide(
            wsi_name,
            pipeline="PBS-diff",
            copy_slide=True,
            delete_slide=False,
            note="Running PBS-diff Pipeline on H-odd-year slides reported as PBS in part description.",
            hoarding=True,
            continue_on_error=True,
            do_extract_features=False,
            check_specimen_clf=False,
        )
