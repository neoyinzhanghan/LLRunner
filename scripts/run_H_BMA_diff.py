from LLRunner.slide_transfer.metadata_management import (
    pool_metadata_one_time,
    decide_what_to_run,
)

from LLRunner.slide_processing.run_one_slide import run_one_slide

if __name__ == "__main__":

    def H_filter(wsi_name):
        """Look for slides with name that starts with H."""
        return wsi_name.startswith("H")

    def identity_filter(pipeline_history_df):
        return pipeline_history_df

    # pool_metadata_one_time(wsi_name_filter_func=H_filter, overwrite=True)

    wsi_names_to_run = decide_what_to_run(identity_filter, pipeline="BMA-diff")
    print(f"Found {len(wsi_names_to_run)} slides to run the BMA-diff pipeline on.")

    wsi_name = wsi_names_to_run[0]
    run_one_slide(
        wsi_name,
        pipeline="BMA-diff",
        copy_slide=True,
        delete_slide=True,
        note="Testing the pipeline.",
        hoarding=False,
        continue_on_error=True,
        do_extract_features=False,
        check_specimen_clf=False,
    )
