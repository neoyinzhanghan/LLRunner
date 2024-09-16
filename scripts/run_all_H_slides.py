from LLRunner.slide_processing.concurrent_processing import main_concurrent_processing


def start_with_H_filter(wsi_name):
    return wsi_name.startswith("H")


def identity_filter(pipeline_history_df):
    return pipeline_history_df


main_concurrent_processing(
    wsi_name_filter_func=start_with_H_filter,
    processing_filter_func=identity_filter,
    num_rsync_workers=4,
    note="First all-slide BMA-diff and PBS-diff processing with specimen classification. Begin on 2024-09-16.",
    delete_slide=True,
)
