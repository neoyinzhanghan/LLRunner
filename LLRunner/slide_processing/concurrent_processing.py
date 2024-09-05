from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from LLRunner.slide_transfer.metadata_management import (
    decide_what_to_run_with_specimen_clf_cross_machine,
    decide_what_to_run_dzsave_across_machines,
    initialize_reported_bma_metadata,
)
from LLRunner.slide_processing.run_one_slide import (
    run_one_slide_with_specimen_clf,
    find_slide,
)
from LLRunner.slide_processing.dzsave import dzsave_wsi_name
from LLRunner.slide_transfer.slides_management import (
    copy_slide_to_tmp,
    delete_slide_from_tmp,
)


def main_concurrent_bma_processing(
    wsi_name_filter_func, processing_filter_func, note="", delete_slide=True
):
    """Main function to run the overlapping BMA-diff pipeline on slides."""

    # first initialize the reported BMA metadata
    initialize_reported_bma_metadata(
        wsi_name_filter_func=wsi_name_filter_func, overwrite=False
    )

    # then call decide_what_to_run_with_specimen_clf_cross_machine
    wsi_names_to_run_BMA_diff = decide_what_to_run_with_specimen_clf_cross_machine(
        wsi_name_filter_func=wsi_name_filter_func,
        processing_filter_func=processing_filter_func,
        pipeline="BMA-diff",
    )

    wsi_names_to_run_dzsave = decide_what_to_run_dzsave_across_machines(
        wsi_name_filter_func=wsi_name_filter_func,
        processing_filter_func=processing_filter_func,
    )

    # get the union of the two lists
    wsi_names_to_run_union = list(
        set(wsi_names_to_run_BMA_diff + wsi_names_to_run_dzsave)
    )

    # get the intersection of the two lists
    wsi_names_to_run_intersection = list(
        set(wsi_names_to_run_BMA_diff) & set(wsi_names_to_run_dzsave)
    )

    # get the list of slides to run the BMA speciment classification and BMA-diff pipeline on but not the dzsave pipeline
    wsi_names_to_run_just_BMA_diff = list(
        set(wsi_names_to_run_BMA_diff) - set(wsi_names_to_run_dzsave)
    )

    # get the list of slides to run the dzsave pipeline on but not the BMA-diff pipeline
    wsi_names_to_run_just_dzsave = list(
        set(wsi_names_to_run_dzsave) - set(wsi_names_to_run_BMA_diff)
    )

    print(f"Found {len(wsi_names_to_run_union)} slides in total to be processed.")
    print(
        f"Found {len(wsi_names_to_run_intersection)} slides to run both the BMA-diff and dzsave pipelines on."
    )
    print(
        f"Found {len(wsi_names_to_run_just_BMA_diff)} slides to run just the BMA-diff pipeline on."
    )
    print(
        f"Found {len(wsi_names_to_run_just_dzsave)} slides to run just the dzsave pipeline on."
    )

    # Create a ThreadPoolExecutor for handling slide copying in parallel
    with ThreadPoolExecutor(
        max_workers=1
    ) as executor:  # You can adjust max_workers as needed
        slide_copy_futures = {}  # To track slide copying tasks

        # Start copying slides in parallel
        for wsi_name in wsi_names_to_run_union:
            slide_copy_futures[wsi_name] = executor.submit(
                find_slide, wsi_name, copy_slide=True
            )

        # Process slides once copying is done
        for wsi_name in tqdm(
            wsi_names_to_run_union,
            desc="Running BMA-diff and dzsave pipeline on slides",
            total=len(wsi_names_to_run_union),
        ):

            # Wait for the slide copying to complete if it hasn't yet
            slide_copy_future = slide_copy_futures[wsi_name]
            print(slide_copy_future.done())
            if not slide_copy_future.done():
                print(f"Waiting for slide {wsi_name} to be copied...")
                slide_copy_future.result()  # Wait for completion

            # Continue with processing
            slide_path = find_slide(
                wsi_name, copy_slide=False
            )  # Now it should be instantaneous

            if wsi_name in wsi_names_to_run_BMA_diff:
                run_one_slide_with_specimen_clf(
                    wsi_name,
                    pipeline="BMA-diff",
                    copy_slide=False,
                    delete_slide=False,
                    note=note,
                    hoarding=True,
                    continue_on_error=True,
                    do_extract_features=False,
                    check_specimen_clf=False,
                )
            if wsi_name in wsi_names_to_run_dzsave:
                dzsave_wsi_name(wsi_name)

            if delete_slide:
                delete_slide_from_tmp(wsi_name)


if __name__ == "__main__":
    test_slides = [
        "H24-53;S16;MSKU - 2024-06-11 18.39.53.ndpi",
        "H24-736;S11;MSKX - 2024-05-13 11.08.03.ndpi",
        "H24-737;S11;MSKY - 2024-05-13 12.29.33.ndpi",
        "H24-737;S11;MSKY - 2024-05-13 12.34.37.ndpi",
        "H24-737;S12;MSKZ - 2024-05-13 11.02.53.ndpi",
    ]

    def test_wsi_name_filter_func(wsi_name):
        return wsi_name in test_slides

    def identity_filter(pipeline_history_df):
        return pipeline_history_df

    main_concurrent_bma_processing(
        wsi_name_filter_func=test_wsi_name_filter_func,
        processing_filter_func=identity_filter,
        note="Testing concurrent processing",
        delete_slide=True,
    )
