from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from LLRunner.slide_transfer.metadata_management import (
    decide_what_to_run_with_specimen_clf_cross_machine,
    decide_what_to_run_dzsave_across_machines,
    decide_what_to_run_dzsave_local,
    initialize_reported_bma_metadata,
    initialize_all_metadata,
    which_are_already_ran,
    which_are_already_dzsaved_h5
)
from LLRunner.slide_processing.run_one_slide import (
    run_one_slide_with_specimen_clf,
    find_slide,
)

# from LLRunner.slide_processing.dzsave import dzsave_wsi_name
from LLRunner.slide_transfer.slides_management import (
    copy_slide_to_tmp,
    delete_slide_from_tmp,
)

from LLRunner.slide_processing.dzsave_h5 import dzsave_wsi_name_h5
from LLRunner.deletion.delete_slide_results import delete_results_from_note


def create_list_of_batches_from_list(list, batch_size):
    """
    This function creates a list of batches from a list.

    :param list: a list
    :param batch_size: the size of each batch
    :return: a list of batches

    >>> create_list_of_batches_from_list([1, 2, 3, 4, 5], 2)
    [[1, 2], [3, 4], [5]]
    >>> create_list_of_batches_from_list([1, 2, 3, 4, 5, 6], 3)
    [[1, 2, 3], [4, 5, 6]]
    >>> create_list_of_batches_from_list([], 3)
    []
    >>> create_list_of_batches_from_list([1, 2], 3)
    [[1, 2]]
    """

    list_of_batches = []

    for i in range(0, len(list), batch_size):
        batch = list[i : i + batch_size]
        list_of_batches.append(batch)

    return list_of_batches


def main_concurrent_processing(
    wsi_name_filter_func,
    processing_filter_func,
    slide_batch_size=50,
    num_rsync_workers=1,
    note="",
    delete_slide=True,
):
    """Main function to run the overlapping BMA-diff and PBS-diff pipeline on slides."""

    # first initialize the reported BMA metadata
    initialize_reported_bma_metadata(
        wsi_name_filter_func=wsi_name_filter_func, overwrite=False
    )

    # then call decide_what_to_run_with_specimen_clf_cross_machine
    wsi_names_to_run_diff = decide_what_to_run_with_specimen_clf_cross_machine(
        wsi_name_filter_func=wsi_name_filter_func,
        processing_filter_func=processing_filter_func,
        pipelines=["BMA-diff", "PBS-diff"],
        rerun=True,
    )

    total_before_check = len(wsi_names_to_run_diff)

    already_ran_wsi_names = which_are_already_ran(wsi_names_to_run_diff, note=note)

    wsi_names_to_run_diff = list(
        set(wsi_names_to_run_diff) - set(already_ran_wsi_names)
    )

    print(
        f"Found {total_before_check} slides to run the BMA-diff and PBS-diff pipelines on."
    )
    print(
        f"Found {len(already_ran_wsi_names)} slides that have already been processed."
    )
    print(f"Only {len(wsi_names_to_run_diff)} slides will be processed in this run.")

    wsi_names_to_run_dzsave = decide_what_to_run_dzsave_across_machines(
        wsi_name_filter_func=wsi_name_filter_func,
        processing_filter_func=processing_filter_func,
    )

    # get the union of the two lists
    wsi_names_to_run_union = list(set(wsi_names_to_run_diff + wsi_names_to_run_dzsave))

    # get the intersection of the two lists
    wsi_names_to_run_intersection = list(
        set(wsi_names_to_run_diff) & set(wsi_names_to_run_dzsave)
    )

    # get the list of slides to run the BMA speciment classification and BMA-diff pipeline on but not the dzsave pipeline
    wsi_names_to_run_just_diff = list(
        set(wsi_names_to_run_diff) - set(wsi_names_to_run_dzsave)
    )

    # get the list of slides to run the dzsave pipeline on but not the BMA-diff pipeline
    wsi_names_to_run_just_dzsave = list(
        set(wsi_names_to_run_dzsave) - set(wsi_names_to_run_diff)
    )

    print(f"Found {len(wsi_names_to_run_union)} slides in total to be processed.")
    print(
        f"Found {len(wsi_names_to_run_intersection)} slides to run both the BMA-diff and dzsave pipelines on."
    )
    print(
        f"Found {len(wsi_names_to_run_just_diff)} slides to run just the BMA-diff pipeline on."
    )
    print(
        f"Found {len(wsi_names_to_run_just_dzsave)} slides to run just the dzsave pipeline on."
    )

    slides_batches = create_list_of_batches_from_list(
        wsi_names_to_run_diff, slide_batch_size
    )

    for i, slide_batch in tqdm(
        enumerate(slides_batches),
        desc="Batch Progress",
        total=len(slides_batches),
    ):

        # Create a ThreadPoolExecutor for handling slide copying in parallel
        with ThreadPoolExecutor(
            max_workers=num_rsync_workers
        ) as executor:  # You can adjust max_workers as needed
            slide_copy_futures = {}  # To track slide copying tasks

            # Start copying slides in parallel
            for wsi_name in slide_batch:
                slide_copy_futures[wsi_name] = executor.submit(
                    find_slide, wsi_name, copy_slide=True
                )

            # Process slides once copying is done
            for wsi_name in tqdm(
                slide_batch,
                desc=f"Running BMA or PBS diff and dzsave pipeline on slides for batch {i+1}/{len(slides_batches)}",
                total=len(slide_batch),
            ):

                # Wait for the slide copying to complete if it hasn't yet
                slide_copy_future = slide_copy_futures[wsi_name]
                print(slide_copy_future.done())
                if not slide_copy_future.done():
                    print(f"Waiting for slide {wsi_name} to be copied...")
                    slide_copy_future.result()  # Wait for completion

                # # Continue with processing
                # slide_path = find_slide(
                #     wsi_name, copy_slide=False
                # )  # Now it should be instantaneous

                if wsi_name in wsi_names_to_run_diff:
                    print(f"Running BMA or PBS diff pipeline on {wsi_name}")
                    run_one_slide_with_specimen_clf(
                        wsi_name,
                        copy_slide=False,
                        delete_slide=False,
                        note=note,
                        hoarding=True,
                        continue_on_error=True,
                        do_extract_features=False,
                        check_specimen_clf=False,
                    )
                    print(f"Finished running BMA or PBS diff pipeline on {wsi_name}")

                # if wsi_name in wsi_names_to_run_dzsave: #TODO we are not going to run dzsave until we fix the dzsave isilon archiving problem
                #     print(f"Running dzsave pipeline on {wsi_name}")
                #     dzsave_wsi_name(wsi_name)
                #     print(f"Finished dzsaving {wsi_name}")

                if delete_slide:
                    delete_slide_from_tmp(wsi_name)


from subprocess import call
from concurrent.futures import ThreadPoolExecutor


def rsync_slide_output(wsi_name, output_path, destination_dir):
    """Run rsync to sync the dzsave output to the destination."""
    rsync_command = [
        "sudo",
        "rsync",
        "-avz",  # Adjust options based on your needs
        output_path,
        destination_dir,
    ]
    result = call(rsync_command)
    if result == 0:
        print(f"Successfully synced {wsi_name} to {destination_dir}")
    else:
        print(f"Failed to sync {wsi_name}. Error code: {result}")


def main_concurrent_dzsave_h5(
    wsi_name_filter_func,
    processing_filter_func,
    slide_batch_size=50,
    num_rsync_workers=1,
    note="",
    delete_slide=True,
):
    """Main function to run the overlapping BMA-diff and PBS-diff pipeline on slides."""

    # first initialize the reported BMA metadata
    initialize_reported_bma_metadata(
        wsi_name_filter_func=wsi_name_filter_func, overwrite=False
    )

    # then call decide_what_to_run_with_specimen_clf_cross_machine
    wsi_names_to_run_dzsave = decide_what_to_run_dzsave_local(
        wsi_name_filter_func=wsi_name_filter_func,
        processing_filter_func=processing_filter_func,
    )

    total_before_check = len(wsi_names_to_run_dzsave_h5)

    already_ran_wsi_names = which_are_already_dzsaved_h5(
        wsi_names_to_run_dzsave_h5, note=note
    )

    wsi_names_to_run_dzsave_h5 = list(
        set(wsi_names_to_run_dzsave_h5) - set(already_ran_wsi_names)
    )

    print(
        f"Found {total_before_check} slides to run the BMA-diff and PBS-diff pipelines on."
    )
    print(
        f"Found {len(already_ran_wsi_names)} slides that have already been processed."
    )
    print(
        f"Only {len(wsi_names_to_run_dzsave_h5)} slides will be processed in this run."
    )

    print(
        f"Found {len(wsi_names_to_run_dzsave_h5)} slides to run the dzsave pipeline on."
    )

    slides_batches = create_list_of_batches_from_list(
        wsi_names_to_run_dzsave, slide_batch_size
    )

    # Create a separate ThreadPoolExecutor for handling rsync operations
    rsync_executor = ThreadPoolExecutor(max_workers=num_rsync_workers)

    for i, slide_batch in tqdm(
        enumerate(slides_batches),
        desc="Batch Progress",
        total=len(slides_batches),
    ):

        # Create a ThreadPoolExecutor for handling slide copying in parallel
        with ThreadPoolExecutor(
            max_workers=num_rsync_workers
        ) as executor:  # You can adjust max_workers as needed
            slide_copy_futures = {}  # To track slide copying tasks

            # Start copying slides in parallel
            for wsi_name in slide_batch:
                slide_copy_futures[wsi_name] = executor.submit(
                    find_slide, wsi_name, copy_slide=True
                )

            # Process slides once copying is done
            for wsi_name in tqdm(
                slide_batch,
                desc=f"Running BMA or PBS diff and dzsave pipeline on slides for batch {i+1}/{len(slides_batches)}",
                total=len(slide_batch),
            ):

                # Wait for the slide copying to complete if it hasn't yet
                slide_copy_future = slide_copy_futures[wsi_name]
                if not slide_copy_future.done():
                    print(f"Waiting for slide {wsi_name} to be copied...")
                    slide_copy_future.result()  # Wait for completion

                # If slide needs to be processed
                if wsi_name in wsi_names_to_run_dzsave:
                    print(f"Running BMA or PBS diff pipeline on {wsi_name}")

                    # Assuming the dzsave output is located at a specific path:
                    output_path = f"/path/to/output/{wsi_name}.h5"

                    # Run dzsave and metadata tracking here

                    # After dzsave is done, start rsync of the output asynchronously
                    destination_dir = "/pesgisipth/neo/slide_tiles_h5"
                    rsync_executor.submit(
                        rsync_slide_output, wsi_name, output_path, destination_dir
                    )

                if delete_slide:
                    delete_slide_from_tmp(wsi_name)

    # Shutdown the rsync executor after all batches are processed
    rsync_executor.shutdown(wait=False)


# def main_serial_bma_processing(
#     wsi_name_filter_func, processing_filter_func, note="", delete_slide=True
# ):
#     """Main function to run the overlapping BMA-diff pipeline on slides in a serial manner."""

#     # first initialize the reported BMA metadata
#     initialize_reported_bma_metadata(
#         wsi_name_filter_func=wsi_name_filter_func, overwrite=False
#     )

#     # then call decide_what_to_run_with_specimen_clf_cross_machine
#     wsi_names_to_run_BMA_diff = decide_what_to_run_with_specimen_clf_cross_machine(
#         wsi_name_filter_func=wsi_name_filter_func,
#         processing_filter_func=processing_filter_func,
#         pipelines=["BMA-diff", "PBS-diff"],
#     )

#     wsi_names_to_run_dzsave = decide_what_to_run_dzsave_across_machines(
#         wsi_name_filter_func=wsi_name_filter_func,
#         processing_filter_func=processing_filter_func,
#     )

#     # get the union of the two lists
#     wsi_names_to_run_union = list(
#         set(wsi_names_to_run_BMA_diff + wsi_names_to_run_dzsave)
#     )

#     # get the intersection of the two lists
#     wsi_names_to_run_intersection = list(
#         set(wsi_names_to_run_BMA_diff) & set(wsi_names_to_run_dzsave)
#     )

#     # get the list of slides to run the BMA specimen classification and BMA-diff pipeline on but not the dzsave pipeline
#     wsi_names_to_run_just_BMA_diff = list(
#         set(wsi_names_to_run_BMA_diff) - set(wsi_names_to_run_dzsave)
#     )

#     # get the list of slides to run the dzsave pipeline on but not the BMA-diff pipeline
#     wsi_names_to_run_just_dzsave = list(
#         set(wsi_names_to_run_dzsave) - set(wsi_names_to_run_BMA_diff)
#     )

#     print(f"Found {len(wsi_names_to_run_union)} slides in total to be processed.")
#     print(
#         f"Found {len(wsi_names_to_run_intersection)} slides to run both the BMA-diff and dzsave pipelines on."
#     )
#     print(
#         f"Found {len(wsi_names_to_run_just_BMA_diff)} slides to run just the BMA-diff pipeline on."
#     )
#     print(
#         f"Found {len(wsi_names_to_run_just_dzsave)} slides to run just the dzsave pipeline on."
#     )

#     # Process slides serially
#     for wsi_name in tqdm(
#         wsi_names_to_run_union,
#         desc="Running BMA-diff and dzsave pipeline on slides",
#         total=len(wsi_names_to_run_union),
#     ):
#         # Find the slide and ensure it's ready for processing
#         slide_path = find_slide(wsi_name, copy_slide=True)

#         # Run the BMA-diff pipeline if required
#         if wsi_name in wsi_names_to_run_BMA_diff:
#             print(f"Running BMA-diff pipeline on {wsi_name}")
#             run_one_slide_with_specimen_clf(
#                 wsi_name,
#                 pipeline="BMA-diff",
#                 copy_slide=False,
#                 delete_slide=False,
#                 note=note,
#                 hoarding=True,
#                 continue_on_error=True,
#                 do_extract_features=False,
#                 check_specimen_clf=False,
#             )
#             print(f"Finished running BMA-diff pipeline on {wsi_name}")

#         # Run the dzsave pipeline if required
#         if wsi_name in wsi_names_to_run_dzsave:
#             print(f"Running dzsave pipeline on {wsi_name}")
#             dzsave_wsi_name(wsi_name)
#             print(f"Finished dzsaving {wsi_name}")

#         # Optionally delete the slide after processing
#         if delete_slide:
#             delete_slide_from_tmp(wsi_name)


if __name__ == "__main__":
    pass
    # import os
    # import time

    # test_slides = [
    #     "H24-00033-1A-1 - 2024-07-17 13.47.29.ndpi",
    #     "H24-00033-1A-2 - 2024-07-17 13.49.14.ndpi",
    #     "H24-00033-1A-2 - 2024-07-17 13.49.15.ndpi",
    #     "H24-00033-1A-3 - 2024-07-17 13.50.18.ndpi",
    #     "H24-00033-1A-4 - 2024-07-17 13.51.50.ndpi",
    #     "H24-00033-1A-5 - 2024-07-17 13.53.43.ndpi",
    #     "H24-00033-1A-6 - 2024-07-17 13.55.33.ndpi",
    #     "H24-00033-1A-7 - 2024-07-17 13.57.28.ndpi",
    #     "H24-00033-2A-10 - 2024-07-17 14.10.06.ndpi",
    #     "H24-00033-2A-11 - 2024-07-17 14.12.03.ndpi",
    #     "H24-00033-2A-1 - 2024-07-17 13.59.47.ndpi",
    #     "H24-00033-2A-1 - 2024-07-17 13.59.48.ndpi",
    #     "H24-00033-2A-12 - 2024-07-17 14.13.57.ndpi",
    #     "H24-00033-2A-2 - 2024-07-17 14.01.37.ndpi",
    #     "H24-00033-2A-3 - 2024-07-17 14.20.15.ndpi",
    #     "H24-00033-2A-3 - 2024-07-17 14.20.16.ndpi",
    #     "H24-00033-2A-4 - 2024-07-17 14.19.54.ndpi",
    #     "H24-00033-2A-5 - 2024-07-17 14.19.32.ndpi",
    #     "H24-00033-2A-6 - 2024-07-17 14.02.50.ndpi",
    #     "H24-00033-2A-7 - 2024-07-17 14.04.40.ndpi",
    #     "H24-00033-2A-8 - 2024-07-17 14.06.19.ndpi",
    #     "H24-00033-2A-9 - 2024-07-17 14.08.14.ndpi",
    #     "H24-00034-1A-1 - 2024-07-12 08.44.34.ndpi",
    #     "H24-00034-1A-1 - 2024-07-16 14.09.21.ndpi",
    #     "H24-00035-1A-1 - 2024-07-12 08.46.29.ndpi",
    #     "H24-00035-1A-1 - 2024-07-16 14.09.39.ndpi",
    #     "H24-00035-2A-1 - 2024-07-12 08.47.11.ndpi",
    #     "H24-00035-2A-1 - 2024-07-16 14.09.56.ndpi",
    #     "H24-00035-2A-1 - 2024-07-16 15.44.02.ndpi",
    #     "H24-1160;S15;MSK3 - 2024-07-10 23.38.36.ndpi",
    #     "H24-1160;S15;MSK3 - 2024-07-10 23.43.52.ndpi",
    #     "H24-1160;S15;MSK3 - 2024-07-10 23.53.16.ndpi",
    #     "H24-1160;S16;MSK4 - 2024-07-10 23.48.25.ndpi",
    #     "H24-1193;S15;MSKZ - 2024-07-10 23.15.13.ndpi",
    #     "H24-1193;S15;MSKZ - 2024-07-10 23.19.43.ndpi",
    #     "H24-1193;S15;MSKZ - 2024-07-10 23.57.50.ndpi",
    #     "H24-1193;S15;MSKZ - 2024-07-11 00.02.21.ndpi",
    #     "H24-1193;S16;MSK- - 2024-07-10 23.24.20.ndpi",
    #     "H24-1311;S11;MSKN - 2024-07-10 22.37.15.ndpi",
    #     "H24-1311;S11;MSKN - 2024-07-10 23.29.17.ndpi",
    #     "H24-1311;S11;MSKN - 2024-07-10 23.34.07.ndpi",
    #     "H24-1311;S12;MSKO - 2024-07-10 22.32.48.ndpi",
    #     "H24-1508;S13;MSK7 - 2024-07-10 22.47.39.ndpi",
    #     "H24-1508;S13;MSK7 - 2024-07-10 23.06.04.ndpi",
    #     "H24-1508;S13;MSK7 - 2024-07-10 23.10.35.ndpi",
    #     "H24-1508;S17;MSKA - 2024-07-10 22.42.39.ndpi",
    #     "H24-1514;S15;MSKW - 2024-07-10 22.57.01.ndpi",
    #     "H24-1514;S15;MSKW - 2024-07-10 23.01.33.ndpi",
    #     "H24-1514;S15;MSKW - 2024-07-23 22.35.32.ndpi",
    #     "H24-1514;S15;MSKW - 2024-07-23 22.40.04.ndpi",
    #     "H24-1514;S17;MSKY - 2024-07-10 22.52.10.ndpi",
    #     "H24-1514;S17;MSKY - 2024-07-23 22.44.33.ndpi",
    #     "H24-1896;S15;MSK9 - 2024-07-10 21.48.32.ndpi",
    #     "H24-1896;S15;MSK9 - 2024-07-10 21.54.13.ndpi",
    #     "H24-1896;S15;MSK9 - 2024-07-10 22.22.36.ndpi",
    #     "H24-1896;S15;MSK9 - 2024-07-10 22.28.11.ndpi",
    #     "H24-1896;S28;MSKC - 2024-07-10 22.18.01.ndpi",
    #     "H24-2001;S15;MSK8 - 2024-05-13 16.53.08.ndpi",
    #     "H24-2001;S15;MSK8 - 2024-05-13 16.58.07.ndpi",
    #     "H24-2001;S16;MSK9 - 2024-05-13 16.48.13.ndpi",
    #     "H24-2363;S15;MSKZ - 2024-05-13 17.03.00.ndpi",
    #     "H24-2363;S15;MSKZ - 2024-05-13 17.08.03.ndpi",
    #     "H24-2363;S16;MSK- - 2024-05-13 17.13.08.ndpi",
    #     "H24-2522;S11;MSKS - 2024-05-13 12.39.46.ndpi",
    #     "H24-2522;S11;MSKS - 2024-05-13 13.45.53.ndpi",
    #     "H24-2522;S11;MSKS - 2024-05-13 14.11.59.ndpi",
    #     "H24-2522;S11;MSKS - 2024-05-13 17.18.01.ndpi",
    #     "H24-2522;S12;MSKT - 2024-05-13 13.20.27.ndpi",
    #     "H24-2591;S11;MSKY - 2024-05-13 14.01.20.ndpi",
    #     "H24-2591;S11;MSKY - 2024-05-29 05.19.08.ndpi",
    #     "H24-2591;S11;MSKY - 2024-05-29 05.24.09.ndpi",
    #     "H24-2591;S11;MSKY - 2024-05-29 05.29.20.ndpi",
    #     "H24-2591;S13;MSK- - 2024-05-13 11.47.58.ndpi",
    #     "H24-2796;S13;MSK7 - 2024-07-10 22.03.30.ndpi",
    #     "H24-2796;S13;MSK7 - 2024-07-10 22.08.07.ndpi",
    #     "H24-2796;S13;MSK7 - 2024-07-10 22.13.18.ndpi",
    #     "H24-2796;S14;MSK8 - 2024-07-10 21.58.44.ndpi",
    #     "H24-3021;S15;MSK1 - 2024-07-10 21.29.52.ndpi",
    #     "H24-3021;S15;MSK1 - 2024-07-10 21.39.10.ndpi",
    #     "H24-3021;S16;MSK2 - 2024-07-10 21.34.23.ndpi",
    #     "H24-3093;S13;MSK8 - 2024-05-20 10.44.31.ndpi",
    #     "H24-3093;S13;MSK8 - 2024-05-20 12.54.46.ndpi",
    #     "H24-3093;S13;MSK8 - 2024-05-20 12.59.35.ndpi",
    #     "H24-3110;S13;MSKY - 2024-05-20 13.04.25.ndpi",
    #     "H24-3110;S13;MSKY - 2024-05-20 13.09.40.ndpi",
    #     "H24-3110;S13;MSKY - 2024-05-20 13.29.47.ndpi",
    #     "H24-3169;S13;MSK2 - 2024-05-20 13.14.56.ndpi",
    #     "H24-3169;S13;MSK2 - 2024-05-20 13.19.52.ndpi",
    #     "H24-3169;S13;MSK2 - 2024-05-20 13.34.38.ndpi",
    #     "H24-3169;S13;MSK2 - 2024-05-20 14.46.55.ndpi",
    #     "H24-3225;S10;MSK2 - 2024-05-20 11.29.45.ndpi",
    #     "H24-3225;S10;MSK2 - 2024-05-20 13.24.45.ndpi",
    #     "H24-3236;S15;MSKZ - 2024-05-20 10.49.22.ndpi",
    #     "H24-3236;S15;MSKZ - 2024-05-20 10.54.25.ndpi",
    #     "H24-3243;S10;MSK2 - 2024-05-20 10.59.35.ndpi",
    #     "H24-3243;S10;MSK2 - 2024-05-20 11.04.31.ndpi",
    #     "H24-3243;S10;MSK2 - 2024-05-20 11.09.19.ndpi",
    #     "H24-3265;S11;MSKX - 2024-05-20 11.14.22.ndpi",
    #     "H24-3265;S11;MSKX - 2024-05-20 11.19.19.ndpi",
    #     "H24-3265;S11;MSKX - 2024-05-20 11.24.40.ndpi",
    #     "H24-3268;S9;MSK7 - 2024-05-20 09.43.27.ndpi",
    #     "H24-3268;S9;MSK7 - 2024-05-20 09.48.34.ndpi",
    #     "H24-3268;S9;MSK7 - 2024-05-20 13.44.30.ndpi",
    #     "H24-3273;S13;MSKY - 2024-05-20 09.53.30.ndpi",
    #     "H24-3273;S13;MSKY - 2024-05-20 09.58.23.ndpi",
    #     "H24-3273;S13;MSKY - 2024-05-20 14.19.27.ndpi",
    #     "H24-3273;S13;MSKY - 2024-05-20 14.59.21.ndpi",
    #     "H24-3280;S17;MSK0 - 2024-05-20 10.29.07.ndpi",
    #     "H24-3280;S17;MSK0 - 2024-05-20 10.34.22.ndpi",
    #     "H24-3280;S18;MSKA - 2024-05-20 10.08.32.ndpi",
    #     "H24-3280;S18;MSKA - 2024-05-20 10.13.45.ndpi",
    #     "H24-3280;S19;MSKB - 2024-05-20 10.03.31.ndpi",
    #     "H24-3280;S19;MSKB - 2024-05-20 11.44.56.ndpi",
    #     "H24-3280;S20;MSKD - 2024-05-20 10.18.44.ndpi",
    #     "H24-3280;S20;MSKD - 2024-05-20 10.23.58.ndpi",
    #     "H24-3359;S13;MSK3 - 2024-05-20 09.23.50.ndpi",
    #     "H24-3359;S13;MSK3 - 2024-05-20 10.39.20.ndpi",
    #     "H24-3359;S13;MSK3 - 2024-05-20 11.49.50.ndpi",
    #     "H24-3436;S13;MSKZ - 2024-05-20 09.28.40.ndpi",
    #     "H24-3436;S13;MSKZ - 2024-05-20 09.33.42.ndpi",
    #     "H24-3436;S13;MSKZ - 2024-05-20 09.38.37.ndpi",
    #     "H24-3456;S17;MSK5 - 2024-05-20 12.29.35.ndpi",
    #     "H24-3456;S17;MSK5 - 2024-05-20 12.34.34.ndpi",
    #     "H24-3456;S18;MSK6 - 2024-05-20 12.04.54.ndpi",
    #     "H24-3456;S18;MSK6 - 2024-05-20 12.19.45.ndpi",
    #     "H24-3456;S19;MSK7 - 2024-05-20 12.24.40.ndpi",
    #     "H24-3456;S19;MSK7 - 2024-05-20 20.33.57.ndpi",
    #     "H24-3456;S20;MSK9 - 2024-05-20 12.09.50.ndpi",
    #     "H24-3456;S20;MSK9 - 2024-05-20 12.14.45.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 11.54.45.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 12.00.04.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 12.39.28.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 12.44.44.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 12.49.46.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 13.39.28.ndpi",
    #     "H24-3462;S13;MSKY - 2024-05-20 14.51.58.ndpi",
    #     "H24-3462;S14;MSKZ - 2024-07-10 21.43.49.ndpi",
    #     "H24-3483;S14;MSK2 - 2024-05-20 11.34.55.ndpi",
    #     "H24-3483;S14;MSK2 - 2024-05-20 11.40.00.ndpi",
    #     "H24-3534;S17;MSK2 - 2024-05-20 18.26.41.ndpi",
    #     "H24-3534;S17;MSK2 - 2024-05-20 18.31.34.ndpi",
    #     "H24-3534;S18;MSK3 - 2024-05-20 18.46.29.ndpi",
    #     "H24-3534;S18;MSK3 - 2024-05-20 18.51.35.ndpi",
    #     "H24-3534;S19;MSK4 - 2024-05-20 18.36.29.ndpi",
    #     "H24-3534;S19;MSK4 - 2024-05-20 18.56.43.ndpi",
    #     "H24-3534;S19;MSK4 - 2024-05-20 23.33.17.ndpi",
    #     "H24-3534;S20;MSK6 - 2024-05-20 18.41.36.ndpi",
    #     "H24-3534;S20;MSK6 - 2024-05-20 19.01.37.ndpi",
    #     "H24-3562;S15;MSK1 - 2024-05-20 20.38.51.ndpi",
    #     "H24-3562;S15;MSK1 - 2024-05-20 20.44.01.ndpi",
    #     "H24-3583;S13;MSK2 - 2024-05-20 21.40.30.ndpi",
    #     "H24-3583;S13;MSK2 - 2024-05-20 21.45.45.ndpi",
    #     "H24-3583;S13;MSK2 - 2024-05-20 21.50.36.ndpi",
    #     "H24-3599;S17;MSKC - 2024-05-20 22.05.40.ndpi",
    #     "H24-3599;S17;MSKC - 2024-05-20 22.15.49.ndpi",
    #     "H24-3599;S18;MSKD - 2024-05-20 19.53.23.ndpi",
    #     "H24-3599;S18;MSKD - 2024-05-20 22.25.56.ndpi",
    #     "H24-3599;S19;MSKE - 2024-05-20 21.55.38.ndpi",
    #     "H24-3599;S19;MSKE - 2024-05-20 22.00.37.ndpi",
    #     "H24-3599;S20;MSKG - 2024-05-20 22.10.50.ndpi",
    #     "H24-3599;S20;MSKG - 2024-05-20 22.20.44.ndpi",
    #     "H24-3604;S13;MSK6 - 2024-05-20 19.38.28.ndpi",
    #     "H24-3604;S13;MSK6 - 2024-05-20 19.43.31.ndpi",
    #     "H24-3604;S13;MSK6 - 2024-05-20 19.48.24.ndpi",
    #     "H24-3625;S13;MSKZ - 2024-05-20 19.11.37.ndpi",
    #     "H24-3625;S13;MSKZ - 2024-05-20 19.17.23.ndpi",
    #     "H24-3625;S13;MSKZ - 2024-05-20 19.22.45.ndpi",
    #     "H24-3625;S13;MSKZ - 2024-05-20 19.28.14.ndpi",
    #     "H24-3625;S13;MSKZ - 2024-05-20 19.33.33.ndpi",
    #     "H24-3630;S11;MSK3 - 2024-05-20 19.06.38.ndpi",
    #     "H24-3630;S11;MSK3 - 2024-05-20 21.30.38.ndpi",
    #     "H24-3630;S11;MSK3 - 2024-05-20 21.35.38.ndpi",
    #     "H24-3660;S10;MSKE - 2024-05-20 21.20.51.ndpi",
    #     "H24-3660;S10;MSKE - 2024-05-20 21.25.45.ndpi",
    #     "H24-3673;S10;MSK9 - 2024-05-20 21.04.59.ndpi",
    #     "H24-3673;S10;MSK9 - 2024-05-20 21.10.38.ndpi",
    #     "H24-3673;S10;MSK9 - 2024-05-20 21.15.34.ndpi",
    #     "H24-3691;S12;MSK1 - 2024-05-20 20.49.08.ndpi",
    #     "H24-3691;S12;MSK1 - 2024-05-20 20.54.03.ndpi",
    #     "H24-3691;S12;MSK1 - 2024-05-20 20.59.52.ndpi",
    #     "H24-3707;S13;MSK0 - 2024-05-20 19.58.32.ndpi",
    #     "H24-3707;S13;MSK0 - 2024-05-20 20.03.31.ndpi",
    #     "H24-3707;S13;MSK0 - 2024-05-20 20.08.35.ndpi",
    #     "H24-3710;S10;MSKA - 2024-05-20 20.13.43.ndpi",
    #     "H24-3710;S10;MSKA - 2024-05-20 20.18.39.ndpi",
    #     "H24-375;S12;MSKX - 2024-05-13 11.17.54.ndpi",
    #     "H24-375;S12;MSKX - 2024-05-13 12.03.06.ndpi",
    #     "H24-375;S12;MSKX - 2024-05-13 12.08.08.ndpi",
    #     "H24-375;S13;MSKY - 2024-05-13 11.12.57.ndpi",
    #     "H24-3785;S11;MSK4 - 2024-05-20 20.23.51.ndpi",
    #     "H24-3785;S11;MSK4 - 2024-05-20 20.28.50.ndpi",
    #     "H24-513;S15;MSKU - 2024-05-13 11.23.08.ndpi",
    #     "H24-513;S15;MSKU - 2024-05-13 13.36.05.ndpi",
    #     "H24-513;S15;MSKU - 2024-05-13 13.40.59.ndpi",
    #     "H24-513;S16;MSKV - 2024-05-13 13.25.37.ndpi",
    #     "H24-513;S16;MSKV - 2024-05-13 13.30.48.ndpi",
    #     "H24-53;S14;MSKS - 2024-05-13 11.33.15.ndpi",
    #     "H24-53;S14;MSKS - 2024-05-13 11.38.07.ndpi",
    #     "H24-53;S14;MSKS - 2024-05-13 11.43.06.ndpi",
    #     "H24-53;S14;MSKS - 2024-06-11 18.45.11.ndpi",
    #     "H24-53;S14;MSKS - 2024-06-11 18.50.02.ndpi",
    #     "H24-53;S14;MSKS - 2024-06-25 11.14.00.ndpi",
    #     "H24-53;S16;MSKU - 2024-05-13 11.28.01.ndpi",
    #     "H24-53;S16;MSKU - 2024-06-11 18.39.53.ndpi",
    #     "H24-736;S11;MSKX - 2024-05-13 11.08.03.ndpi",
    #     "H24-737;S11;MSKY - 2024-05-13 12.29.33.ndpi",
    #     "H24-737;S11;MSKY - 2024-05-13 12.34.37.ndpi",
    #     "H24-737;S12;MSKZ - 2024-05-13 11.02.53.ndpi",
    # ]

    # for test_slide in tqdm(test_slides, desc="Deleting slides to tmp_slide_dir"):
    #     # if the test slide already exist in the tmp_slide_dir, delete it
    #     if os.path.exists(f"/media/hdd3/neo/tmp_slide_dir/{test_slide}"):
    #         delete_slide_from_tmp(test_slide)

    # test_slides = test_slides[:50]

    # def test_wsi_name_filter_func(wsi_name):
    #     return wsi_name in test_slides

    # def identity_filter(pipeline_history_df):
    #     return pipeline_history_df

    # # first delete the folder /media/hdd3/neo/dzsave_dir
    # print("Reinitializing dzsave_dir and results_dir")
    # os.system("rm -r /media/hdd3/neo/dzsave_dir")
    # initialize_dzsave_dir()
    # delete_results_from_note(
    #     note="Testing concurrent processing", ask_for_confirmation=False
    # )
    # delete_results_from_note(
    #     note="Testing serial processing", ask_for_confirmation=False
    # )

    # print("Starting concurrent processing")
    # start_time = time.time()
    # main_concurrent_processing(
    #     wsi_name_filter_func=test_wsi_name_filter_func,
    #     processing_filter_func=identity_filter,
    #     num_rsync_workers=4,
    #     note="Testing concurrent processing",
    #     delete_slide=True,
    # )

    # concurrent_processing_time = time.time() - start_time
    # print("Finished concurrent processing")

    # # # first delete the folder /media/hdd3/neo/dzsave_dir
    # # print("Reinitializing dzsave_dir and results_dir")
    # # os.system("rm -r /media/hdd3/neo/dzsave_dir")
    # # initialize_dzsave_dir()
    # # delete_results_from_note(
    # #     note="Testing concurrent processing", ask_for_confirmation=False
    # # )
    # # delete_results_from_note(
    # #     note="Testing serial processing", ask_for_confirmation=False
    # # )

    # # print("Starting serial processing")
    # # start_time = time.time()
    # # main_serial_bma_processing(
    # #     wsi_name_filter_func=test_wsi_name_filter_func,
    # #     processing_filter_func=identity_filter,
    # #     note="Testing concurrent processing",
    # #     delete_slide=True,
    # # )
    # # print("Finished serial processing")

    # # serial_processing_time = time.time() - start_time

    # print(f"Concurrent processing took {concurrent_processing_time} seconds")
