while True:

    import os
    import time
    import shutil
    import datetime
    import openslide
    import subprocess
    import pandas as pd
    from tqdm import tqdm
    from LLBMA.front_end.api import analyse_bma
    from LLRunner.slide_processing.dzsave_h5 import dzsave_h5
    from LLRunner.slide_processing.specimen_clf import (
        get_topview_bma_score,
        get_topview_pbs_score,
    )
    from intialize_csv_file import initialize_minimal_servcice_csv_file
    from sample_N_cells import sample_N_cells

    cutoffdatetime = "2024-12-08 00:00:00"
    enddatetime = None
    # convert the cutoff datetime to a datetime object
    cutoffdatetime = pd.to_datetime(cutoffdatetime, format="%Y-%m-%d %H:%M:%S")
    if enddatetime is not None:
        enddatetime = pd.to_datetime(enddatetime, format="%Y-%m-%d %H:%M:%S")
    headers = ["H24", "H25", "H26"]

    slide_source_dir = "/pesgisipth/NDPI"
    tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"
    LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
    dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
    metadata_path = "/media/hdd2/neo/same_day_processing_metadata.csv"
    topview_save_dir = "/media/hdd2/neo/tmp_slides_dir/topview"
    ssh_name = "glv3"
    remote_backup_dir = "/media/hdd2/neo"
    remote_dzsave_dir = "/media/hdd2/neo/SameDayDzsave"
    remote_LLBMA_results_dir = "/media/hdd2/neo/SameDayLLBMAResults"
    remote_topview_save_dir = "/media/hdd2/neo/tmp_slides_dir/topview"
    remote_tmp_slide_dir = "/media/hdd2/neo/tmp_slides_dir"

    if not os.path.exists(metadata_path):
        initialize_minimal_servcice_csv_file(metadata_path)

    metadata_df = pd.read_csv(metadata_path)

    # get the wsi_name column of the metadata_df as a list of strings
    wsi_names = metadata_df["wsi_name"].tolist()

    # get the list of all the .ndpi files in the slide_source_dir with name starting with something from the HEADERS
    all_slide_names = []
    for header in headers:
        slide_names = [
            slide_name
            for slide_name in os.listdir(slide_source_dir)
            if slide_name.startswith(header)
            and slide_name.endswith(".ndpi")
            and os.path.isfile(os.path.join(slide_source_dir, slide_name))
        ]

        all_slide_names.extend(slide_names)

    def get_slide_datetime(slide_name):
        try:
            name = slide_name.split(".ndpi")[0]
            datetime = name.split(" - ")[-1]

            # convert the datetime to a datetime object
            datetime = pd.to_datetime(datetime, format="%Y-%m-%d %H.%M.%S")
        except Exception as e:
            print(f"Error getting datetime for {slide_name}: {e}")
            raise e
        return datetime

    # get the list of all the slides that are newer than the CUTOFFDATETIME
    newer_slides = []

    for slide_name in all_slide_names:
        slide_datetime = get_slide_datetime(slide_name)
        if slide_datetime > cutoffdatetime and enddatetime is None:
            newer_slides.append(slide_name)
        elif slide_datetime > cutoffdatetime and slide_datetime < enddatetime:
            newer_slides.append(slide_name)

    print(f"Found a total of {len(newer_slides)} slides.")
    print(f"This many has already been processed: {len(wsi_names)}")

    # only keep the slides that have not been processed
    newer_slides_to_process = [
        slide_name for slide_name in newer_slides if slide_name not in wsi_names
    ]
    print(f"Found a total of {len(newer_slides_to_process)} slides to process.")

    def process_slide(slide_name, metadata_df):
        metadata_df = pd.read_csv(metadata_path)
        # first copy the slide to the tmp_slide_dir
        slide_path = os.path.join(slide_source_dir, slide_name)
        tmp_slide_path = os.path.join(tmp_slide_dir, slide_name)
        dzsave_h5_path = os.path.join(dzsave_dir, slide_name.replace(".ndpi", ".h5"))

        new_metadata_row_dict = {
            "wsi_name": slide_name,
            "result_dir_name": None,
            "dzsave_h5_path": dzsave_h5_path,
            "datetime_processed": None,
            "pipeline": None,
            "datetime_dzsaved": None,
            "slide_copy_error": None,
            "dzsave_error": None,
            "pipeline_error": None,
            "slide_copy_time": None,
            "dzsave_time": None,
            "pipeline_time": None,
            "bma_score": None,
            "pbs_score": None,
            "is_bma": None,
            "is_pbs": None,
        }

        new_metadata_row_dict["datetime_processed"] = datetime.datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        print(f"Copying slide from {slide_name} to {tmp_slide_path}")
        copy_start_time = time.time()
        try:
            shutil.copy(slide_path, tmp_slide_path)
            # print("already copied")
        except Exception as e:
            print(f"Error copying slide {slide_name}: {e}")
            new_metadata_row_dict["slide_copy_error"] = str(e)
        slide_copy_time = time.time() - copy_start_time
        new_metadata_row_dict["slide_copy_time"] = slide_copy_time
        print(f"Slide copy completed. Took {slide_copy_time} seconds.")

        print(f"Performing specimen classification on slide {slide_name}")
        try:
            wsi = openslide.OpenSlide(tmp_slide_path)
            # topview is the entire level 7 image
            topview = wsi.read_region((0, 0), 7, wsi.level_dimensions[7])
            # if RGBA then convert to RGB
            if topview.mode == "RGBA":
                topview = topview.convert("RGB")

            # save the topview image
            topview_path = os.path.join(
                topview_save_dir, slide_name.replace(".ndpi", ".jpg")
            )
            topview.save(topview_path)

            bma_score = get_topview_bma_score(topview)
            pbs_score = get_topview_pbs_score(topview)
            new_metadata_row_dict["bma_score"] = bma_score
            new_metadata_row_dict["pbs_score"] = pbs_score
            is_bma = bma_score >= 0.5
            is_pbs = pbs_score >= 0.5
            new_metadata_row_dict["is_bma"] = is_bma
            new_metadata_row_dict["is_pbs"] = is_pbs

            print(f"Specimen BMA classification score for {slide_name}: {bma_score}")
            print(f"Specimen PBS classification score for {slide_name}: {pbs_score}")

        except Exception as e:
            print(
                f"Error performing specimen classification on slide {slide_name}: {e}"
            )
            new_metadata_row_dict["pipeline_error"] = str(e)

            is_bma = False
            is_pbs = False
            new_metadata_row_dict["is_bma"] = is_bma
            new_metadata_row_dict["is_pbs"] = is_pbs

        if not is_bma:
            print(f"dzsaving slide {slide_name} to {dzsave_h5_path}")
            dzsave_start_time = time.time()
            try:
                dzsave_h5(
                    wsi_path=tmp_slide_path,
                    h5_path=dzsave_h5_path,
                    num_cpus=32,
                    tile_size=512,
                )
                dzsave_time = time.time() - dzsave_start_time
                new_metadata_row_dict["dzsave_time"] = dzsave_time
                new_metadata_row_dict["dzsave_h5_path"] = dzsave_h5_path
                new_metadata_row_dict["datetime_dzsaved"] = (
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
            except Exception as e:
                print(f"Error dzsaving slide {slide_name}: {e}")
                new_metadata_row_dict["dzsave_error"] = str(e)

        else:
            print(f"We will run the LLBMA pipeline on slide {slide_name}")
            new_metadata_row_dict["pipeline"] = "BMA-diff"

            pipeline_start_time = time.time()

            try:
                # Run the heme_analyze function
                save_path, has_error = analyse_bma(
                    slide_path=tmp_slide_path,
                    dump_dir=LLBMA_results_dir,
                    hoarding=True,
                    extra_hoarding=False,
                    continue_on_error=True,
                    do_extract_features=False,
                    check_specimen_clf=False,
                    pretiled_h5_path=None,
                )
                pipeline_time = time.time() - pipeline_start_time
                new_metadata_row_dict["pipeline_time"] = pipeline_time

                slide_name_no_ext = slide_name.split(".ndpi")[0]
                pipeline_slide_h5_path = os.path.join(
                    LLBMA_results_dir, save_path, "slide.h5"
                )

                # Moving the slide.h5 to the dzsave_dir
                print(
                    f"Moving slide.h5 from {pipeline_slide_h5_path} to {dzsave_h5_path}"
                )
                dzsave_start_time = time.time()
                shutil.move(pipeline_slide_h5_path, dzsave_h5_path)
                dzsave_time = time.time() - dzsave_start_time

                new_metadata_row_dict["result_dir_name"] = os.path.basename(save_path)
                if has_error:
                    # get the content of the save_path/error.txt file
                    with open(os.path.join(save_path, "error.txt"), "r") as f:
                        pipeline_error = f.read()
                    new_metadata_row_dict["pipeline_error"] = pipeline_error

                else:
                    sample_N_cells(os.path.join(LLBMA_results_dir, save_path), N=200)
                new_metadata_row_dict["dzsave_h5_path"] = dzsave_h5_path
                new_metadata_row_dict["dzsave_time"] = dzsave_time
                new_metadata_row_dict["datetime_dzsaved"] = (
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

            except Exception as e:
                print(f"Error running LLBMA pipeline on slide {slide_name}: {e}")
                new_metadata_row_dict["pipeline_error"] = str(e)

                pipeline_time = time.time() - pipeline_start_time
                new_metadata_row_dict["pipeline_time"] = pipeline_time

                print(
                    f"Due to pipeline error, we are dzsaving slide {slide_name} to {dzsave_h5_path}"
                )
                dzsave_start_time = time.time()
                try:
                    dzsave_h5(
                        wsi_path=tmp_slide_path,
                        h5_path=dzsave_h5_path,
                        num_cpus=32,
                        tile_size=512,
                    )
                    dzsave_time = time.time() - dzsave_start_time
                    new_metadata_row_dict["dzsave_time"] = dzsave_time
                    new_metadata_row_dict["dzsave_h5_path"] = dzsave_h5_path
                    new_metadata_row_dict["datetime_dzsaved"] = (
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )

                except Exception as e:
                    print(f"Error dzsaving slide {slide_name}: {e}")
                    new_metadata_row_dict["dzsave_error"] = str(e)

        print(new_metadata_row_dict)

        # concat the new metadata row to the metadata_df
        new_metadata_row_df = pd.DataFrame([new_metadata_row_dict])
        metadata_df = pd.concat([metadata_df, new_metadata_row_df], ignore_index=True)

        # save the metadata_df back to the metadata_path
        metadata_df.to_csv(metadata_path, index=False)

        print(f"Deleting slide {tmp_slide_path}")
        os.remove(tmp_slide_path)

    if len(newer_slides_to_process) == 0:
        print("No slides to process. Sleeping for 30s...")
        time.sleep(30)
        print("Service reiterating...")

    else:

        # find the slide in newer_slides that is the oldest
        # process the oldest slide first
        oldest_slide_to_process = newer_slides_to_process[0]
        for slide_name in newer_slides_to_process:
            slide_datetime = get_slide_datetime(slide_name)
            if slide_datetime < get_slide_datetime(oldest_slide_to_process):
                oldest_slide_to_process = slide_name

        print(f"Processing slide {oldest_slide_to_process}")
        process_slide(oldest_slide_to_process, metadata_df)

        print("Initiating output backup and moving to production server...")
        h5_name = oldest_slide_to_process.replace(".ndpi", ".h5")
        h5_path = os.path.join(dzsave_dir, h5_name)

        tmp_slide_path = os.path.join(tmp_slide_dir, oldest_slide_to_process)
        result_folder_path = os.path.join(
            LLBMA_results_dir, oldest_slide_to_process.split(".ndpi")[0]
        )
        error_result_folder_path = os.path.join(
            LLBMA_results_dir, "ERROR_" + oldest_slide_to_process.split(".ndpi")[0]
        )

        # if h5_path exists, rsync it to the remote location in the backgroud
        if os.path.exists(h5_path):
            print(f"Rsyncing {h5_path} to {remote_dzsave_dir}")
            command = [
                "rsync",
                "-a",
                h5_path,
                f"{ssh_name}:{remote_dzsave_dir}",
            ]
            subprocess.Popen(command)

        # if tmp_slide_path exists, rsync it to the remote location in the backgroud
        if os.path.exists(tmp_slide_path):
            print(f"Rsyncing {tmp_slide_path} to {remote_tmp_slide_dir}")
            command = [
                "rsync",
                "-a",
                tmp_slide_path,
                f"{ssh_name}:{remote_tmp_slide_dir}/{oldest_slide_to_process}",
            ]
            subprocess.Popen(command)

        # if result_folder_path exists, rsync it to the remote location in the backgroud
        if os.path.exists(result_folder_path):
            print(f"Rsyncing {result_folder_path} to {remote_LLBMA_results_dir}")
            command = [
                "rsync",
                "-a",
                result_folder_path + "/",
                f"{ssh_name}:{remote_LLBMA_results_dir}/{oldest_slide_to_process.split('.ndpi')[0]}",
            ]
            subprocess.Popen(command)

        # if error_result_folder_path exists, rsync it to the remote location in the backgroud
        if os.path.exists(error_result_folder_path):
            print(f"Rsyncing {error_result_folder_path} to {remote_LLBMA_results_dir}")
            command = [
                "rsync",
                "-a",
                error_result_folder_path + "/",
                f"{ssh_name}:{remote_LLBMA_results_dir}/ERROR_{oldest_slide_to_process.split('.ndpi')[0]}",
            ]
            subprocess.Popen(command)

        # if topview_path exists, rsync it to the remote location in the backgroud
        topview_path = os.path.join(
            tmp_slide_dir, "topview", oldest_slide_to_process.replace(".ndpi", ".jpg")
        )
        if os.path.exists(topview_path):
            print(f"Rsyncing {topview_path} to {remote_topview_save_dir}")
            command = [
                "rsync",
                "-avz",
                "--progress",
                topview_path,
                f"{ssh_name}:{remote_topview_save_dir}/{oldest_slide_to_process.replace('.ndpi', '.jpg')}",
            ]
            subprocess.Popen(command)

        sleep_num_seconds = 3
        print("Service complete.")
        print(f"Sleeping for {sleep_num_seconds} seconds...")
        time.sleep(sleep_num_seconds)  # currently set to 30 seconds
        print("Service reiterating...")
