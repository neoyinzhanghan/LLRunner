import os
import pandas as pd
import time
import paramiko
from LLRunner.config import (
    results_dir,
    available_machines,
    ssh_config,
    slide_metadata_path,
    bma_diff_metadata_path,
    pipeline_run_history_path,
)
from LLRunner.read.BMAResult import BMAResultSSH
from LLRunner.slide_transfer.sshos import SSHOS
from tqdm import tqdm


def get_slide_metadata_df(machine):
    """Get the slide metadata dataframe for the specified machine."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    with SSHOS(hostname=hostname, username=username) as sshos:
        df = sshos.get_csv_as_df(remote_path=slide_metadata_path)

    return df


def get_bma_diff_metadata_df(machine):
    """Get the BMA-diff metadata dataframe for the specified machine."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    with SSHOS(hostname=hostname, username=username) as sshos:
        df = sshos.get_csv_as_df(remote_path=bma_diff_metadata_path)

    return df


def get_pipeline_run_history_df(machine):
    """Get the pipeline run history dataframe for the specified machine."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    with SSHOS(hostname=hostname, username=username) as sshos:
        df = sshos.get_csv_as_df(remote_path=pipeline_run_history_path)

    return df


def all_result_dirs(machine):
    """Get all the result directories for the specified machine."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    with SSHOS(hostname=hostname, username=username) as sshos:
        result_dirs = sshos.listdir(results_dir)

        # only keep the ones that are actually directories
        result_dirs = [
            result_dir
            for result_dir in result_dirs
            if sshos.isdir(os.path.join(results_dir, result_dir))
        ]

    return result_dirs


def compile_results():

    df_dict = {
        "machine": [],
        "hostname": [],
        "username": [],
        "remote_result_dir": [],
        "wsi_name": [],
        "pipeline": [],
        "Dx": [],
        "sub_Dx": [],
        "datetime_processed": [],
        "note": [],
    }

    for machine in available_machines:
        remote_result_dirs = all_result_dirs(machine)

        slide_metadata_df = get_slide_metadata_df(machine)
        pipeline_run_history_df = get_pipeline_run_history_df(machine)

        for remote_result_dir in tqdm(
            remote_result_dirs, desc=f"Compiling Result Dirs on Machine: {machine}"
        ):

            try:
                pipeline = remote_result_dir.split("_")[0]
                datetime_processed = remote_result_dir.split("_")[1]
            except Exception as e:
                print(
                    f"Error: {e}, occurred for remote_result_dir: {remote_result_dir}. This is most likely due to continually running data extraction pipeline processes."
                )
                # # raise e while printing the remote_result_dir
                # e.args = (
                #     f"Error: {e}, occurred for remote_result_dir: {remote_result_dir}",
                # )

                # continue to the next remote_result_dir
                continue

            # look for the row in pipeline_run_history_df with the same pipeline and datetime_processed
            pipeline_run_history_row = pipeline_run_history_df[
                (pipeline_run_history_df["pipeline"] == pipeline)
                & (pipeline_run_history_df["datetime_processed"] == datetime_processed)
            ]

            assert (
                len(pipeline_run_history_row) >= 1
            ), f"Error: No row found in pipeline_run_history_df with pipeline: {pipeline}, datetime_processed: {datetime_processed}."
            assert (
                len(pipeline_run_history_row) == 1
            ), f"Error: More than one row found in pipeline_run_history_df with pipeline: {pipeline}, datetime_processed: {datetime_processed}."

            wsi_name = pipeline_run_history_row["wsi_name"].iloc[0]
            has_error = pipeline_run_history_row["error"].iloc[0]
            note = pipeline_run_history_row["note"].iloc[0]

            if not has_error:

                # look for the row in slide_metadata_df with the same wsi_name
                slide_metadata_row = slide_metadata_df[
                    slide_metadata_df["wsi_name"] == wsi_name
                ]

                assert (
                    len(slide_metadata_row) >= 1
                ), f"Error: No row found in slide_metadata_df with wsi_name: {wsi_name}."
                assert (
                    len(slide_metadata_row) == 1
                ), f"Error: More than one row found in slide_metadata_df with wsi_name: {wsi_name}."

                dx = slide_metadata_row["Dx"].iloc[0]
                sub_dx = slide_metadata_row["sub_Dx"].iloc[0]

                df_dict["machine"].append(machine)
                df_dict["hostname"].append(ssh_config[machine]["hostname"])
                df_dict["username"].append(ssh_config[machine]["username"])
                df_dict["remote_result_dir"].append(remote_result_dir)
                df_dict["wsi_name"].append(wsi_name)
                df_dict["pipeline"].append(pipeline)
                df_dict["Dx"].append(dx)
                df_dict["sub_Dx"].append(sub_dx)
                df_dict["datetime_processed"].append(datetime_processed)
                df_dict["note"].append(note)

            # start_time = time.time()
            # try:
            #     bma_result = BMAResultSSH(
            #         hostname=ssh_config[machine]["hostname"],
            #         username=ssh_config[machine]["username"],
            #         remote_result_dir=remote_result_dir,
            #     )
            # except Exception as e:
            #     print(f"Error: {e}")
            #     print(f"Machine: {machine}, remote_result_dir: {remote_result_dir}")

            #     raise e

            # if not bma_result.has_error():
            #     df_dict["machine"].append(machine)
            #     df_dict["hostname"].append(ssh_config[machine]["hostname"])
            #     df_dict["username"].append(ssh_config[machine]["username"])
            #     df_dict["remote_result_dir"].append(remote_result_dir)
            #     df_dict["wsi_name"].append(bma_result.get_wsi_name())
            #     df_dict["pipeline"].append(bma_result.get_pipeline())
            #     df_dict["Dx"].append(bma_result.get_Dx_and_sub_Dx()[0])
            #     df_dict["sub_Dx"].append(bma_result.get_Dx_and_sub_Dx()[1])
            #     df_dict["datetime_processed"].append(
            #         bma_result.get_datetime_processed()
            #     )
            #     df_dict["note"].append(bma_result.get_note())

            # time_taken = time.time() - start_time

            # print(f"Time taken for {remote_result_dir}: {time_taken:.2f} seconds.")

    tmp_df = pd.DataFrame(df_dict)

    # tmp_df['datetime_processed'] = pd.to_datetime(tmp_df['datetime_processed']) # DEPRECATED - not needed

    return tmp_df
