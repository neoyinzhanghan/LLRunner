import os
import pandas as pd
from LLRunner.config import results_dir, available_machines, ssh_config
from LLRunner.read.BMAResult import BMAResultSSH
from LLRunner.slide_transfer.sshos import SSHOS
from tqdm import tqdm


def all_result_dirs(machine):
    """Get all the result directories for the specified machine."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    with SSHOS(hostname=hostname, username=username) as sshos:
        result_dirs = sshos.listdir(results_dir)

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

        for remote_result_dir in tqdm(
            remote_result_dirs, desc=f"Compiling Result Dirs on Machine: {machine}"
        ):

            remote_result_dir = os.path.join(results_dir, remote_result_dir)
            try:
                bma_result = BMAResultSSH(
                    hostname=ssh_config[machine]["hostname"],
                    username=ssh_config[machine]["username"],
                    remote_result_dir=remote_result_dir,
                )
            except Exception as e:
                print(f"Error: {e}")
                print(f"Machine: {machine}, remote_result_dir: {remote_result_dir}")

                raise e

            if not bma_result.has_error():
                df_dict["machine"].append(machine)
                df_dict["hostname"].append(ssh_config[machine]["hostname"])
                df_dict["username"].append(ssh_config[machine]["username"])
                df_dict["remote_result_dir"].append(remote_result_dir)
                df_dict["wsi_name"].append(bma_result.get_wsi_name())
                df_dict["pipeline"].append(bma_result.get_pipeline())
                df_dict["Dx"].append(bma_result.get_Dx_and_sub_Dx()[0])
                df_dict["sub_Dx"].append(bma_result.get_Dx_and_sub_Dx()[1])
                df_dict["datetime_processed"].append(
                    bma_result.get_datetime_processed()
                )
                df_dict["note"].append(bma_result.get_note())

    df = pd.DataFrame(df_dict)

    return df
