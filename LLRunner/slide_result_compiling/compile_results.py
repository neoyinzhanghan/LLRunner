# import os
# import pandas as pd
# from LLRunner.config import results_dir, available_machines, ssh_config
# from LLRunner.read.BMAResult import BMAResultSSH
# from LLRunner.slide_transfer.sshos import SSHOS
# from tqdm import tqdm


# def all_result_dirs(machine):
#     """Get all the result directories for the specified machine."""

#     assert (
#         machine in available_machines
#     ), f"Machine {machine} not in available machines in list {available_machines}."

#     username = ssh_config[machine]["username"]
#     hostname = ssh_config[machine]["hostname"]

#     with SSHOS(hostname=hostname, username=username) as sshos:
#         result_dirs = sshos.listdir(results_dir)

#     return result_dirs


# def compile_results():

#     df_dict = {
#         "machine": [],
#         "hostname": [],
#         "username": [],
#         "remote_result_dir": [],
#         "wsi_name": [],
#         "pipeline": [],
#         "Dx": [],
#         "sub_Dx": [],
#         "datetime_processed": [],
#         "note": [],
#     }

#     for machine in available_machines:
#         remote_result_dirs = all_result_dirs(machine)

#         for remote_result_dir in tqdm(
#             remote_result_dirs, desc=f"Compiling Result Dirs on Machine: {machine}"
#         ):

#             remote_result_dir = os.path.join(results_dir, remote_result_dir)
#             try:
#                 bma_result = BMAResultSSH(
#                     hostname=ssh_config[machine]["hostname"],
#                     username=ssh_config[machine]["username"],
#                     remote_result_dir=remote_result_dir,
#                 )
#             except Exception as e:
#                 print(f"Error: {e}")
#                 print(f"Machine: {machine}, remote_result_dir: {remote_result_dir}")

#                 raise e

#             if not bma_result.has_error():
#                 df_dict["machine"].append(machine)
#                 df_dict["hostname"].append(ssh_config[machine]["hostname"])
#                 df_dict["username"].append(ssh_config[machine]["username"])
#                 df_dict["remote_result_dir"].append(remote_result_dir)
#                 df_dict["wsi_name"].append(bma_result.get_wsi_name())
#                 df_dict["pipeline"].append(bma_result.get_pipeline())
#                 df_dict["Dx"].append(bma_result.get_Dx_and_sub_Dx()[0])
#                 df_dict["sub_Dx"].append(bma_result.get_Dx_and_sub_Dx()[1])
#                 df_dict["datetime_processed"].append(
#                     bma_result.get_datetime_processed()
#                 )
#                 df_dict["note"].append(bma_result.get_note())

#     df = pd.DataFrame(df_dict)

#     return df

import os
import pandas as pd
import ray
from LLRunner.config import results_dir, available_machines, ssh_config
from LLRunner.read.BMAResult import BMAResultSSH
from LLRunner.slide_transfer.sshos import SSHOS
from tqdm import tqdm

# Initialize Ray
ray.init(ignore_reinit_error=True)

@ray.remote
def process_single_result(machine, remote_result_dir):
    """Process a single result directory for a given machine."""
    try:
        remote_result_dir = os.path.join(results_dir, remote_result_dir)
        bma_result = BMAResultSSH(
            hostname=ssh_config[machine]["hostname"],
            username=ssh_config[machine]["username"],
            remote_result_dir=remote_result_dir,
        )
        
        if not bma_result.has_error():
            return {
                "machine": machine,
                "hostname": ssh_config[machine]["hostname"],
                "username": ssh_config[machine]["username"],
                "remote_result_dir": remote_result_dir,
                "wsi_name": bma_result.get_wsi_name(),
                "pipeline": bma_result.get_pipeline(),
                "Dx": bma_result.get_Dx_and_sub_Dx()[0],
                "sub_Dx": bma_result.get_Dx_and_sub_Dx()[1],
                "datetime_processed": bma_result.get_datetime_processed(),
                "note": bma_result.get_note(),
            }
    except Exception as e:
        print(f"Error: {e}")
        print(f"Machine: {machine}, remote_result_dir: {remote_result_dir}")
    return None

def all_result_dirs(machine):
    """Get all the result directories for the specified machine."""
    assert machine in available_machines, f"Machine {machine} not in available machines: {available_machines}."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    with SSHOS(hostname=hostname, username=username) as sshos:
        result_dirs = sshos.listdir(results_dir)

    return result_dirs

def compile_results():
    result_futures = []
    
    # Create Ray tasks for each result directory on each machine
    for machine in available_machines:
        remote_result_dirs = all_result_dirs(machine)
        for remote_result_dir in remote_result_dirs:
            result_futures.append(process_single_result.remote(machine, remote_result_dir))
    
    # Collect results with tqdm progress bar
    results = []
    for result in tqdm(ray.get(result_futures), desc="Compiling Results", total=len(result_futures)):
        if result is not None:
            results.append(result)
    
    # Convert results to a DataFrame
    df = pd.DataFrame(results)

    return df

if __name__ == "__main__":
    df = compile_results()
    print(df.head())

    # Optionally, shut down Ray
    ray.shutdown()
