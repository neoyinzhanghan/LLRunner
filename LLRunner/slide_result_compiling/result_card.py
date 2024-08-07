from LLRunner.read.BMAResult import BMAResult, BMAResultSSH
from LLRunner.config import available_machines, ssh_config


def get_mini_result_card(remote_result_dir, machine):
    """Get the mini result card for the BMA-diff pipeline."""

    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines."

    username = ssh_config[machine]["username"]
    hostname = ssh_config[machine]["hostname"]

    bma_result = BMAResultSSH(
        hostname=hostname,
        username=username,
        remote_result_dir=remote_result_dir,
        max_retries=5,  # Optional: set the max retries for rsync, defaults to 3
        backoff_factor=2,  # Optional: set the backoff factor for rsync, defaults to 2
    )

    confidence_heatmap = bma_result.get_confidence_heatmap()
