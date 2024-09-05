import paramiko
import subprocess
import time
import stat
import pandas as pd
import os
from time import sleep
from LLRunner.config import (
    slide_source_hostname,
    slide_source_username,
    available_machines,
    pipeline_run_history_path,
    dzsave_metadata_path,
)


def sftp_walk(sftp, remotepath):
    """Walks a remote directory tree using SFTP."""

    remotepath = str(remotepath)
    files = []
    directories = []
    for item in sftp.listdir_attr(remotepath):
        mode = item.st_mode
        if stat.S_ISDIR(mode):
            directories.append(item.filename)
        else:
            files.append(item.filename)

    yield remotepath, directories, files

    for directory in directories:
        new_path = os.path.join(remotepath, directory)
        for x in sftp_walk(sftp, new_path):
            yield x


class SSHOS:
    def __init__(
        self,
        hostname=slide_source_hostname,
        username=slide_source_username,
        key_filename=None,
    ):
        self.hostname = hostname
        self.username = username
        self.key_filename = key_filename
        self.client = None
        self.sftp = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.disconnect()

    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if self.key_filename:
            self.client.connect(
                self.hostname, username=self.username, key_filename=self.key_filename
            )
        else:
            self.client.connect(self.hostname, username=self.username)
        self.sftp = self.client.open_sftp()

    def disconnect(self):
        if self.sftp:
            self.sftp.close()
        if self.client:
            self.client.close()

    def listdir(self, remote_path):

        # make sure remote_path is a string
        remote_path = str(remote_path)
        try:
            return self.sftp.listdir(remote_path)
        except IOError as e:
            print(f"Error reading remote directory: {e}")
            return []

    def isfile(self, remote_path):

        # make sure remote_path is a string
        remote_path = str(remote_path)
        try:
            return stat.S_ISREG(self.sftp.stat(remote_path).st_mode)
        except IOError:
            return False

    def isdir(self, remote_path):

        # make sure remote_path is a string
        remote_path = str(remote_path)
        try:
            return stat.S_ISDIR(self.sftp.stat(remote_path).st_mode)
        except IOError:
            return False

    def rsync_file(self, remote_path, local_dir, retries=5, backoff_factor=1.5):
        """Rsync a single file from the remote server to a local directory with retry logic."""
        remote_file = f"{self.username}@{self.hostname}:{remote_path}"
        cmd = ["rsync", "-avz", "-e", "ssh", remote_file, local_dir]
        attempt = 0

        while attempt < retries:
            try:
                subprocess.run(cmd, check=True)
                print("Rsync successful.")
                # return the number of attempts it took to succeed
                return attempt + 1
                break
            except subprocess.CalledProcessError as e:
                attempt += 1
                if attempt < retries:
                    sleep_time = backoff_factor**attempt
                    print(
                        f"Rsync failed (attempt {attempt}/{retries}). Retrying in {sleep_time} seconds..."
                    )
                    time.sleep(sleep_time)
                else:
                    print(f"Rsync failed after {retries} attempts.")
                    raise e

    def rsync_dir(self, remote_dir, local_dir):
        """Rsync an entire directory from the remote server to a local directory."""
        remote_directory = f"{self.username}@{self.hostname}:{remote_dir}/"
        cmd = ["rsync", "-avz", "-e", "ssh", remote_directory, local_dir]
        subprocess.run(cmd, check=True)

    def get_csv_as_df(self, remote_path):
        """Get a remote CSV file as a pandas DataFrame."""
        stdin, stdout, stderr = self.client.exec_command(f"cat {remote_path}")
        return pd.read_csv(stdout)


# Example usage:
# ssh_os = SSHOS(hostname="172.28.164.166", username="greg")
# ssh_os.connect()
# files = ssh_os.listdir("/pesgisipth/NDPI")
# print(files)
# is_file = ssh_os.isfile("/pesgisipth/NDPI/somefile.ndpi")
# print(is_file)
# ssh_os.disconnect()


def get_pipeline_run_history_df(machine):
    # first check if machine is in available machines
    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    # get the ssh config for the machine
    hostname = available_machines[machine]["hostname"]
    username = available_machines[machine]["username"]

    # create an SSHOS object
    with SSHOS(hostname=hostname, username=username) as sshos:
        # get the pipeline run history as a DataFrame
        df = sshos.get_csv_as_df(remote_path=pipeline_run_history_path)

    return df


def get_dzsave_metadata_df(machine):
    # first check if machine is in available machines
    assert (
        machine in available_machines
    ), f"Machine {machine} not in available machines in list {available_machines}."

    # get the ssh config for the machine
    hostname = available_machines[machine]["hostname"]
    username = available_machines[machine]["username"]

    # create an SSHOS object
    with SSHOS(hostname=hostname, username=username) as sshos:
        # get the dzsave metadata as a DataFrame
        df = sshos.get_csv_as_df(remote_path=dzsave_metadata_path)

    return df
