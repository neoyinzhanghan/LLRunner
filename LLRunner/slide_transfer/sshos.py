import paramiko
import subprocess
import os
from LLRunner.config import slide_source_hostname, slide_source_username


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

    def disconnect(self):
        if self.client:
            self.client.close()

    def listdir(self, remote_path):
        stdin, stdout, stderr = self.client.exec_command(f"ls -p {remote_path}")
        return stdout.read().decode().splitlines()

    def isfile(self, remote_path):
        stdin, stdout, stderr = self.client.exec_command(
            f"if [ -f {remote_path} ]; then echo 'True'; else echo 'False'; fi"
        )
        return stdout.read().decode().strip() == "True"

    def isdir(self, remote_path):
        stdin, stdout, stderr = self.client.exec_command(
            f"if [ -d {remote_path} ]; then echo 'True'; else echo 'False'; fi"
        )
        return stdout.read().decode().strip() == "True"

    def rsync_file(self, remote_path, local_dir):
        """Rsync a single file from the remote server to a local directory."""
        # if the file already exists in the local directory, delete it first then rsync
        filename = os.path.basename(remote_path)
        local_file = os.path.join(local_dir, filename)

        if os.path.exists(local_file):
            os.remove(local_file)

        remote_file = f"{self.username}@{self.hostname}:{remote_path}"
        cmd = ["rsync", "-avz", "-e", "ssh", remote_file, local_dir]
        subprocess.run(cmd, check=True)

    def rsync_dir(self, remote_dir, local_dir):
        """Rsync an entire directory from the remote server to a local directory."""
        remote_directory = f"{self.username}@{self.hostname}:{remote_dir}/"
        cmd = ["rsync", "-avz", "-e", "ssh", remote_directory, local_dir]
        subprocess.run(cmd, check=True)


# Example usage:
# ssh_os = SSHOS(hostname="172.28.164.166", username="greg")
# ssh_os.connect()
# files = ssh_os.listdir("/pesgisipth/NDPI")
# print(files)
# is_file = ssh_os.isfile("/pesgisipth/NDPI/somefile.ndpi")
# print(is_file)
# ssh_os.disconnect()
