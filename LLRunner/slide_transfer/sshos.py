import paramiko
import subprocess
import time
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
            f"if [ -f \"{remote_path}\" ]; then echo 'True'; else echo 'False'; fi"
        )
        return stdout.read().decode().strip() == "True"

    def isdir(self, remote_path):
        stdin, stdout, stderr = self.client.exec_command(
            f"if [ -d \"{remote_path}\" ]; then echo 'True'; else echo 'False'; fi"
        )

        return stdout.read().decode().strip() == "True"

    def rsync_file(self, remote_path, local_dir, retries=5, backoff_factor=1.5):
        """Rsync a single file from the remote server to a local directory with retry logic."""
        remote_file = f"{self.username}@{self.hostname}:{remote_path}"
        cmd = ["rsync", "-avz", "-e", "ssh", remote_file, local_dir]
        attempt = 0

        while attempt < retries:
            try:
                subprocess.run(cmd, check=True)
                print("Rsync successful.")
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


# Example usage:
# ssh_os = SSHOS(hostname="172.28.164.166", username="greg")
# ssh_os.connect()
# files = ssh_os.listdir("/pesgisipth/NDPI")
# print(files)
# is_file = ssh_os.isfile("/pesgisipth/NDPI/somefile.ndpi")
# print(is_file)
# ssh_os.disconnect()
