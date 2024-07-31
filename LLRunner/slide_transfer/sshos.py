import paramiko
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

    def connect(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if self.key_filename:
            self.client.connect(
                self.hostname, username=self.username, key_filename=self.key_filename
            )
        else:
            self.client.connect(
                self.hostname, username=self.username
            )

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

    def remove(self, remote_path):
        self.client.exec_command(f"rm {remote_path}")

    def makedirs(self, remote_path):
        self.client.exec_command(f"mkdir -p {remote_path}")


# Example usage:
# ssh_os = SSHOS(hostname="172.28.164.166", username="greg")
# ssh_os.connect()
# files = ssh_os.listdir("/pesgisipth/NDPI")
# print(files)
# is_file = ssh_os.isfile("/pesgisipth/NDPI/somefile.ndpi")
# print(is_file)
# ssh_os.disconnect()
