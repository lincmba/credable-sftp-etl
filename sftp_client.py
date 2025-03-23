import os
import paramiko

class SFTPClient:
    def __init__(self):
        """Initialize the SFTP client with environment variables."""
        self.hostname = os.getenv("SFTP_HOST")
        self.port = int(os.getenv("SFTP_PORT", 22))  # Default port 22
        self.username = os.getenv("SFTP_USER")
        self.password = os.getenv("SFTP_PASS")
        self.private_key_path = os.getenv("SFTP_KEY")  # Optional SSH key

        # Directories from environment variables
        self.remote_directory = os.getenv("SFTP_REMOTE_DIR", "/remote/path/")
        self.local_directory = os.getenv("SFTP_LOCAL_DIR", "./downloaded_files/")

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def connect(self):
        """Establish an SFTP connection."""
        try:
            if self.private_key_path:
                key = paramiko.RSAKey(filename=self.private_key_path)
                self.ssh_client.connect(self.hostname, self.port, self.username, pkey=key)
            else:
                self.ssh_client.connect(self.hostname, self.port, self.username, self.password)

            self.sftp = self.ssh_client.open_sftp()
            print("SFTP connection established.")

        except Exception as e:
            print(f"Error connecting to SFTP: {e}")
            self.ssh_client = None
            self.sftp = None

    def upload_file(self, local_path, remote_path):
        """Upload a file to the SFTP server."""
        if self.sftp:
            self.sftp.put(local_path, remote_path)
            print(f"Uploaded {local_path} to {remote_path}")

    def download_file(self, remote_path, local_path):
        """Download a file from the SFTP server."""
        if self.sftp:
            self.sftp.get(remote_path, local_path)
            print(f"Downloaded {remote_path} to {local_path}")

    def list_files(self):
        """List files in the remote directory."""
        if self.sftp:
            files = self.sftp.listdir(self.remote_directory)
            print("Remote files:", files)
            return files

    def download_directory(self, extensions=(".csv", ".json")):
        """Download all files with specific extensions from the remote directory."""
        if not self.sftp:
            print("SFTP connection not established.")
            return

        try:
            # Ensure the local directory exists
            os.makedirs(self.local_directory, exist_ok=True)

            # List remote files
            remote_files = self.sftp.listdir(self.remote_directory)

            # Filter files by the given extensions
            filtered_files = [f for f in remote_files if f.endswith(extensions)]

            if not filtered_files:
                print(f"No files with extensions {extensions} found in {self.remote_directory}.")
                return

            # Download each matching file
            for file in filtered_files:
                remote_file_path = f"{self.remote_directory}/{file}"
                local_file_path = os.path.join(self.local_directory, file)
                self.sftp.get(remote_file_path, local_file_path)
                print(f"Downloaded: {remote_file_path} -> {local_file_path}")

        except Exception as e:
            print(f"Error downloading files: {e}")

    def close(self):
        """Close the SFTP connection."""
        if self.sftp:
            self.sftp.close()
        if self.ssh_client:
            self.ssh_client.close()
        print("SFTP connection closed.")
