import os
from data_cleaner import DataCleaner
from database_sink import PostgresStorage
from sftp_client import SFTPClient


def main():
    # Directories and database details from env variables
    data_directory = os.getenv("SFTP_LOCAL_DIR", "./downloaded_files/")

    # Initialize modules
    cleaner = DataCleaner(data_directory)
    storage = PostgresStorage()
    sftp_client = SFTPClient()
    sftp_client.connect()

    # Download all .csv and .json files from the remote directory to the local directory
    sftp_client.download_directory(extensions=(".csv", ".json"))

    # Close connection
    sftp_client.close()


    # clean and store data
    for file in os.listdir(data_directory):
        if file.endswith(".csv") or file.endswith(".json"):
            file_path = os.path.join(data_directory, file)
            print(f"Processing: {file_path}")

            df = cleaner.read_file(file_path)
            if df is not None:
                df = cleaner.clean_data(df)
                storage.store_data(df)

    # Close database connection
    storage.close()


if __name__ == "__main__":
    main()
