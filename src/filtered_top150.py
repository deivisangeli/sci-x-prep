import os
import dropbox
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Replace with your own access token
ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN")

# Initialize Dropbox client
dbx = dropbox.Dropbox(ACCESS_TOKEN)

# Function to download a single file from Dropbox to a local directory
def download_single_file_from_dropbox(dropbox_file_path, local_file_path):
    try:
        # Download the file from Dropbox
        print(f"Downloading {dropbox_file_path} to {local_file_path}")
        
        # Get the file content from Dropbox
        metadata, res = dbx.files_download(path=dropbox_file_path)

        # Ensure the local directory exists
        local_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # Write the file content to the local disk
        with open(local_file_path, "wb") as f:
            f.write(res.content)

        print(f"Downloaded {dropbox_file_path} to {local_file_path}")

    except Exception as e:
        print(f"Error downloading file: {e}")

# main
def get_filtered_top150academics():
    dropbox_file_path = '/Science Twitter/Data/Raw/openalex/people/top150filtered/all.xlsx'  # Replace with the actual Dropbox file path
    filename = dropbox_file_path.split('/')[-2]
    local_file_path = f'data/downloaded_files/{filename}.xlsx'  # Replace with the local path where you want to save the file
    download_single_file_from_dropbox(dropbox_file_path, local_file_path)

if __name__ == "__main__":
    get_filtered_top150academics()