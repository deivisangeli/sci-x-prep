import os
import dropbox
import pandas as pd  # For handling data in Excel files
from dotenv import load_dotenv
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Load environment variables from .env file
load_dotenv()

# Replace with your own access token
ACCESS_TOKEN = os.getenv("DROPBOX_ACCESS_TOKEN")

# Initialize Dropbox client
dbx = dropbox.Dropbox(ACCESS_TOKEN)

# Function to process a single Excel file from Dropbox and extract a column
def process_file(dropbox_file_path, column_name):
    try:
        # Get the file content from Dropbox
        metadata, res = dbx.files_download(path=dropbox_file_path)

        # Load the file content into a BytesIO object
        file_content = BytesIO(res.content)

        # Read the Excel file into a DataFrame
        df = pd.read_excel(file_content)

        # Extract the specified column and return it as a list
        if column_name in df.columns:
            return df[column_name].dropna().tolist()
        else:
            print(f"Column '{column_name}' not found in file: {dropbox_file_path}")
            return []
    except Exception as e:
        print(f"Error processing file {dropbox_file_path}: {e}")
        return []

# Function to process all files in a Dropbox folder and extract a column
def process_all_files_from_dropbox(dropbox_folder_path, column_name):
    try:
        # List files in the folder
        result = dbx.files_list_folder(dropbox_folder_path)
        files_to_process = []

        # Prepare a list of files to process
        while True:
            for entry in result.entries:
                if isinstance(entry, dropbox.files.FileMetadata) and entry.name.endswith('.xlsx'):
                    files_to_process.append(entry.path_display)

            # Check if there are more files to list
            if result.has_more:
                result = dbx.files_list_folder_continue(result.cursor)
            else:
                break

        # Initialize a list to hold all column data
        all_data = []

        # Use ThreadPoolExecutor to process files in parallel
        num_workers = os.cpu_count() * 2  # Number of worker threads to use
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Initialize tqdm for progress bar
            with tqdm(total=len(files_to_process), desc="Processing Files", unit="file") as pbar:
                futures = {
                    executor.submit(process_file, file_path, column_name): file_path
                    for file_path in files_to_process
                }

                # Wait for all processing to finish
                for future in as_completed(futures):
                    column_data = future.result()
                    if column_data:
                        all_data.extend(column_data)
                    pbar.update(1)  # Update the progress bar after each file

        return all_data
    except Exception as e:
        print(f"Error: {e}")
        return []

def get_all_top150_academics():
    shared_folder_path = '/Science Twitter/Data/Raw/openalex/people/allTop150academics'  # Replace with your folder path
    column_name = 'id'  # Replace with the name of the column to extract
    return set(process_all_files_from_dropbox(shared_folder_path, column_name))

if __name__ == "__main__":
    data = set(get_all_top150_academics())
    # Save the data to a file
    with open('data/ids/allTop150academics.txt', 'w') as f:
        for item in data:
            f.write("%s\n" % item)

