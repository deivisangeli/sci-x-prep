import logging
import random
import time
from multiprocessing import Pool, cpu_count
from pathlib import Path

import boto3
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("download_log.log"), logging.StreamHandler()],
)

# Suppressing checksum validation
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# AWS S3 Configuration
s3 = boto3.client("s3", config=boto3.session.Config(signature_version="s3v4"))
bucket = "openalex"
prefix = "data/works"

# Local Directories
project_root = (
    Path(__file__).resolve().parent.parent
)  # Assuming this script is in `src`
download_dir = project_root / "data/snapshot"
download_dir.mkdir(parents=True, exist_ok=True)  # Ensure the directory exists


# Retry mechanism with exponential backoff for throttling
def download_file(s3_key):
    retries = 50
    backoff_factor = 2
    max_delay = 60
    local_file_path = download_dir / Path(s3_key.replace("/", "_"))

    for attempt in range(retries):
        try:
            if not local_file_path.exists():
                logging.info(f"Downloading: {s3_key}")
                s3.download_file(bucket, s3_key, str(local_file_path))
            return local_file_path
        except Exception as e:
            if "RequestLimitExceeded" in str(e) or "Throttling" in str(e):
                wait_time = min(
                    backoff_factor**attempt + random.uniform(0, 1), max_delay
                )
                logging.warning(
                    f"Throttling detected. Retrying in {wait_time:.2f} seconds for {s3_key}..."
                )
                time.sleep(wait_time)
            elif attempt < retries - 1:
                logging.warning(
                    f"Retrying download for {s3_key} ({attempt + 1}/{retries})..."
                )
                time.sleep(backoff_factor**attempt + random.uniform(0, 1))
            else:
                logging.error(f"Failed to download {s3_key}: {e}")
                raise


# Download all files in parallel
def download_all_files():
    # List all files in the S3 bucket under the specified prefix
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_files = [
        obj["Key"]
        for page in page_iterator
        for obj in page.get("Contents", [])
        if obj["Key"].endswith(".gz")
    ]

    logging.info(f"Total files to download: {len(all_files)}")

    with Pool(cpu_count() * 2) as pool:
        # Display progress with tqdm
        for _ in tqdm(
            pool.imap(download_file, all_files),
            total=len(all_files),
            desc="Downloading Files",
        ):
            pass

    logging.info("All files downloaded successfully.")


if __name__ == "__main__":
    download_all_files()
