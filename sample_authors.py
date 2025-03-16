import csv
import gzip
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import orjson
import pandas as pd
from tqdm import tqdm

log_file = "process_log.log"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)

MAX_RETRIES = 4

# Load the institution IDs once (outside the per-line function)
def load_institution_ids():
    db_path = os.getenv("db_path")
    if not db_path:
        raise ValueError("Environment variable 'db_path' is not set.")
    excel_path = Path(db_path, "Science Twitter/Data/Raw/openalex/institutions/top150_openalex_ids.xlsx")
    df = pd.read_excel(excel_path)
    return set(df["id"])

# Global cache for institution_ids (loaded only once)
INSTITUTION_IDS = load_institution_ids()

def pick_author_id(line):
    record = orjson.loads(line.strip())
    # check if the author has any affiliations at all
    if not record.get("affiliations"):
        return None
    elif any(aff["institution"]["id"] in INSTITUTION_IDS for aff in record["affiliations"]):
        return record["id"]
    return None

def process_local_file(input_file, output_dir):
    # grab the bit relative to openalex-snapshot/data/authors
    relative_path = input_file.relative_to("data/snapshot/openalex-snapshot/data/authors")
    output_file = output_dir / relative_path.with_suffix(".csv")
    
    # check if the file is already processed
    if output_file.exists():
        logging.info(f"File already processed: {output_file}")
        return
    
    logging.info(f"Processing file: {input_file}")
    lines = []
    try:
        with gzip.open(input_file, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    line_result = pick_author_id(line)
                    if line_result:
                        # Wrap the result in a list to form a valid CSV row.
                        lines.append([line_result])
                except Exception as e:
                    logging.warning(f"Failed processing line: {e}")

        # Save results
        if lines:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["id"])
                writer.writerows(lines)
                    
        logging.info(f"Successfully processed file: {input_file}")
    except Exception as e:
        logging.error(f"Error processing file {input_file}: {e}")
        raise

def get_all_authors(output_dir):
    """
    Process all files in the download directory.
    """
    input_dir = Path("data/snapshot/openalex-snapshot/data/authors")
    all_files = list(input_dir.rglob("*.gz"))
    total_files = len(all_files)

    max_workers = os.cpu_count() - 2 if os.cpu_count() and os.cpu_count() > 2 else 1
    logging.info(f"Using {max_workers} workers")

    with tqdm(total=total_files, desc="Overall Progress") as progress:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Map file to its future.
            futures = {executor.submit(process_local_file, file, output_dir): file for file in all_files}

            for future in as_completed(futures):
                file = futures[future]
                attempt = 0
                while attempt < MAX_RETRIES:
                    try:
                        future.result()  # This will raise if the worker failed.
                        progress.update(1)
                        break  # Break out if successful.
                    except Exception as e:
                        attempt += 1
                        logging.error(
                            f"Failed processing file {file} (Attempt {attempt}/{MAX_RETRIES}): {e}"
                        )
                        if attempt >= MAX_RETRIES:
                            logging.error(f"File {file} failed after {MAX_RETRIES} retries.")
                            break  # Optionally, you could re-submit the task here.

# finally let's define a function that takes all the processed files and aggregates them
def aggregate_authors(input_dir, output_file):
    all_files = list(input_dir.rglob("*.csv"))
    total_files = len(all_files)
    logging.info(f"Found {total_files} files to aggregate.")

    all_authors = set()
    for file in all_files:
        with open(file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                all_authors.add(row[0])

    logging.info(f"Total authors: {len(all_authors)}")

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])
        writer.writerows([[author] for author in all_authors])