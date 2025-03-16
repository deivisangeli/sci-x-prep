import csv
import gzip
import shutil
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import orjson
import pandas as pd
from tqdm import tqdm

from src.download_s3 import download_all_files

# Local Directories
download_dir = Path("data/snapshot")
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

# Retry mechanism
MAX_RETRIES = 4


def process_line(line):
    record = orjson.loads(line.strip())
    
    topic_id = record.get("id")
    display_name = record.get("display_name")
    description = record.get("description")
    subfield = record.get("subfield")['id']
    field = record.get("field")['id']
    domain = record.get("domain")['id']

    row = [
        topic_id,
        display_name,
        description,
        subfield,
        field,
        domain
    ]
    return row
    
# for each file, we grab the lines that contain any of the valid_ids and save them to a new file

def process_local_file(input_file, valid_ids, output_dir):
    # grab the bit relative to openalex-snapshot/data/works
    relative_path = input_file.relative_to("data/snapshot/openalex-snapshot/data/works")
    output_file = output_dir / relative_path.with_suffix(".gz")

    # check if the file is already processed
    if output_file.exists():
        logging.info(f"File already processed: {output_file}")
    else:
        logging.info(f"Processing file: {input_file}")
        lines = []
        try:
            with gzip.open(input_file, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        line_result = orjson.loads(line.strip())
                        if line_result:
                            lines.append(line_result)
                    except Exception as e:
                        logging.warning(f"Failed processing line: {e}")

            if lines:
                # make sure the output directory exists
                output_file.parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(output_file, "wt", encoding="utf-8") as f:
                    # use the process_line function to make the row
                    for line in lines:
                        row = process_line(line)
                        if row:
                            f.write(orjson.dumps(row) + "\n")
                    

            logging.info(f"Processed file: {input_file}")
        except Exception as e:
            logging.error(f"Error processing file {input_file}: {e}")
            raise



def separate_topics(output_dir, valid_ids_path):
    """
    Process all files in the download directory using given valid IDs file as reference.
    valid_ids_path: Path to the file containing valid author IDs.
    """

    valid_ids = pd.read_csv(valid_ids_path)[id_col].tolist()
    # make it a set
    valid_ids = set(valid_ids)
    print(f"Valid IDs: {len(valid_ids)}")

    folder_name = output_dir / Path(valid_ids_path).stem
    folder_name.mkdir(parents=True, exist_ok=True)

    all_files = list( Path("data/snapshot/openalex-snapshot/data/works").rglob("*.gz"))
    total_files = len(all_files)

    max_workers = os.cpu_count() - 4  # 2 cores for other tasks
    logging.info(f"Using {max_workers} workers")

    with tqdm(total=total_files, desc="Overall Progress") as progress:
        with ProcessPoolExecutor(
            max_workers=max_workers
        ) as executor:  # max_workers=max_workers
            futures = {
                executor.submit(process_local_file, file, valid_ids, folder_name): file
                for file in all_files
            }

            for future in as_completed(futures):
                file = futures[future]
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        future.result()  # Raises exception if worker failed
                        progress.update(1)
                        break
                    except Exception as e:
                        retries += 1
                        logging.error(
                            f"Failed processing file {file} (Attempt {retries}/{MAX_RETRIES}): {e}"
                        )
                        if retries >= MAX_RETRIES:
                            logging.error(
                                f"File {file} failed after {MAX_RETRIES} retries."
                            )



