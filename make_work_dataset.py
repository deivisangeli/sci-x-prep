import csv
import gzip
import logging
import os
import orjson
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tqdm import tqdm


log_file = "process_log.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)

MAX_RETRIES = 4

def process_line(line):
    try:
        record = orjson.loads(line.strip())
    except Exception as e:
        logging.warning(f"Invalid JSON: {line} - {e}")
        return None

    # Safely extract authorship IDs.
    authorships = record.get("authorships", [])
    author_ids = set()
    for auth in authorships:
        author = auth.get("author", {})
        if isinstance(author, dict):
            a_id = author.get("id")
            if a_id is not None:
                author_ids.add(a_id)
    author_ids_str = "|".join(author_ids)

    # Extract fields with safe fallback for nested dictionaries.
    work_id = record.get("id")
    publication_year = record.get("publication_year")
    work_type = record.get("type")

    # Safely extract primary_location -> source -> id
    primary_location = record.get("primary_location", {}) or {}
    if not isinstance(primary_location, dict):
        primary_location = {}
    source = primary_location.get("source", {}) or {}
    location_id = source.get("id")

    # Safely extract primary_topic -> id
    primary_topic = record.get("primary_topic", {}) or {}
    if not isinstance(primary_topic, dict):
        primary_topic = {}
    topic_id = primary_topic.get("id")

    row = [
        work_id,
        publication_year,
        work_type,
        location_id,
        topic_id,
        author_ids_str,
    ]
    return row



def process_local_file(input_file, output_dir, input_dir):
    relative_path = input_file.relative_to(input_dir)
    output_file = output_dir / relative_path.with_suffix(".csv")

    # check if the file is already processed
    if output_file.exists():
        logging.info(f"File already processed: {output_file}")
        return
    
    rows = []
    try:
        with gzip.open(input_file, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    row = process_line(line)
                    if row:
                        rows.append(row)
                except Exception as e:
                    logging.warning(f"Failed processing line: {e}")

        if rows:
            # make sure the output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)
            file_exists = output_file.exists()
            with open(output_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
                if not file_exists:
                    headers = [
                        "work_id", 
                        "year", 
                        "type", 
                        "primary_location_source_id", 
                        "primary_topic_id", 
                        "author_ids"
                    ]
                    writer.writerow(headers)
                writer.writerows(rows)
            print(f"Saved {len(rows)} rows to {output_file}")

        logging.info(f"Processed file: {input_file}")
    except Exception as e:
        logging.error(f"Error processing file {input_file}: {e}")
        raise

def prep_works(output_dir, valid_ids_path, input_dir):
    """
    Process all files in the input_dir using the valid IDs from valid_ids_path.
    """

    out_subfolder = output_dir / Path(valid_ids_path).stem
    out_subfolder.mkdir(parents=True, exist_ok=True)

    all_files = list(input_dir.rglob("*.gz"))
    print(f"Total files: {len(all_files)}")
    total_files = len(all_files)

    max_workers = os.cpu_count() - 2  # leave some cores free
    logging.info(f"Using {max_workers} workers")

    with tqdm(total=total_files, desc="Overall Progress") as progress:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_local_file, file, out_subfolder, input_dir): file
                for file in all_files
            }
            for future in as_completed(futures):
                file = futures[future]
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        future.result()  # will raise exception if processing failed
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


def agg_relevant_works(input_dir, output_dir):


    all_files = list(input_dir.rglob("*.csv"))
    print(f"Total files: {len(all_files)}")
    
    all_data = pd.concat([pd.read_csv(file) for file in all_files])
    all_data.to_csv(output_dir / "all_data.csv", index=False)
