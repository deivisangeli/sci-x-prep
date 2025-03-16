import csv
import gzip
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
output_dir = Path("processed_scopes")
log_file = "process_log.log"

# Ensure directories exist
output_dir.mkdir(parents=True, exist_ok=True)

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
MAX_RETRIES = 3


def load_valid_ids(path, id_col):
    if path.endswith(".txt"):
        with open(path, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    elif path.endswith(".csv"):
        df = pd.read_csv(path)
        return set(df[id_col])
    else:
        # excel
        df = pd.read_excel(path)
        return set(df[id_col])


def process_line(line, valid_ids):
    try:
        record = orjson.loads(line.strip())
    except Exception as e:
        logging.warning(f"Invalid JSON: {line} - {e}")
        return None  # Skip invalid or malformed lines

    publication_year = record.get("publication_year")
    record_type = record.get("type")
    authorships = record.get("authorships", [])

    results = {"works": [], "coauthors": [], "citations": []}

    # Scope 1: Works
    for authorship in authorships:
        author_id = authorship["author"]["id"]
        if author_id in valid_ids:
            results["works"].append([author_id, publication_year, record_type, 1])

    # Scope 2: Coauthors
    coauthor_names = [
        authorship["author"]["display_name"] for authorship in authorships
    ]
    for authorship in authorships:
        author_id = authorship["author"]["id"]
        if author_id in valid_ids:
            coauthors = [
                coauthor_name
                for coauthor_name, coauthor_id in zip(
                    coauthor_names,
                    [authorship["author"]["id"] for authorship in authorships],
                )
                if coauthor_id != author_id
            ]
            results["coauthors"].append(
                [author_id, publication_year, record_type, ";".join(coauthors)]
            )

    # Scope 3: Citations
    for year_data in record.get("counts_by_year", []):
        citation_year = year_data["year"]
        citation_count = year_data["cited_by_count"]
        for authorship in authorships:
            author_id = authorship["author"]["id"]
            if author_id in valid_ids:
                results["citations"].append(
                    [
                        author_id,
                        publication_year,
                        citation_year,
                        record_type,
                        citation_count,
                    ]
                )

    return results


def save_to_csv(scope, rows, file_prefix, folder_name):
    scope_dir = output_dir / folder_name / scope
    scope_dir.mkdir(parents=True, exist_ok=True)
    file_path = scope_dir / f"{file_prefix}_{scope}.csv"

    file_exists = file_path.exists()
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if not file_exists:
            headers = {
                "works": ["author_id", "year", "type", "count"],
                "coauthors": ["author_id", "year", "type", "coauthors"],
                "citations": ["author_id", "year", "citation_year", "type", "count"],
            }
            writer.writerow(headers[scope])
        writer.writerows(rows)


def process_local_file(local_file_path, valid_ids, folder_name):
    file_prefix = Path(local_file_path).stem
    results = {"works": [], "coauthors": [], "citations": []}

    try:
        # Process file in chunks to reduce memory usage
        with gzip.open(local_file_path, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    line_result = process_line(line, valid_ids)
                    if line_result:
                        for scope in results:
                            results[scope].extend(line_result[scope])
                except Exception as e:
                    logging.warning(f"Failed processing line: {e}")

        # Save results for each scope
        for scope, rows in results.items():
            save_to_csv(scope, rows, file_prefix, folder_name)

        logging.info(f"Successfully processed file: {local_file_path}")
    except Exception as e:
        logging.error(f"Error processing file {local_file_path}: {e}")
        raise


def make_folder(output_dir, ids_path):
    """
    Create a folder with the name of the file containing valid IDs to save processed data.
    output_dir: Path to the output directory.
    ids_path: Path to the file containing valid author IDs.
    """
    folder_name = Path(ids_path).stem
    scope_dir = output_dir / folder_name
    scope_dir.mkdir(parents=True, exist_ok=True)
    return folder_name


def process_all(output_dir, valid_ids_path, id_col):
    """
    Process all files in the download directory using given valid IDs file as reference.
    output_dir: Path to the defined output directory.
    valid_ids_path: Path to the file containing valid author IDs.
    """

    valid_ids = load_valid_ids(valid_ids_path, id_col)

    folder_name = make_folder(output_dir, valid_ids_path)
    all_files = list(download_dir.glob("*.gz"))
    total_files = len(all_files)

    max_workers = os.cpu_count() - 2  # 2 cores for other tasks
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


# Example
if __name__ == "__main__":
    # Local Directories
    # valid_ids_path = "data/ids/allTop150academics.txt"
    # valid_ids_path = "data/ids/top150filtered_ids.txt"
    valid_ids_path = "data/ids/allAcademics202501_exemplo.csv"  # <<< INPUT
    authors_ids = "id"  # <<< INPUT

    download_dir = Path("data/snapshot")

    output_dir = Path("processed_scopes")

    log_file = "process_log.log"

    # Ensure directories exist
    output_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    # If download_dir is empty, use function download_s3.py
    if not any(download_dir.iterdir()):
        print("Downloading files from S3... This might take a while.")
        download_all_files()

    process_all(
        valid_ids_path=valid_ids_path, output_dir=output_dir, id_col=authors_ids
    )
    logging.info("Script completed successfully.")
