import csv
import gzip
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
import random
import ast

import orjson
import pandas as pd
from tqdm import tqdm

from src.download_s3 import download_all_files

# Local Directories
download_dir = Path("data/snapshot")
output_dir = Path("data/processed")
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
MAX_RETRIES = 4

def process_line(line, work_id_list):
    try:
        record = orjson.loads(line.strip())
    except Exception as e:
        logging.warning(f"Invalid JSON: {line} - {e}")
        return None  # Skip invalid or malformed lines
    
    publication_year = record.get("publication_year")
    cited_works = record.get("referenced_works", [])
    
    if not publication_year or int(publication_year) < 2001 or not cited_works:
        return None
    else:
        results = []
        for cited_work in cited_works:
            if cited_work in work_id_list[publication_year]:
                results.append((cited_work, record.get("id"), publication_year))
        return results

def save_to_csv(rows, file_prefix, folder_name):
    file_path = folder_name / f"{file_prefix}.csv"
    file_exists = file_path.exists()
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if not file_exists:
            writer.writerow(["cited_work_id", "citing_work_id", "citation_year"])
        writer.writerows(rows)

def process_local_file(local_file_path, work_id_list, folder_name):
    file_prefix = Path(local_file_path).stem
    # skikp if the file has already been processed
    if (folder_name / f"{file_prefix}.csv").exists():
        logging.info(f"File already processed: {local_file_path}")
        return None
    else:
        results = []
        try:
            with gzip.open(local_file_path, "rt", encoding="utf-8") as f:
                for line in f:
                    try:
                        line_results = process_line(line, work_id_list)
                        if line_results:
                            results.extend(line_results)
                    except Exception as e:
                        logging.warning(f"Failed processing line: {e}")
            
            if results:
                save_to_csv(results, file_prefix, folder_name)
            
            logging.info(f"Successfully processed file: {local_file_path}")
        except Exception as e:
            logging.error(f"Error processing file {local_file_path}: {e}")
            raise

# def process_local_file(local_file_path, work_id_list, folder_name):
#     file_prefix = Path(local_file_path).stem
#     file_path = folder_name / f"{file_prefix}.csv"
#     file_exists = file_path.exists()

#     with open(file_path, "a", newline="", encoding="utf-8") as csv_file:
#         writer = csv.writer(csv_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
#         if not file_exists:
#             writer.writerow(["cited_work_id", "citing_work_id", "citation_year"])
        
#         try:
#             # Read and decompress into memory
#             with gzip.open(local_file_path, "rb") as gz_file:
#                 file_data = gz_file.read()

#             # Now decode once
#             lines = file_data.decode("utf-8").splitlines()

#             # Iterate lines
#             buffer = []
#             batch_size = 10_000
#             for line in lines:
#                 line_results = process_line(line, work_id_list)
#                 if line_results:
#                     buffer.extend(line_results)
#                 if len(buffer) >= batch_size:
#                     writer.writerows(buffer)
#                     buffer.clear()

#             if buffer:
#                 writer.writerows(buffer)

#             logging.info(f"Successfully processed file: {local_file_path}")

#         except Exception as e:
#             logging.error(f"Error processing file {local_file_path}: {e}")
#             raise



def set_aside_citations(output_dir, work_ids_path, sample_name, test):
    print("Setting aside citations")
    work_ids = pd.read_csv(work_ids_path)  
    # make sublists of work_ids for each year>t, t=1961 to 2022
    work_id_list = {}
    for year in range(2001, 2026):
        valid_ids = work_ids[work_ids["year"] <= year]["work_id"].unique()
        work_id_list[year] = set(valid_ids)


    logging.info(f"Works to process: {len(work_ids)}")
    
    folder_name = Path(output_dir) / sample_name
    folder_name.mkdir(parents=True, exist_ok=True)
    
    if test:
        all_files = list(download_dir.glob("*.gz"))[:2] 
    else: # randomize the order of the files
        all_files = list(download_dir.glob("*.gz"))
        random.shuffle(all_files)
    total_files = len(all_files)
    print(total_files)
    
    max_workers = os.cpu_count() # - 2  # Reserve 2 cores for other tasks
    logging.info(f"Using {max_workers} workers")
    
    with tqdm(total=total_files, desc="Overall Progress") as progress:
        with ProcessPoolExecutor(
            max_workers=max_workers
        ) as executor:  # max_workers=max_workers
            futures = {
                executor.submit(process_local_file, file, work_id_list, folder_name): file
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


# function that takes all the processed files and aggregates them
def agg_citations(input_dir, output_dir, work_ids_path):
    print("Aggregating citations")
    all_files = list(input_dir.glob("*.csv"))
    print(f"Total files: {len(all_files)}")
    
    # read all the files and concatenate them
    all_data = pd.concat([pd.read_csv(file) for file in all_files])

    # collapse the data to the cited_work_id level, counting the number of citations in each year
    all_data = all_data.groupby(["cited_work_id", "citation_year"]).size().reset_index(name="citations")
    # sort the data by cited_work_id and citation_year
    all_data = all_data.sort_values(by=["cited_work_id", "citation_year"])

    work_ids = pd.read_csv(work_ids_path)  

    # merge with work data to get the  authors
    all_data = all_data.merge(work_ids, left_on="cited_work_id", right_on="work_id", how="left")
    # drop the cited_work_id column
    all_data = all_data.drop("cited_work_id", axis=1)

    all_data.to_csv(output_dir / "all_data.csv", index=False)
    print("Aggregation completed successfully.")


# now we take the output of the previous function count citations per author each year
def count_citations_per_author_per_year(agg_citations, relelant_ids_path, relevant_ids_column, output_dir, test):
    print("Counting citations per author per year")

    # read the relevant ids
    relevant_ids = pd.read_csv(relelant_ids_path)[relevant_ids_column]
    print(f"Total relevant ids: {len(relevant_ids)}")

    # read the data
    if test:
        all_data = pd.read_csv(agg_citations).sample(100000)
    else:
        all_data = pd.read_csv(agg_citations, dtype={"author_ids": str,
                                                        "citation_year": int,
                                                        "citations": int,
                                                        "year": int})
        print(f"total rows: {all_data.shape[0]}")
    
    # keep only the relevant columns
    all_data = all_data[["author_ids", "citation_year", "citations","year"]]
    print(all_data.head())

    # make sure the author_ids column is a list
    all_data["author_ids"] = all_data["author_ids"].apply(ast.literal_eval)

    # explode the author_ids column
    all_data = all_data.explode("author_ids")
    print(all_data.head())

    # filter the data
    all_data = all_data[all_data["author_ids"].isin(relevant_ids)]

    # count the number of citations per author per citation_year for works of each year
    all_data = all_data.groupby(["author_ids", "citation_year", "year"]).agg({"citations": "sum"}).reset_index()

    # sort the data
    all_data = all_data.sort_values(by=["author_ids", "year", "citation_year"])

    all_data.to_csv(output_dir / "citations_per_author_per_year.csv", index=False)



