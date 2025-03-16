from pathlib import Path

from juntator import aggregate_all
from process_scopes import process_all
from src.download_s3 import download_all_files

if __name__ == "__main__":
    # Local Directories definitions-------------------------------------------------------------------------------------------
    valid_ids_path = (
        "data/ids/allAcademics202501.csv"  # <<< INPUT (file with author IDs)
    )
    valid_ids_filename = Path(valid_ids_path).stem.replace(
        ".csv", ""
    )  # File name without extension

    authors_ids_col = "id"  # <<< INPUT (OA id column name)

    download_dir = Path(
        "data/snapshot"
    )  # <<< INPUT (where the downloaded raw files are or will be)

    output_dir = Path(
        "processed_scopes"
    )  # <<< OUTPUT (where the processed files per scope will be saved)

    aggregated_dir = Path(
        "aggregated_results_duckdb"
    )  # <<< OUTPUT (where the aggregated files will be saved)

    # Aggregation directories:
    agg_input_dir = output_dir / valid_ids_filename
    agg_output_dir = aggregated_dir / valid_ids_filename
    agg_output_dir.mkdir(parents=True, exist_ok=True)

    # Ensure directories exist-------------------------------------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)
    aggregated_dir.mkdir(parents=True, exist_ok=True)

    # Logging configuration---------------------------------------------------------------------------------------------------
    log_file = "process_log.log"

    # Download files from S3 if download_dir is empty--------------------------------------------------------------------------
    if not any(download_dir.iterdir()):
        print("Downloading files from S3... This might take a while.")
        download_all_files()

    # PROCESSING PIPELINE------------------------------------------------------------------------------------------------------

    # Process all files in parallel
    process_all(
        valid_ids_path=valid_ids_path, output_dir=output_dir, id_col=authors_ids_col
    )
    print("Processing completed successfully.")
    print("Moving to aggregation...")

    # Aggregating processed files
    try:
        aggregate_all(agg_input_dir, agg_output_dir)
    except Exception as e:
        print(f"Error during aggregation: {e}")
