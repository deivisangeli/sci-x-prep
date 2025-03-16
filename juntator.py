from pathlib import Path

import duckdb


def aggregate_table(input_dir, scope, output_file, aggregation_query):
    """
    Generic function to aggregate a specific scope (citations, coauthors, works).
    """
    scope_path = Path(input_dir) / scope
    all_files = list(scope_path.glob("*.csv"))

    if not all_files:
        raise FileNotFoundError(f"No CSV files found for {scope} in {scope_path}")

    conn = duckdb.connect(database=":memory:")

    # Incrementally load files and ensure column consistency
    for idx, file in enumerate(all_files):
        print(f"Loading file {idx + 1}/{len(all_files)}: {file}")
        if idx == 0:
            conn.execute(
                f"CREATE TABLE {scope} AS SELECT * FROM read_csv_auto('{file}', ALL_VARCHAR=TRUE)"
            )
        else:
            conn.execute(
                f"INSERT INTO {scope} SELECT * FROM read_csv_auto('{file}', ALL_VARCHAR=TRUE)"
            )

    # Debugging: Check row count after loading
    row_count = conn.execute(f"SELECT COUNT(*) FROM {scope}").fetchone()[0]
    print(f"Total rows loaded into {scope}: {row_count}")

    # Perform aggregation
    print(f"Aggregating {scope} data...")
    aggregated_data = conn.execute(aggregation_query).df()

    # Save aggregated data
    aggregated_data.to_csv(output_file, index=False)
    print(f"Aggregated {scope} saved to {output_file}")


def aggregate_all(input_dir, output_dir):
    """
    Aggregate all scopes: citations, coauthors, and works.
    """
    # Citations aggregation
    aggregate_table(
        input_dir,
        "citations",
        output_dir / "aggregated_citations.csv",
        """
        SELECT author_id, year, citation_year, type, SUM(CAST(count AS INTEGER)) AS total_count
        FROM citations
        GROUP BY author_id, year, citation_year, type
        """,
    )

    # Coauthors aggregation (no deduplication)
    aggregate_table(
        input_dir,
        "coauthors",
        output_dir / "aggregated_coauthors.csv",
        """
        SELECT author_id, year, type, STRING_AGG(coauthors, ';') AS all_coauthors
        FROM coauthors
        GROUP BY author_id, year, type
        """,
    )

    # Works aggregation
    aggregate_table(
        input_dir,
        "works",
        output_dir / "aggregated_works.csv",
        """
        SELECT author_id, year, type, SUM(CAST(count AS INTEGER)) AS total_count
        FROM works
        GROUP BY author_id, year, type
        """,
    )


# Example
if __name__ == "__main__":
    input_dir = "processed_scopes/allAcademics202501_exemplo"  # Directory containing citations, coauthors, and works
    output_dir = Path("aggregated_results_duckdb/allAcademics202501")
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        aggregate_all(input_dir, output_dir)
    except Exception as e:
        print(f"Error during aggregation: {e}")
