#!/bin/bash



# Usage: ./script.sh [normal|reverse]
#   normal: processes files in ascending order (default)
#   reverse: processes files in descending order

ORDER=${1:-normal}
count=0
max_files=500000

# Generate the list of .gz files in the desired order.
if [ "$ORDER" == "reverse" ]; then
    data_files=$(find openalex-snapshot/data/works -type f -name "*.gz" | sort -r)
else
    data_files=$(find openalex-snapshot/data/works -type f -name "*.gz" | sort)
fi

# Loop through the list of files.
for data_file in $data_files; do
    # Check if we've reached the limit.
    if [ $count -ge $max_files ]; then
        echo "Reached limit of $max_files files. Exiting."
        break
    fi

    # Create a file_key by replacing '/' with '_' to track the file.
    file_key=$(echo "$data_file" | sed 's/[\/]/_/g')

    # Check if the file was already loaded.
    if grep -q "$file_key" loaded_works_files.txt 2>/dev/null; then
        echo "Skipping $data_file (already loaded)"
        continue
    fi

    # Attempt to load the file using BigQuery.
    if bq load --source_format=CSV -F '\t' \
        --schema 'work:string' \
        --project_id openalex-bq-453818 \
        openalex.works "$data_file"; then

        # On success, add the file_key to the tracking file.
        echo "$file_key" >> loaded_works_files.txt
        echo "Successfully loaded $data_file"
    else
        echo "Failed to load $data_file, will retry next run"
    fi

    # Increment the counter.
    count=$((count+1))
done

echo "Processed $count files"
