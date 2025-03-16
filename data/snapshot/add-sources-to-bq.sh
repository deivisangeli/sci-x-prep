#!/bin/bash

# Initialize a counter
count=0
max_files=5

for data_file in openalex-snapshot/data/sources/*/*.gz; do
    # Check if we've reached the limit
    if [ $count -ge $max_files ]; then
        echo "Reached limit of $max_files files. Exiting."
        break
    fi
    
    # Use the full path for tracking
    file_key=$(echo "$data_file" | sed 's/[\/]/_/g')
    
    # Check if file was already loaded successfully
    if grep -q "$file_key" loaded_sources_files.txt 2>/dev/null; then
        echo "Skipping $data_file (already loaded)"
        continue
    fi
    
    # Try to load the file
    if bq load --source_format=CSV -F '\t' \
        --schema 'sources:string' \
        --project_id openalex-bq \
        openalex.sources "$data_file"; then
        
        # If successful, add to our tracking file
        echo "$file_key" >> loaded_sources_files.txt
        echo "Successfully loaded $data_file"
    else
        echo "Failed to load $data_file, will retry next run"
    fi
    
    # Increment the counter
    count=$((count+1))
done

echo "Processed $count files"