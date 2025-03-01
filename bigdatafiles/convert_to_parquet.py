import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import dask.bag as db
from dask.diagnostics import ProgressBar
import random
import string

# Input and output directories
INPUT_DIR = 'DataUnzip_cleaned'
OUTPUT_DIR = 'Data_parquet'
ERROR_LOG = 'error_log.txt'

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def log_error(file_path, error_message):
    """
    Logs errors to an error log file.
    """
    try:
        with open(ERROR_LOG, 'a') as log_file:  # Append to the error log
            log_file.write(f"File: {file_path}, Error: {error_message}\n")
    except Exception as e:
        print(f"Failed to log error for {file_path}: {str(e)}")

def is_valid_json_file(file_path):
    """
    Check if a file is non-empty and contains valid JSON.
    """
    try:
        if os.path.getsize(file_path) == 0:  # Check for empty file
            print(f"Skipping empty file: {file_path}")
            log_error(file_path, "File is empty")
            return False
        with open(file_path, 'r', encoding='utf-8') as f:
            json.load(f)  # Try to load the file as JSON
        return True
    except (json.JSONDecodeError, Exception) as e:
        print(f"Invalid JSON file: {file_path}, Error: {e}")
        log_error(file_path, f"Invalid JSON file: {e}")
        return False

def convert_json_to_parquet(file_path):
    """
    Converts a single JSON file into a Parquet file.
    The Parquet file is saved in Sampler_output/ with the same name as the JSON file.
    """
    try:
        # Extract the file name (without extension) to name the Parquet file
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        output_path = os.path.join(OUTPUT_DIR, f"{random_string}_{file_name}.parquet")
        
        print(f"Converting JSON file to Parquet: {file_path} -> {output_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)  # Load the entire JSON file as Python object

            # Ensure the data is a list of dictionaries for Parquet compatibility
            if isinstance(json_data, dict):
                json_data = [json_data]  # Wrap the dict into a list
            elif not isinstance(json_data, list):
                raise ValueError(f"JSON file {file_path} must be a list or a dictionary")

            # Convert the list of dictionaries to a PyArrow table
            table = pa.Table.from_pylist(json_data)
            
            # Write the table to a Parquet file
            pq.write_table(table, output_path, compression='snappy')
            
            print(f"Successfully converted {file_path} to Parquet at {output_path}")
            return f"Processed {file_path} successfully."
        except Exception as e:
            print(f"Error converting JSON to Parquet: {str(e)}")
            log_error(file_path, f"Error converting JSON to Parquet: {e}")
            return f"Error processing file {file_path}: {str(e)}"
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        log_error(file_path, f"Error processing file: {e}")
        return f"Error processing file: {str(e)}"

def main():
    """
    Main function to process all JSON files independently.
    """
    # Get all JSON files (recursively) in the INPUT_DIR
    all_json_files = [
        os.path.join(dirpath, filename)
        for dirpath, subdirs, filenames in os.walk(INPUT_DIR)
        for filename in filenames 
        if filename.endswith('.json') and os.path.isfile(os.path.join(dirpath, filename))
    ]

    print(f"Found {len(all_json_files)} JSON files to process: {all_json_files}")
    
    # Use Dask Bag to parallelize the processing of individual JSON files
    bag = db.from_sequence(all_json_files, npartitions=16)  # Divide files into 16 partitions
    results = bag.map(convert_json_to_parquet).compute()
    
    # Print summary of processing
    print(f"Results: {results}")

if __name__ == "__main__":
    main()