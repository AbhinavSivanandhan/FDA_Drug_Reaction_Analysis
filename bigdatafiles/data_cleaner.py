import os
import json
import dask.bag as db
from dask.diagnostics import ProgressBar

# List of fields to remove
FIELDS_TO_REMOVE = [
    "rxcui",
    "product_ndc",
    "spl_id",
    "spl_set_id",
    "package_ndc",
    "nui",
    "application_number"
]

# Input and output directories
INPUT_DIR = 'DataUnzip'
OUTPUT_DIR = 'DataUnzip_cleaned'

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def remove_fields_from_dict(data, fields_to_remove):
    """
    Recursively removes specified fields from a nested dictionary or list.
    """
    if isinstance(data, dict):
        return {
            key: remove_fields_from_dict(value, fields_to_remove)
            for key, value in data.items() if key not in fields_to_remove
        }
    elif isinstance(data, list):
        return [remove_fields_from_dict(item, fields_to_remove) for item in data]
    else:
        return data

def process_large_file(file_path):
    """
    Process a large JSON file chunk-by-chunk to avoid memory overflow.
    Writes cleaned data to the corresponding file in the output directory.
    """
    try:
        # Get the relative path and output path
        relative_path = os.path.relpath(file_path, INPUT_DIR)
        output_path = os.path.join(OUTPUT_DIR, relative_path)
        print(f"Input: {file_path} -> Relative: {relative_path} -> Output: {output_path}")
        
        # Ensure the output directory structure exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Open the input file for reading
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"JSONDecodeError: Failed to load {file_path} due to {e}")
            return f"Error processing {file_path}: {str(e)}"
        except Exception as e:
            print(f"Unexpected Error: Failed to load {file_path} due to {e}")
            return f"Error processing {file_path}: {str(e)}"
        
        # Remove specified fields
        cleaned_data = remove_fields_from_dict(data, FIELDS_TO_REMOVE)
        
        # Write the cleaned JSON to the output file
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, indent=2)
            print(f"Successfully wrote cleaned file to {output_path}")
            return f"Processed {file_path} successfully."
        except Exception as e:
            print(f"Failed to write file {output_path} due to {e}")
            return f"Error writing {output_path}: {str(e)}"
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return f"Error processing {file_path}: {str(e)}"

def main():
    """
    Main function to process large JSON files in parallel using dask.bag.
    """
    # Get all JSON files from the input directory
    all_json_files = [
        os.path.join(dirpath, filename)
        for dirpath, _, filenames in os.walk(INPUT_DIR)
        for filename in filenames if filename.endswith('.json')
    ]

    # Check how many files are found
    print(f"Found {len(all_json_files)} JSON files to process: {all_json_files}")
    
    # Use dask bag to parallelize the file processing
    bag = db.from_sequence(all_json_files, npartitions=30)  # Divide files into 16 partitions
    results = bag.map(process_large_file).compute()
    
    # Print summary of processing
    print(f"Results: {results}")

if __name__ == "__main__":
    main()