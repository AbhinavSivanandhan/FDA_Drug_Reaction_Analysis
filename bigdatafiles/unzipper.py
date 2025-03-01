import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

def unzip_file(file_path, output_folder, max_size_bytes, total_extracted_size, size_lock, file_counter):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Calculate the total size of files inside the zip
            total_size = sum(info.file_size for info in zip_ref.infolist())
            
            # Lock to safely update the cumulative size
            with size_lock:
                if total_extracted_size[0] + total_size > max_size_bytes:
                    print(f"Skipping {os.path.basename(file_path)}: Exceeds 800GB limit.")
                    return 0  # Skip this file without extraction
                
                # Add the size of this zip to the cumulative size
                total_extracted_size[0] += total_size
                file_counter[0] += 1  # Increment the file count safely

            # Create a subfolder named after the zip file (without the .zip extension)
            zip_name = os.path.splitext(os.path.basename(file_path))[0]
            extract_folder = os.path.join(output_folder, zip_name)
            os.makedirs(extract_folder, exist_ok=True)

            # Extract all files to the subfolder
            print(f"[{file_counter[0]}] Unzipping: {os.path.basename(file_path)}")
            zip_ref.extractall(extract_folder)
            print(f"[{file_counter[0]}] Completed unzipping: {os.path.basename(file_path)} to {extract_folder}, Current total size: {total_extracted_size[0] / (1024 ** 3):.2f} GB")

            return total_size
    except Exception as e:
        print(f"Failed to unzip {os.path.basename(file_path)}: {e}")
        return 0

def unzip_files_parallel(data_folder, output_folder, max_size_gb=800, max_workers=5):
    # Convert max size to bytes
    max_size_bytes = max_size_gb * (1024 ** 3)
    total_extracted_size = [0]  # Mutable tracker for the cumulative size in bytes
    file_counter = [0]  # Mutable tracker for counting processed files
    size_lock = Lock()  # Lock for safely updating the cumulative size and file count

    # Ensure the output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Get a list of all .zip files in the data folder
    zip_files = [os.path.join(data_folder, f) for f in os.listdir(data_folder) if zipfile.is_zipfile(os.path.join(data_folder, f))]

    # Parallelize unzipping with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(unzip_file, file_path, output_folder, max_size_bytes, total_extracted_size, size_lock, file_counter): file_path for file_path in zip_files}

        for future in as_completed(futures):
            file_path = futures[future]
            try:
                extracted_size = future.result()
                
                # Check if cumulative size limit has been reached
                if total_extracted_size[0] >= max_size_bytes:
                    print("Total extracted size limit of 800GB reached. Stopping further unzipping.")
                    break

            except Exception as exc:
                print(f"An error occurred while processing {os.path.basename(file_path)}: {exc}")

    print("Unzipping completed.")

# Usage example:
# Specify the data folder, the output folder, and the number of parallel unzipping workers
unzip_files_parallel('Data', 'DataUnzip', max_size_gb=800, max_workers=30)