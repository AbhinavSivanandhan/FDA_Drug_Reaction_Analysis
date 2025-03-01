import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def download_file(link, line_number, data_folder, max_size_bytes, total_downloaded_size):
    try:
        link = link.strip()
        if not link:
            return 0  # Skip empty lines

        # Print progress message at the start of download
        print(f"Starting download for file {line_number}: {link}")

        response = requests.get(link, stream=True)
        response.raise_for_status()  # Check for request errors

        # Get the filename from the link or use a default naming scheme
        filename = str(line_number)+"-"+os.path.basename(link) or f'file_{line_number}.html'
        file_path = os.path.join(data_folder, filename)

        # Write the content to the file in chunks and track the size
        file_size = 0
        with open(file_path, 'wb') as output_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                output_file.write(chunk)
                file_size += len(chunk)
                
                # Stop if the cumulative size of all downloads exceeds the limit
                if total_downloaded_size[0] + file_size > max_size_bytes:
                    print(f"Reached the 50GB limit. Stopping downloads.")
                    return file_size  # Return the file size even if interrupted early

        print(f'Completed download for file {line_number}: {file_path}')
        return file_size
    except requests.RequestException as e:
        print(f"Failed to download {link}: {e}")
        return 0

def download_links_parallel(link_file, start_line, end_line, max_size_gb=50, max_workers=20):
    # Ensure the Data folder exists
    data_folder = 'Data'
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Convert max_size to bytes for comparison
    max_size_bytes = max_size_gb * (1024 ** 3)
    total_downloaded_size = [0]  # Using a list for mutable tracking in threads

    # Read the link file and get the specified range of lines
    with open(link_file, 'r', encoding='utf-8') as file:
        links = file.readlines()

    # Adjust end_line to not exceed the number of lines in the file
    end_line = min(end_line, len(links))

    # Prepare links and line numbers for the specified range
    links_to_download = [(links[i - 1], i) for i in range(start_line, end_line + 1)]

    # Use ThreadPoolExecutor to parallelize downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_file, link, line_number, data_folder, max_size_bytes, total_downloaded_size)
                   for link, line_number in links_to_download]

        for future in as_completed(futures):
            downloaded_size = future.result()
            total_downloaded_size[0] += downloaded_size

            # Stop all tasks if the size limit is reached
            if total_downloaded_size[0] >= max_size_bytes:
                print("Download limit reached. Cancelling remaining downloads.")
                break

# Usage example:
# Specify the file with links, range of lines, and number of parallel downloads
download_links_parallel('output_links.txt', 1, 1568, max_size_gb=100, max_workers=50)
