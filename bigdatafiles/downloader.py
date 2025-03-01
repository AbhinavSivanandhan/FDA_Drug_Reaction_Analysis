import os
import requests

def download_links(link_file, start_line, end_line):
    # Ensure the Data folder exists
    data_folder = 'Data'
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Read the link file and get the specified range of lines
    with open(link_file, 'r', encoding='utf-8') as file:
        links = file.readlines()

    # Adjust end_line to not exceed the number of lines in the file
    end_line = min(end_line, len(links))

    # Download and save each link in the specified range
    for line_number in range(start_line, end_line + 1):
        try:
            link = links[line_number - 1].strip()  # Adjust for 0-based index
            if link:  # Check if the line is not empty
                response = requests.get(link)
                response.raise_for_status()  # Check for request errors

                # Get the filename from the link or use a default naming scheme
                filename = str(line_number)+os.path.basename(link) or f'file_{line_number}.html'
                file_path = os.path.join(data_folder, filename)

                # Write the content to the file
                with open(file_path, 'wb') as output_file:
                    output_file.write(response.content)
                print(f'Downloaded: {line_number}/{end_line} : {link} to {file_path}')
            else:
                print(f"Line {line_number} is empty, skipping.")
        except requests.RequestException as e:
            print(f"Failed to download {link}: {e}")

# Usage example:
# Specify the file with links and the range of lines you want to download
download_links('output_links.txt', 1, 1568)  # Example: download links from lines 1 to 500
