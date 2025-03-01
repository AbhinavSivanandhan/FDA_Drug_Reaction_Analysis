import re

def extract_links(html_file, output_file):
    # Read the HTML content
    with open(html_file, 'r', encoding='utf-8') as file:
        html_content = file.read()
    
    # Use regular expressions to find all href links
    links = re.findall(r'href=["\'](.*?)["\']', html_content)
    
    # Write links to the output file, one link per line
    with open(output_file, 'w', encoding='utf-8') as file:
        for link in links:
            file.write(link + '\n')

# Usage example
extract_links('project.html', 'output_links.txt')