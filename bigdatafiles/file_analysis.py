import os
import json
import logging
from dask import delayed, compute
from dask.diagnostics import ProgressBar


def configure_logging(log_file):
    """
    Configure logging to log to both a file and the console.

    Parameters:
        log_file (str): Path to the log file.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()


def extract_structure(data, parent_key='', depth=1):
    """
    Recursively extract the structure of a JSON object up to the specified depth.

    Parameters:
        data (dict or list): JSON data to analyze.
        parent_key (str): Current key path (for nested keys).
        depth (int): Maximum depth to analyze nested structures.

    Returns:
        set: A set of all nested keys (as "parent.child" paths) in the JSON structure.
    """
    if depth == 0:
        return {parent_key} if parent_key else set()

    keys = set()
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            keys |= extract_structure(value, full_key, depth - 1)
    elif isinstance(data, list):
        if data:
            keys |= extract_structure(data[0], parent_key, depth - 1)
    else:
        keys.add(parent_key)

    return keys


def analyze_json_structure(file_path, depth=1):
    """
    Analyze the key structure of a JSON file.

    Parameters:
        file_path (str): Path to the JSON file.
        depth (int): Maximum depth to analyze nested structures.

    Returns:
        dict: A dictionary containing the structure of the JSON file or error details.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        structure = extract_structure(data, depth=depth)
        return {"file": file_path, "structure": structure, "error": None}
    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {e}")
        return {"file": file_path, "structure": None, "error": str(e)}


def compare_structures(results):
    """
    Compare the structures of all analyzed JSON files.

    Parameters:
        results (list): List of dictionaries with file structures.

    Returns:
        dict: A summary of differences between file structures.
    """
    structures = {result['file']: result['structure'] for result in results if result['error'] is None}
    errors = [result for result in results if result['error']]

    if not structures:
        return {"common_keys": [], "unique_keys": {}, "errors": errors}

    # Find common keys across all structures
    common_keys = set.intersection(*(struct for struct in structures.values()))

    # Find unique keys for each file
    unique_keys = {file: list(struct.difference(common_keys)) for file, struct in structures.items()}

    return {
        "common_keys": list(common_keys),  # Convert to list for JSON serialization
        "unique_keys": unique_keys,
        "errors": errors
    }


def find_json_files_in_subdirectories(root_dir):
    """
    Find JSON files in subdirectories of a given root directory.

    Parameters:
        root_dir (str): Root directory to search.

    Returns:
        list: List of JSON file paths.
    """
    json_files = []
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(subdir, file))
    return json_files


def main(
    json_root_dir,
    log_file="json_analysis.log",
    report_file="json_analysis_report.json",
    depth=1,
    scheduler="threads",
    n_workers=None
):
    """
    Main function to analyze JSON files and generate reports.

    Parameters:
        json_root_dir (str): Root directory containing subdirectories with JSON files.
        log_file (str): Path to the log file.
        report_file (str): Path to save the JSON analysis report.
        depth (int): Maximum depth to analyze nested structures.
        scheduler (str): Dask scheduler to use ('threads', 'processes', etc.).
        n_workers (int): Number of parallel workers (default is determined by Dask).
    """
    global logger
    logger = configure_logging(log_file)
    logger.info(f"Starting analysis of JSON files in {json_root_dir} with depth={depth}, scheduler={scheduler}, n_workers={n_workers}")

    # Find JSON files in subdirectories
    json_files = find_json_files_in_subdirectories(json_root_dir)
    if not json_files:
        logger.warning("No JSON files found in the directory.")
        return

    logger.info(f"Found {len(json_files)} JSON files for analysis.")

    # Use Dask to analyze files in parallel
    delayed_tasks = [delayed(analyze_json_structure)(file, depth) for file in json_files]
    with ProgressBar():
        results = compute(*delayed_tasks, scheduler=scheduler, num_workers=n_workers)

    # Process results
    results = list(results)
    summary = compare_structures(results)

    # Save the summary to a file
    with open(report_file, "w") as f:
        json.dump(summary, f, indent=4)
    logger.info(f"Analysis complete! Report saved to {report_file}")

    # Display progress and summary on terminal
    logger.info(f"Common keys across files: {summary['common_keys']}")
    logger.info(f"Unique keys in each file: {summary['unique_keys']}")
    if summary["errors"]:
        logger.error(f"Errors encountered: {summary['errors']}")


# Run the script
if __name__ == "__main__":
    # Adjustable parameters
    json_root_directory = "DataUnzip"  # Root directory containing JSON files in subdirectories
    log_file_path = "json_analysis.log"  # Path to the log file
    report_file_path = "json_analysis_report.json"  # Path to save the JSON analysis report
    analysis_depth = 5  # Maximum depth to analyze nested structures
    dask_scheduler = "threads"  # Scheduler to use: 'threads', 'processes', or 'synchronous'
    parallel_workers = 8  # Number of parallel workers (set None to use Dask's default)

    # Call the main function with the parameters
    main(
        json_root_dir=json_root_directory,
        log_file=log_file_path,
        report_file=report_file_path,
        depth=analysis_depth,
        scheduler=dask_scheduler,
        n_workers=parallel_workers
    )