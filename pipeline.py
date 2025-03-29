# File: pipeline.py

from pyflow_core import Workflow
# Updated import statement
from tasks import split_file as task_split_file
from tasks import run_word_count_on_list as task_run_word_count_on_list
from tasks import sum_counts as task_sum_counts
import sys
import argparse
import os

# --- Create a Workflow instance ---
workflow = Workflow(
    work_dir="_pyflow_split_map_reduce_config",
    max_workers=4
    # config_file set via CLI
)

# --- Apply the decorator to the imported functions ---
split_file = workflow.task(task_split_file)
run_word_count_on_list = workflow.task(task_run_word_count_on_list)
# Ensure sum_counts is also decorated
sum_counts = workflow.task(task_sum_counts)


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the PyFlow Split-Map-Reduce pipeline using config file.")
    parser.add_argument(
        "-c", "--config", required=True, help="Path to JSON configuration file."
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Clean up the work directory before running."
    )
    args = parser.parse_args()

    # --- Load Configuration ---
    try:
        workflow.config = workflow._load_config(args.config)
        print(f"Loaded configuration from: {args.config}")
    except Exception as e:
         print(f"Error loading config file '{args.config}': {e}", file=sys.stderr)
         sys.exit(1)

    # --- Get Workflow Parameters from Config ---
    config_input_file = workflow.config.get("input_file")
    config_num_splits = workflow.config.get("num_splits")
    config_output_dir = workflow.config.get("output_dir") # Get output dir for validation

    # --- Validate Config Parameters ---
    if not config_input_file:
        print("Error: 'input_file' not found in configuration file.", file=sys.stderr)
        sys.exit(1)
    if config_num_splits is None:
        print("Error: 'num_splits' not found in configuration file.", file=sys.stderr)
        sys.exit(1)
    if not config_output_dir:
        print("Error: 'output_dir' not found in configuration file.", file=sys.stderr)
        sys.exit(1)

    try:
        config_num_splits = int(config_num_splits)
        if config_num_splits <= 0:
             raise ValueError("Number of splits must be positive.")
    except (ValueError, TypeError) as e:
         print(f"Error: Invalid value for 'num_splits' in configuration: {e}", file=sys.stderr)
         sys.exit(1)

    # --- Handle Paths and Script Location ---
    abs_input_path = os.path.abspath(config_input_file)
    if not os.path.exists(abs_input_path):
        print(f"Error: Input file specified in config not found: {abs_input_path}", file=sys.stderr)
        sys.exit(1)

    # Output directory path handling (make absolute relative to CWD)
    # The task itself will create it, but we log the absolute path here
    abs_output_dir = os.path.abspath(config_output_dir)
    print(f"Using configured output directory: {abs_output_dir}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    wc_script_name = "word_counter.sh"
    wc_script_path = os.path.join(script_dir, wc_script_name)

    if not os.path.exists(wc_script_path):
        print(f"Error: Word count script '{wc_script_name}' not found in script directory: {script_dir}", file=sys.stderr)
        sys.exit(1)
    if not os.access(wc_script_path, os.X_OK):
        print(f"Error: Word count script '{wc_script_path}' is not executable. Please run: chmod +x {wc_script_name}", file=sys.stderr)
        sys.exit(1)

    # Inject the script path into the config dictionary
    workflow.config["word_count_script_path"] = wc_script_path
    print(f"Using word count script: {wc_script_path}")

    # --- Define the ACTUAL Workflow Graph using config values ---
    print("\n--- Defining Workflow Structure ---")

    split_files_list_output = split_file(
        input_path=abs_input_path,
        num_splits=config_num_splits
        # config is passed implicitly
    )
    counts_list_output = run_word_count_on_list(
        split_files_list=split_files_list_output
        # config is passed implicitly
    )
    # Call the sum_counts task which now accepts config implicitly
    total_count_file_output = sum_counts(
        counts_list=counts_list_output
    )

    print("--- Workflow Structure Definition Complete ---")
    # Check if the final target is a TaskOutput object
    is_task_output = hasattr(total_count_file_output, 'id') and total_count_file_output.id in workflow.task_calls
    if is_task_output:
        print(f"Target task ID: {total_count_file_output.id}")
    else:
        print(f"Final target is not a task output: {total_count_file_output}")

    # --- Execute ---
    if args.cleanup:
        workflow.cleanup() # Cleans the workflow intermediate dir, not the final output dir

    try:
        print("\n--- Running Workflow ---")
        if not is_task_output:
             print(f"Final target '{total_count_file_output}' is not a runnable task output. Exiting.")
             sys.exit(0)

        # Execute the workflow. The result is now the path to the final file.
        final_output_file_path = workflow.run(total_count_file_output)

        print("\n--- Workflow Run Method Finished ---")
        # Updated print message
        print(f"Final result file generated at: {final_output_file_path}")

        # Optionally read the content of the result file
        try:
             with open(final_output_file_path, 'r') as f_res:
                  final_value = f_res.read().strip()
                  print(f"Value in result file: {final_value}")
        except Exception as e:
             print(f"Warning: Could not read result file '{final_output_file_path}': {e}")


    except Exception as e:
        print(f"\nPipeline execution failed overall: {e}", file=sys.stderr)
        # import traceback
        # traceback.print_exc()
        sys.exit(1)