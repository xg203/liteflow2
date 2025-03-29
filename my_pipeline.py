# File: my_pipeline.py

from pyflow_core import Workflow
from my_tasks import split_file as task_split_file
from my_tasks import run_word_count_on_list as task_run_word_count_on_list
from my_tasks import sum_counts as task_sum_counts
import sys
import argparse
import os # Import os

# --- Create a Workflow instance ---
workflow = Workflow(
    work_dir="_pyflow_split_map_reduce_config", # New work dir
    max_workers=4
    # config_file set via CLI
)

# --- Apply the decorator to the imported functions ---
split_file = workflow.task(task_split_file)
run_word_count_on_list = workflow.task(task_run_word_count_on_list)
sum_counts = workflow.task(task_sum_counts)


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the PyFlow Split-Map-Reduce pipeline using config file.")
    # Only need config path and cleanup flag now
    parser.add_argument(
        "-c", "--config", required=True, help="Path to JSON configuration file."
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Clean up the work directory before running."
    )
    args = parser.parse_args()

    # --- Load Configuration ---
    try:
        # Load the base config first
        workflow.config = workflow._load_config(args.config)
        print(f"Loaded configuration from: {args.config}")
    except Exception as e:
         print(f"Error loading config file '{args.config}': {e}", file=sys.stderr)
         sys.exit(1)

    # --- Get Workflow Parameters from Config ---
    config_input_file = workflow.config.get("input_file")
    config_num_splits = workflow.config.get("num_splits")

    if not config_input_file:
        print("Error: 'input_file' not found in configuration file.", file=sys.stderr)
        sys.exit(1)
    if config_num_splits is None: # Check for None explicitly as 0 could be valid (though unlikely here)
        print("Error: 'num_splits' not found in configuration file.", file=sys.stderr)
        sys.exit(1)
    try:
        # Validate num_splits is an integer > 0
        config_num_splits = int(config_num_splits)
        if config_num_splits <= 0:
             raise ValueError("Number of splits must be positive.")
    except (ValueError, TypeError) as e:
         print(f"Error: Invalid value for 'num_splits' in configuration: {e}", file=sys.stderr)
         sys.exit(1)

    # --- Handle Paths and Script Location ---
    # Make input path absolute (relative to config file location or CWD?)
    # Safest: Assume path in config is relative to the CWD where my_pipeline is run
    abs_input_path = os.path.abspath(config_input_file)
    if not os.path.exists(abs_input_path):
        print(f"Error: Input file specified in config not found: {abs_input_path}", file=sys.stderr)
        sys.exit(1)

    # Find the word count script relative to *this* pipeline script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    wc_script_name = "word_counter.sh"
    wc_script_path = os.path.join(script_dir, wc_script_name)

    if not os.path.exists(wc_script_path):
        print(f"Error: Word count script '{wc_script_name}' not found in script directory: {script_dir}", file=sys.stderr)
        sys.exit(1)
    if not os.access(wc_script_path, os.X_OK):
        print(f"Error: Word count script '{wc_script_path}' is not executable. Please run: chmod +x {wc_script_name}", file=sys.stderr)
        sys.exit(1)

    # Inject the absolute script path into the config dictionary that tasks will see
    workflow.config["word_count_script_path"] = wc_script_path
    print(f"Using word count script: {wc_script_path}")

    # --- Define the ACTUAL Workflow Graph using config values ---
    print("\n--- Defining Workflow Structure ---")

    split_files_list_output = split_file(
        input_path=abs_input_path,
        num_splits=config_num_splits
    )
    counts_list_output = run_word_count_on_list(
        split_files_list=split_files_list_output
    )
    total_count_output = sum_counts(
        counts_list=counts_list_output
    )

    print("--- Workflow Structure Definition Complete ---")
    if hasattr(total_count_output, 'id') and total_count_output.id in workflow.task_calls:
        print(f"Target task ID: {total_count_output.id}")
    else:
        print(f"Final target is not a task output: {total_count_output}")

    # --- Execute ---
    if args.cleanup:
        workflow.cleanup()

    try:
        print("\n--- Running Workflow ---")
        if not hasattr(total_count_output, 'id') or total_count_output.id not in workflow.task_calls:
             print(f"Final target '{total_count_output}' is not a runnable task output. Exiting.")
             sys.exit(0)

        final_result_value = workflow.run(total_count_output)

        print("\n--- Workflow Run Method Finished ---")
        print(f"Final calculated total word count: {final_result_value}")

    except Exception as e:
        print(f"\nPipeline execution failed overall: {e}", file=sys.stderr)
        # import traceback
        # traceback.print_exc()
        sys.exit(1)