# File: pipeline.py

from pyflow_core import Workflow
from tasks import split_file as task_split_file
from tasks import run_word_count_on_list as task_run_word_count_on_list
from tasks import sum_counts as task_sum_counts
import sys
import argparse
import os
import json # Import json to load config initially

# Default intermediate work directory if not specified in config
DEFAULT_WORK_DIR = "_pyflow_default_work"

# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the PyFlow Split-Map-Reduce pipeline using structured config.")
    parser.add_argument(
        "-c", "--config", required=True, help="Path to JSON configuration file."
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Clean up the work directory before running."
    )
    args = parser.parse_args()

    # --- Pre-load config partially to get work_dir ---
    config_data = {}
    try:
        with open(args.config, 'r') as f:
            config_data = json.load(f)
        print(f"Pre-loaded configuration from: {args.config}")
    except FileNotFoundError:
        print(f"Error: Configuration file not found: {args.config}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from configuration file '{args.config}': {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
         print(f"Error reading config file '{args.config}' before Workflow init: {e}", file=sys.stderr)
         sys.exit(1)

    # Extract work_dir from pre-loaded data, use default if missing
    global_params_initial = config_data.get("global_params", {})
    workflow_work_dir = global_params_initial.get("work_dir", DEFAULT_WORK_DIR)
    print(f"Using intermediate work directory: {workflow_work_dir}")


    # --- Create a Workflow instance with the configured work_dir ---
    workflow = Workflow(
        work_dir=workflow_work_dir, # Pass the extracted or default work_dir
        max_workers=4              # Keep other params as needed
        # config_file is not needed here, we load manually below
    )

    # --- Assign the full pre-loaded config to the workflow instance ---
    # The Workflow instance now exists and has the correct work_dir set
    workflow.config = config_data # Assign the already loaded dictionary

    # --- Get Config Sections (from the instance's config) ---
    global_params = workflow.config.get("global_params", {})
    tasks_config = workflow.config.get("tasks", {})

    # --- Validate Global Params ---
    config_output_dir = global_params.get("output_dir")
    if not config_output_dir:
        print("Error: 'output_dir' not found in 'global_params' in configuration file.", file=sys.stderr)
        sys.exit(1)
    print(f"Pipeline Description: {global_params.get('pipeline_description', 'N/A')}")

    # --- Extract and Validate Parameters for Each Task (remains the same) ---
    # ... (validation for split_task_config, wc_task_config, sum_task_config) ...
    # --- Split Task ---
    split_task_config = tasks_config.get("file_splitter", {})
    split_params = split_task_config.get("params", {})
    split_input_file = split_params.get("input_file")
    split_num_splits = split_params.get("num_splits")
    if not split_input_file: sys.exit("Error: tasks.file_splitter.params.input_file not found.")
    if split_num_splits is None: sys.exit("Error: tasks.file_splitter.params.num_splits not found.")
    try:
        split_num_splits = int(split_num_splits)
        if split_num_splits <= 0: raise ValueError("must be positive")
    except (ValueError, TypeError): sys.exit(f"Error: Invalid 'num_splits' value.")

    # --- Word Count Task ---
    # (No params needed here for validation, script path injected later)

    # --- Summarize Task ---
    sum_task_config = tasks_config.get("result_summer", {})
    sum_params = sum_task_config.get("params", {})
    sum_output_filename = sum_params.get("final_output_filename")
    if not sum_output_filename: sys.exit("Error: tasks.result_summer.params.final_output_filename not found.")


    # --- Handle Paths and Script Location (remains the same) ---
    abs_input_path = os.path.abspath(split_input_file)
    if not os.path.exists(abs_input_path):
        print(f"Error: Input file '{split_input_file}' not found at '{abs_input_path}'", file=sys.stderr)
        sys.exit(1)

    abs_output_dir = os.path.abspath(config_output_dir)
    print(f"Using configured output directory: {abs_output_dir}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    wc_script_name = "word_counter.sh"
    wc_script_path = os.path.join(script_dir, wc_script_name)
    if not os.path.exists(wc_script_path): sys.exit(f"Error: Word count script '{wc_script_name}' not found.")
    if not os.access(wc_script_path, os.X_OK): sys.exit(f"Error: Word count script '{wc_script_path}' not executable.")

    # Inject script path into the config object held by the workflow instance
    if "tasks" not in workflow.config: workflow.config["tasks"] = {}
    if "word_counter" not in workflow.config["tasks"]: workflow.config["tasks"]["word_counter"] = {}
    if "params" not in workflow.config["tasks"]["word_counter"]: workflow.config["tasks"]["word_counter"]["params"] = {}
    workflow.config["tasks"]["word_counter"]["params"]["word_count_script_path"] = wc_script_path
    print(f"Using word count script: {wc_script_path}")


    # --- Apply the decorator AFTER workflow instance is created ---
    # It's crucial that the workflow instance exists before decorating
    split_file = workflow.task(task_split_file)
    run_word_count_on_list = workflow.task(task_run_word_count_on_list)
    sum_counts = workflow.task(task_sum_counts)


    # --- Define the ACTUAL Workflow Graph (remains the same logic) ---
    print("\n--- Defining Workflow Structure ---")

    split_files_list_output = split_file(
        input_path=abs_input_path,
        num_splits=split_num_splits
    )
    counts_list_output = run_word_count_on_list(
        split_files_list=split_files_list_output
    )
    total_count_file_output = sum_counts(
        counts_list=counts_list_output,
        final_output_filename=sum_output_filename
    )

    print("--- Workflow Structure Definition Complete ---")
    is_task_output = hasattr(total_count_file_output, 'id') and total_count_file_output.id in workflow.task_calls
    if is_task_output:
        print(f"Target task ID: {total_count_file_output.id}")
    else:
        print(f"Final target is not a task output: {total_count_file_output}")


    # --- Execute (remains the same) ---
    if args.cleanup:
        # This cleanup uses the work_dir configured during Workflow init
        workflow.cleanup()

    try:
        print("\n--- Running Workflow ---")
        if not is_task_output:
             print(f"Final target '{total_count_file_output}' is not a runnable task output. Exiting.")
             sys.exit(0)

        final_output_file_path = workflow.run(total_count_file_output)

        print("\n--- Workflow Run Method Finished ---")
        print(f"Final result file generated at: {final_output_file_path}")
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