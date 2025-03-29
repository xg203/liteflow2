# File: pipeline.py

from pyflow_core import Workflow
# Import task functions
from tasks import split_file as task_split_file
from tasks import run_word_count_on_list as task_run_word_count_on_list
from tasks import sum_counts as task_sum_counts
import sys
import argparse
import os

# --- Create a Workflow instance ---
workflow = Workflow(
    work_dir="_pyflow_structured_config", # New work dir
    max_workers=4
    # config_file set via CLI
)

# --- Apply the decorator to the imported functions ---
# We'll use these handles later
split_file = workflow.task(task_split_file)
run_word_count_on_list = workflow.task(task_run_word_count_on_list)
sum_counts = workflow.task(task_sum_counts)


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

    # --- Load Configuration ---
    try:
        # Load the entire structured config
        workflow.config = workflow._load_config(args.config) # Use method on instance
        print(f"Loaded configuration from: {args.config}")
    except Exception as e:
         print(f"Error loading config file '{args.config}': {e}", file=sys.stderr)
         sys.exit(1)

    # --- Get Config Sections ---
    global_params = workflow.config.get("global_params", {})
    tasks_config = workflow.config.get("tasks", {})

    # --- Validate Global Params ---
    config_output_dir = global_params.get("output_dir")
    if not config_output_dir:
        print("Error: 'output_dir' not found in 'global_params' in configuration file.", file=sys.stderr)
        sys.exit(1)
    print(f"Pipeline Description: {global_params.get('pipeline_description', 'N/A')}")

    # --- Extract and Validate Parameters for Each Task ---

    # --- Split Task ---
    split_task_config = tasks_config.get("file_splitter", {})
    split_params = split_task_config.get("params", {})
    split_input_file = split_params.get("input_file")
    split_num_splits = split_params.get("num_splits")

    if not split_input_file:
         print("Error: tasks.file_splitter.params.input_file not found in config.", file=sys.stderr)
         sys.exit(1)
    if split_num_splits is None:
         print("Error: tasks.file_splitter.params.num_splits not found in config.", file=sys.stderr)
         sys.exit(1)
    try:
        split_num_splits = int(split_num_splits)
        if split_num_splits <= 0: raise ValueError("must be positive")
    except (ValueError, TypeError):
        print(f"Error: Invalid 'num_splits' value ({split_params.get('num_splits')}) in config.", file=sys.stderr)
        sys.exit(1)

    # --- Word Count Task ---
    wc_task_config = tasks_config.get("word_counter", {})
    wc_params = wc_task_config.get("params", {})
    # We don't read specific params here, but we will inject the script path

    # --- Summarize Task ---
    sum_task_config = tasks_config.get("result_summer", {})
    sum_params = sum_task_config.get("params", {})
    sum_output_filename = sum_params.get("final_output_filename")
    if not sum_output_filename:
         print("Error: tasks.result_summer.params.final_output_filename not found in config.", file=sys.stderr)
         sys.exit(1)

    # --- Handle Paths and Script Location ---
    abs_input_path = os.path.abspath(split_input_file)
    if not os.path.exists(abs_input_path):
        print(f"Error: Input file '{split_input_file}' specified in config not found at '{abs_input_path}'", file=sys.stderr)
        sys.exit(1)

    abs_output_dir = os.path.abspath(config_output_dir)
    print(f"Using configured output directory: {abs_output_dir}")

    # Find and inject the word count script path into the config object
    # Tasks will access the config object passed by the executor
    script_dir = os.path.dirname(os.path.abspath(__file__))
    wc_script_name = "word_counter.sh"
    wc_script_path = os.path.join(script_dir, wc_script_name)

    if not os.path.exists(wc_script_path):
        print(f"Error: Word count script '{wc_script_name}' not found in script directory: {script_dir}", file=sys.stderr)
        sys.exit(1)
    if not os.access(wc_script_path, os.X_OK):
        print(f"Error: Word count script '{wc_script_path}' is not executable.", file=sys.stderr)
        sys.exit(1)

    # Inject path into the config dict that tasks will receive
    # Create the structure if it doesn't exist
    if "tasks" not in workflow.config: workflow.config["tasks"] = {}
    if "word_counter" not in workflow.config["tasks"]: workflow.config["tasks"]["word_counter"] = {}
    if "params" not in workflow.config["tasks"]["word_counter"]: workflow.config["tasks"]["word_counter"]["params"] = {}
    workflow.config["tasks"]["word_counter"]["params"]["word_count_script_path"] = wc_script_path
    print(f"Using word count script: {wc_script_path}")


    # --- Define the ACTUAL Workflow Graph using extracted config values ---
    print("\n--- Defining Workflow Structure ---")

    split_files_list_output = split_file(
        input_path=abs_input_path,         # Pass specific arg
        num_splits=split_num_splits        # Pass specific arg
        # config object is passed implicitly by the engine
    )
    counts_list_output = run_word_count_on_list(
        split_files_list=split_files_list_output # Pass dependency output
        # config object is passed implicitly by the engine
    )
    total_count_file_output = sum_counts(
        counts_list=counts_list_output,    # Pass dependency output
        final_output_filename=sum_output_filename # Pass specific arg
        # config object is passed implicitly by the engine
    )

    print("--- Workflow Structure Definition Complete ---")
    is_task_output = hasattr(total_count_file_output, 'id') and total_count_file_output.id in workflow.task_calls
    if is_task_output:
        print(f"Target task ID: {total_count_file_output.id}")
    else:
        print(f"Final target is not a task output: {total_count_file_output}")


    # --- Execute ---
    if args.cleanup:
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
