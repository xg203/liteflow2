# File: workflow/pipeline.py

# Updated imports to reflect the new location within the 'workflow' package
from workflow.pyflow_core import Workflow
from workflow.tasks import split_file as task_split_file
from workflow.tasks import run_word_count_on_list as task_run_word_count_on_list
from workflow.tasks import sum_counts as task_sum_counts
import sys
import argparse
import os
import json # Keep json import

# Default intermediate work directory if not specified in config
DEFAULT_WORK_DIR = "_pyflow_default_work"

# --- Main Execution Block ---
if __name__ == "__main__":
    # Adjust help message for config path
    parser = argparse.ArgumentParser(description="Run the PyFlow Split-Map-Reduce pipeline using structured config.")
    parser.add_argument(
        "-c", "--config", required=True, help="Path to JSON configuration file (e.g., config/config.json)."
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Clean up the work directory before running."
    )
    args = parser.parse_args()

    # --- Pre-load config partially to get work_dir ---
    config_data = {}
    try:
        # Ensure config path is treated correctly
        abs_config_path_arg = os.path.abspath(args.config)
        if not os.path.exists(abs_config_path_arg):
             raise FileNotFoundError(f"Config file specified not found: {abs_config_path_arg}")
        with open(abs_config_path_arg, 'r') as f:
            config_data = json.load(f)
        print(f"Pre-loaded configuration from: {abs_config_path_arg}")
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from configuration file '{abs_config_path_arg}': {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
         print(f"Error reading config file '{abs_config_path_arg}' before Workflow init: {e}", file=sys.stderr)
         sys.exit(1)

    # Extract work_dir from pre-loaded data, use default if missing
    global_params_initial = config_data.get("global_params", {})
    workflow_work_dir = global_params_initial.get("work_dir", DEFAULT_WORK_DIR)
    # Make work_dir absolute relative to the CWD where pipeline.py is run
    abs_workflow_work_dir = os.path.abspath(workflow_work_dir)
    print(f"Using intermediate work directory: {abs_workflow_work_dir}")


    # --- Create a Workflow instance with the configured work_dir ---
    # Pass the absolute work dir path
    workflow = Workflow(
        work_dir=abs_workflow_work_dir,
        max_workers=4
    )

    # --- Assign the full pre-loaded config to the workflow instance ---
    workflow.config = config_data

    # --- Get Config Sections (from the instance's config) ---
    global_params = workflow.config.get("global_params", {})
    tasks_config = workflow.config.get("tasks", {})

    # --- Validate Global Params ---
    config_output_dir = global_params.get("output_dir")
    if not config_output_dir:
        print("Error: 'output_dir' not found in 'global_params'.", file=sys.stderr)
        sys.exit(1)
    print(f"Pipeline Description: {global_params.get('pipeline_description', 'N/A')}")

    # --- Extract and Validate Parameters for Each Task ---
    # --- Split Task ---
    split_task_config = tasks_config.get("file_splitter", {})
    split_params = split_task_config.get("params", {})
    split_input_file = split_params.get("input_file")
    split_num_splits = split_params.get("num_splits")
    if not split_input_file:
        print("Error: tasks.file_splitter.params.input_file not found.", file=sys.stderr)
        sys.exit(1)
    if split_num_splits is None:
        print("Error: tasks.file_splitter.params.num_splits not found.", file=sys.stderr)
        sys.exit(1)
    try:
        split_num_splits = int(split_num_splits)
        if split_num_splits <= 0: raise ValueError("must be positive")
    except (ValueError, TypeError) as e:
        print(f"Error: Invalid 'num_splits' value ('{split_params.get('num_splits')}') in config: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Word Count Task (Validation primarily happens within task for script path) ---
    wc_task_config = tasks_config.get("word_counter", {})
    # We don't strictly need to validate params here as the task checks script path

    # --- Summarize Task ---
    sum_task_config = tasks_config.get("result_summer", {})
    sum_params = sum_task_config.get("params", {})
    sum_output_filename = sum_params.get("final_output_filename")
    if not sum_output_filename:
        print("Error: tasks.result_summer.params.final_output_filename not found.", file=sys.stderr)
        sys.exit(1)


    # --- Handle Paths and Script Location ---
    # Assume input_file path in config is relative to CWD where pipeline.py is run
    abs_input_path = os.path.abspath(split_input_file)
    if not os.path.exists(abs_input_path):
        print(f"Error: Input file '{split_input_file}' specified in config not found at '{abs_input_path}'", file=sys.stderr)
        sys.exit(1)

    # Make output dir path absolute relative to CWD
    abs_output_dir = os.path.abspath(config_output_dir)
    print(f"Using configured output directory: {abs_output_dir}")

    # --- Find the word count script relative to the project root ---
    pipeline_script_path = os.path.abspath(__file__) # Path to this pipeline.py
    workflow_dir = os.path.dirname(pipeline_script_path) # Path to workflow/
    project_root = os.path.dirname(workflow_dir) # Path to project root (one level up)
    script_dir_relative = "script" # Relative path from project root
    script_dir = os.path.join(project_root, script_dir_relative)

    wc_script_name = "word_counter.sh"
    wc_script_path = os.path.join(script_dir, wc_script_name)

    if not os.path.exists(wc_script_path):
        print(f"Error: Word count script '{wc_script_name}' not found in expected directory '{script_dir}'", file=sys.stderr)
        sys.exit(1)
    if not os.access(wc_script_path, os.X_OK):
        print(f"Error: Word count script '{wc_script_path}' is not executable. Please run: chmod +x {script_dir_relative}/{wc_script_name}", file=sys.stderr)
        sys.exit(1)

    # Inject script path into the config object held by the workflow instance
    # Ensure the nested dictionary structure exists before assigning
    if "tasks" not in workflow.config: workflow.config["tasks"] = {}
    if "word_counter" not in workflow.config["tasks"]: workflow.config["tasks"]["word_counter"] = {}
    if "params" not in workflow.config["tasks"]["word_counter"]: workflow.config["tasks"]["word_counter"]["params"] = {}
    workflow.config["tasks"]["word_counter"]["params"]["word_count_script_path"] = wc_script_path
    print(f"Using word count script: {wc_script_path}")

    # --- Apply the decorator AFTER workflow instance is created ---
    # Assign decorated functions to variables
    split_file = workflow.task(task_split_file)
    run_word_count_on_list = workflow.task(task_run_word_count_on_list)
    sum_counts = workflow.task(task_sum_counts)

    # --- Define the ACTUAL Workflow Graph using extracted config values and task handles ---
    print("\n--- Defining Workflow Structure ---")

    split_files_list_output = split_file(
        input_path=abs_input_path,
        num_splits=split_num_splits
        # config object is passed implicitly by the engine
    )
    counts_list_output = run_word_count_on_list(
        split_files_list=split_files_list_output # Pass dependency output
        # config object is passed implicitly by the engine
    )
    # Call the sum_counts task which now accepts config implicitly
    total_count_file_output = sum_counts(
        counts_list=counts_list_output,    # Pass dependency output
        final_output_filename=sum_output_filename # Pass specific arg
        # config object is passed implicitly by the engine
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
        # This cleanup uses the work_dir configured during Workflow init
        workflow.cleanup()

    try:
        print("\n--- Running Workflow ---")
        if not is_task_output:
             print(f"Final target '{total_count_file_output}' is not a runnable task output. Exiting.", file=sys.stderr)
             sys.exit(1) # Exit with error if target isn't valid

        # Execute the workflow. The result is the path to the final file.
        final_output_file_path = workflow.run(total_count_file_output)

        print("\n--- Workflow Run Method Finished ---")
        # Check if the run was successful (didn't raise exception) and got a result path
        if final_output_file_path:
            print(f"Final result file generated at: {final_output_file_path}")
            # Optionally read the content of the result file
            try:
                 with open(final_output_file_path, 'r') as f_res:
                      final_value = f_res.read().strip()
                      print(f"Value in result file: {final_value}")
            except Exception as e:
                 print(f"Warning: Could not read result file '{final_output_file_path}': {e}")
        else:
             # This case might happen if the final task didn't return anything,
             # or if the workflow completed but the target somehow wasn't in results.
             print("Workflow completed, but no final output path was returned.", file=sys.stderr)


    except Exception as e:
        print(f"\nPipeline execution failed overall: {e}", file=sys.stderr)
        # import traceback
        # traceback.print_exc() # Uncomment for detailed traceback during debugging
        sys.exit(1) # Exit with error status