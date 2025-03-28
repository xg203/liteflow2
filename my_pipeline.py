# File: my_pipeline.py

from pyflow_core import Workflow # Import Workflow
# Import the task functions from the new module
from my_tasks import generate_initial_data as task_generate_initial_data
from my_tasks import process_data as task_process_data
from my_tasks import summarize_result as task_summarize_result
import sys
import argparse

# --- Create a Workflow instance ---
workflow = Workflow(
    work_dir="_pyflow_work_separate_tasks", # Changed work dir for clarity
    max_workers=4
    # config_file set via CLI
)

# --- Apply the decorator to the imported functions ---
# Give them potentially different names if desired, or keep original
generate_initial_data = workflow.task(task_generate_initial_data)
process_data = workflow.task(task_process_data)
summarize_result = workflow.task(task_summarize_result)

# --- Define the Workflow Graph (using the decorated functions) ---
# This part remains exactly the same syntactically
print("--- Defining Workflow Structure ---")

data_a = generate_initial_data(filename_base="sampleA")
data_b = generate_initial_data(filename_base="sampleB")

processed_a = process_data(input_file_path=data_a, suffix="from_A")
processed_b = process_data(input_file_path=data_b, suffix="from_B")

final_summary = summarize_result(processed_file_a=processed_a, processed_file_b=processed_b)

print("--- Workflow Structure Definition Complete ---")
if isinstance(final_summary, workflow.task_calls.get(final_summary.id).__class__): # Basic check
    print(f"Target task ID: {final_summary.id}")
else:
     print(f"Final target is not a task output: {final_summary}")


# --- Main Execution Block ---
# This part remains the same
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the PyFlow prototype pipeline.")
    parser.add_argument(
        "-c", "--config", help="Path to JSON configuration file."
    )
    parser.add_argument(
        "--fail-b", action="store_true", help="Intentionally fail the 'process_data' task for branch B."
    )
    parser.add_argument(
        "--cleanup", action="store_true", help="Clean up the work directory before running."
    )
    args = parser.parse_args()

    # Update workflow config based on CLI args
    try:
        workflow.config = workflow._load_config(args.config) # Load config file if provided
    except Exception as e:
         print(f"Error loading config: {e}", file=sys.stderr)
         sys.exit(1)


    # Override specific config from CLI for testing failure
    if args.fail_b:
         workflow.config["fail_processing_b"] = True

    if args.cleanup:
        workflow.cleanup()

    try:
        print("\n--- Running Workflow ---")
        # Check if final_summary is indeed a task output before running
        if not isinstance(final_summary, workflow.task_calls.get(final_summary.id).__class__ if hasattr(final_summary,'id') and final_summary.id in workflow.task_calls else object):
             print("Final target is not a runnable task output. Exiting.")
             sys.exit(0)

        # Execute the workflow by asking for the final desired output
        final_output_path = workflow.run(final_summary)

        print("\n--- Workflow Run Method Finished ---")
        if final_output_path:
            print(f"Final output generated at: {final_output_path}")

    except Exception as e:
        print(f"\nPipeline execution failed overall: {e}", file=sys.stderr)
        # traceback.print_exc() # Uncomment for more detail if needed
        sys.exit(1)