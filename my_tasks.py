# File: my_tasks.py
import os
from pyflow_core import run_shell # Import helpers if needed

# Define task logic as plain functions
# They will receive task_work_dir and config from the engine when called

def generate_initial_data(filename_base, task_work_dir, config):
    """Creates an initial data file, using config."""
    content = config.get("data_content", "default_content ")
    multiplier = config.get("content_multiplier", 3)

    output_filename = os.path.join(task_work_dir, f"{filename_base}.txt")
    abs_output_filename = os.path.abspath(output_filename) # Make path absolute

    with open(abs_output_filename, 'w') as f:
        f.write(content * multiplier)
    print(f"  [Task Logic] generate_initial_data: Created {abs_output_filename} with content='{content}', multiplier={multiplier}")
    return abs_output_filename # Return the absolute path

def process_data(input_file_path, suffix, task_work_dir, config):
    """Processes the data file using a shell command (e.g., word count)."""
    # input_file_path should now be absolute from the upstream task
    output_filename = os.path.join(task_work_dir, f"processed_{suffix}.txt")
    abs_output_filename = os.path.abspath(output_filename) # Make output path absolute too

    wc_command = config.get("wordcount_command", "wc -w")
    fail_on_b = config.get("fail_processing_b", False)

    if suffix == "from_B" and fail_on_b:
        print(f"  [Task Logic] process_data (for B): INTENTIONALLY FAILING as per config")
        raise RuntimeError("Intentional failure for process_data on branch B")
    else:
        # Command uses the absolute input path now
        command = f"{wc_command} < \"{input_file_path}\" > \"{abs_output_filename}\""

    # Only run shell if not intentionally failing
    if not (suffix == "from_B" and fail_on_b):
         abs_task_work_dir = os.path.abspath(task_work_dir) # Make cwd absolute too (good practice)
         run_shell(command, cwd=abs_task_work_dir) # Use absolute cwd
         print(f"  [Task Logic] process_data: Created {abs_output_filename}")

    return abs_output_filename # Return the absolute path

def summarize_result(processed_file_a, processed_file_b, task_work_dir):
    """Reads results from two files and summarizes."""
    # processed_file_a and processed_file_b should now be absolute paths
    summary_filename = os.path.join(task_work_dir, "summary.txt")
    abs_summary_filename = os.path.abspath(summary_filename) # Make path absolute

    try:
        with open(processed_file_a, 'r') as f_a:
            count_a = f_a.read().strip()
    except FileNotFoundError:
        count_a = "[FILE A NOT FOUND]"
        print(f"  [Task Logic] summarize_result: File not found {processed_file_a}")
    except Exception as e:
        count_a = f"[ERROR READING FILE A: {e}]"
        print(f"  [Task Logic] summarize_result: Error reading {processed_file_a}: {e}")

    try:
        with open(processed_file_b, 'r') as f_b:
            count_b = f_b.read().strip()
    except FileNotFoundError:
        count_b = "[FILE B NOT FOUND]"
        print(f"  [Task Logic] summarize_result: File not found {processed_file_b}")
    except Exception as e:
        count_b = f"[ERROR READING FILE B: {e}]"
        print(f"  [Task Logic] summarize_result: Error reading {processed_file_b}: {e}")


    summary = f"File A count: {count_a}\nFile B count: {count_b}"

    with open(abs_summary_filename, 'w') as f_out:
        f_out.write(summary)

    print(f"  [Task Logic] summarize_result: Created {abs_summary_filename}")
    print(f"  [Task Logic] --- Summary ---:\n{summary}")
    return abs_summary_filename # Return the absolute path