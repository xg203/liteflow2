# File: my_tasks.py
import os
import math
import subprocess # Keep subprocess for CalledProcessError
from pyflow_core import run_shell # Import helpers

# --- split_file and sum_counts remain the same ---

def split_file(input_path, num_splits, task_work_dir, config):
    """Splits an input file into N roughly equal parts."""
    print(f"  [Task Logic] split_file: Splitting '{input_path}' into {num_splits} parts.")
    output_files = []
    try:
        with open(input_path, 'r') as f_in:
            lines = f_in.readlines()

        total_lines = len(lines)
        if total_lines == 0:
             print("  [Task Logic] split_file: Input file is empty.")
             lines_per_split = 0
        else:
             lines_per_split = math.ceil(total_lines / num_splits)

        for i in range(num_splits):
            start_line = i * lines_per_split
            end_line = min((i + 1) * lines_per_split, total_lines)
            split_lines = lines[start_line:end_line]

            output_filename = os.path.join(task_work_dir, f"split_{i+1:02d}.txt")
            abs_output_filename = os.path.abspath(output_filename)

            with open(abs_output_filename, 'w') as f_out:
                f_out.writelines(split_lines)

            print(f"  [Task Logic] split_file: Created '{abs_output_filename}' ({len(split_lines)} lines)")
            output_files.append(abs_output_filename)

        while len(output_files) < num_splits:
            i = len(output_files)
            output_filename = os.path.join(task_work_dir, f"split_{i+1:02d}.txt")
            abs_output_filename = os.path.abspath(output_filename)
            with open(abs_output_filename, 'w') as f_out:
                 pass # Create empty file
            print(f"  [Task Logic] split_file: Created empty '{abs_output_filename}'")
            output_files.append(abs_output_filename)

    except FileNotFoundError:
        print(f"  [Task Logic] split_file: Input file not found '{input_path}'")
        raise
    except Exception as e:
        print(f"  [Task Logic] split_file: Error during splitting: {e}")
        raise

    return output_files

def run_word_count_on_list(split_files_list, task_work_dir, config):
    """Runs word count SCRIPT on each file in the list and returns a list of counts."""
    counts = []
    # Get the path to the script - it should be injected into config by my_pipeline.py
    script_path = config.get("word_count_script_path")
    if not script_path or not os.path.exists(script_path):
         print(f"  [Task Logic] run_word_count_on_list: ERROR - Word count script path not found in config or script missing ('{script_path}')")
         # Fail the task if script is missing
         raise FileNotFoundError(f"Word count script not configured or found: {script_path}")

    print(f"  [Task Logic] run_word_count_on_list: Processing {len(split_files_list)} files using script '{script_path}'.")

    for i, file_path in enumerate(split_files_list):
        # Output file for the count result itself (optional but can be useful)
        count_output_file = os.path.join(task_work_dir, f"count_{i+1:02d}.txt")
        abs_count_output_file = os.path.abspath(count_output_file)

        # Construct the command to run the script, passing the split file path
        # Ensure paths are quoted for safety
        command = f"bash \"{script_path}\" \"{file_path}\" > \"{abs_count_output_file}\""

        try:
            # run_shell executes the command and checks for errors
            run_shell(command, cwd=os.path.abspath(task_work_dir))

            # Read the result (single number) from the script's output file
            with open(abs_count_output_file, 'r') as f_count:
                count_str = f_count.read().strip()
                if not count_str: # Handle empty output from script
                     raise ValueError("Word count script produced empty output.")
                count = int(count_str)

            print(f"  [Task Logic] run_word_count_on_list: Count for '{os.path.basename(file_path)}' is {count}")
            counts.append(count)
        except FileNotFoundError:
             # This would likely be caught by the script itself now, but keep for robustness
             print(f"  [Task Logic] run_word_count_on_list: Input split file not found '{file_path}' - Appending 0 count.")
             counts.append(0)
        except (subprocess.CalledProcessError, ValueError, IndexError) as e:
            print(f"  [Task Logic] run_word_count_on_list: Error processing '{os.path.basename(file_path)}' with script: {e}. Appending 0 count.")
            counts.append(0)
        except Exception as e:
             print(f"  [Task Logic] run_word_count_on_list: Unexpected error processing '{os.path.basename(file_path)}': {e}. Appending 0 count.")
             counts.append(0)

    print(f"  [Task Logic] run_word_count_on_list: Finished. Returning counts: {counts}")
    return counts

def sum_counts(counts_list):
    """Sums a list of numbers."""
    print(f"  [Task Logic] sum_counts: Summing list: {counts_list}")
    total = sum(counts_list)
    print(f"  [Task Logic] sum_counts: Total count is {total}")
    return total