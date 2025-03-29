# File: tasks.py
import os
import math
import subprocess
from pyflow_core import run_shell

# --- split_file ---
# Accepts specific params directly, accesses full config if needed (not needed here)
def split_file(input_path, num_splits, task_work_dir, config):
    """Splits an input file into N roughly equal parts."""
    print(f"  [Task Logic] split_file: Splitting '{input_path}' into {num_splits} parts.")
    output_files = []
    try:
        # ... (splitting logic remains the same) ...
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

# --- run_word_count_on_list ---
# Accepts list directly. Needs full config to get injected script path.
def run_word_count_on_list(split_files_list, task_work_dir, config):
    """Runs word count SCRIPT on each file in the list and returns a list of counts."""
    counts = []
    # Access the config structure set up by pipeline.py to find the script path
    # It's better if pipeline.py injects this reliably.
    script_path = config.get("tasks", {}).get("word_counter", {}).get("params", {}).get("word_count_script_path")

    if not script_path or not os.path.exists(script_path):
         print(f"  [Task Logic] run_word_count_on_list: ERROR - Word count script path not found in config or script missing ('{script_path}')")
         raise FileNotFoundError(f"Word count script not found or configured: {script_path}")

    print(f"  [Task Logic] run_word_count_on_list: Processing {len(split_files_list)} files using script '{script_path}'.")

    # ... (rest of the loop and error handling remains the same) ...
    for i, file_path in enumerate(split_files_list):
        count_output_file = os.path.join(task_work_dir, f"count_{i+1:02d}.txt")
        abs_count_output_file = os.path.abspath(count_output_file)
        command = f"bash \"{script_path}\" \"{file_path}\" > \"{abs_count_output_file}\""
        try:
            run_shell(command, cwd=os.path.abspath(task_work_dir))
            with open(abs_count_output_file, 'r') as f_count:
                count_str = f_count.read().strip()
                if not count_str:
                     raise ValueError("Word count script produced empty output.")
                count = int(count_str)
            print(f"  [Task Logic] run_word_count_on_list: Count for '{os.path.basename(file_path)}' is {count}")
            counts.append(count)
        except FileNotFoundError:
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

# --- sum_counts ---
# Accepts list directly. Needs config for output_dir and final_output_filename.
def sum_counts(counts_list, final_output_filename, task_work_dir, config):
    """
    Sums a list of numbers and writes the result to a file
    in the output directory specified in the config.
    """
    print(f"  [Task Logic] sum_counts: Summing list: {counts_list}")
    total = sum(counts_list)
    print(f"  [Task Logic] sum_counts: Total count is {total}")

    # Get global output directory from the main config object
    global_params = config.get("global_params", {})
    output_dir_rel = global_params.get("output_dir")
    if not output_dir_rel:
        print("  [Task Logic] sum_counts: ERROR - 'output_dir' not specified in global_params.")
        raise ValueError("'output_dir' not found in global_params")

    if not final_output_filename:
         print("  [Task Logic] sum_counts: ERROR - Final output filename not provided.")
         raise ValueError("Final output filename parameter missing")

    # Make the output directory path absolute relative to the original CWD
    output_dir_abs = os.path.abspath(output_dir_rel)
    final_output_path = os.path.join(output_dir_abs, final_output_filename)

    try:
        print(f"  [Task Logic] sum_counts: Ensuring output directory exists: {output_dir_abs}")
        os.makedirs(output_dir_abs, exist_ok=True)
        print(f"  [Task Logic] sum_counts: Writing total count to {final_output_path}")
        with open(final_output_path, 'w') as f_out:
            f_out.write(str(total) + "\n")
    except Exception as e:
        print(f"  [Task Logic] sum_counts: ERROR writing output file '{final_output_path}': {e}")
        raise
    return final_output_path