# File: tasks.py
import os
import math
import subprocess
import hashlib  # <-- Import hashlib
from pyflow_core import run_shell

# --- split_file remains the same ---
def split_file(input_path, num_splits, task_work_dir, config):
    # ... (no changes here) ...
    print(f"  [Task Logic] split_file: Splitting '{input_path}' into {num_splits} parts.")
    output_files = []
    try:
        with open(input_path, 'r') as f_in: lines = f_in.readlines()
        total_lines = len(lines); lines_per_split = math.ceil(total_lines / num_splits) if total_lines > 0 else 0
        for i in range(num_splits):
            start_line = i * lines_per_split; end_line = min((i + 1) * lines_per_split, total_lines)
            split_lines = lines[start_line:end_line]
            output_filename = os.path.join(task_work_dir, f"split_{i+1:02d}.txt"); abs_output_filename = os.path.abspath(output_filename)
            with open(abs_output_filename, 'w') as f_out: f_out.writelines(split_lines)
            print(f"  [Task Logic] split_file: Created '{abs_output_filename}' ({len(split_lines)} lines)")
            output_files.append(abs_output_filename)
        while len(output_files) < num_splits:
            i = len(output_files); output_filename = os.path.join(task_work_dir, f"split_{i+1:02d}.txt"); abs_output_filename = os.path.abspath(output_filename)
            with open(abs_output_filename, 'w') as f_out: pass
            print(f"  [Task Logic] split_file: Created empty '{abs_output_filename}'")
            output_files.append(abs_output_filename)
    except FileNotFoundError: raise
    except Exception as e: print(f"  [Task Logic] split_file: Error: {e}"); raise
    return output_files


# --- MODIFIED run_word_count_on_list ---
def run_word_count_on_list(split_files_list, task_work_dir, config):
    """
    Runs word count SCRIPT on each file in the list, placing each result
    in a separate subdirectory named by a hash of the input file path,
    and returns a list of counts.
    """
    counts = []
    script_path = config.get("tasks", {}).get("word_counter", {}).get("params", {}).get("word_count_script_path")

    if not script_path or not os.path.exists(script_path):
         print(f"  [Task Logic] run_word_count_on_list: ERROR - Word count script path not found ('{script_path}')")
         raise FileNotFoundError(f"Word count script not found or configured: {script_path}")

    print(f"  [Task Logic] run_word_count_on_list: Processing {len(split_files_list)} files using script '{script_path}'.")
    abs_task_work_dir = os.path.abspath(task_work_dir)

    for i, file_path in enumerate(split_files_list):
        # Generate a hash based on the absolute input file path for the subdirectory name
        # Using absolute path ensures consistency even if relative paths were somehow passed
        abs_file_path = os.path.abspath(file_path)
        input_hash = hashlib.md5(abs_file_path.encode('utf-8')).hexdigest()[:10] # 10-char MD5 hash

        # Use the hash as the subdirectory name
        part_subdir_path = os.path.join(abs_task_work_dir, input_hash) # <-- Use hash here

        # Define the output filename (can keep the index for clarity within the hash dir)
        count_output_filename = f"count_{i+1:02d}.txt"
        abs_count_output_file = os.path.join(part_subdir_path, count_output_filename)

        command = f"bash \"{script_path}\" \"{file_path}\" > \"{abs_count_output_file}\""

        try:
            # Create the hash-named subdirectory
            print(f"  [Task Logic] run_word_count_on_list: Creating subdirectory '{part_subdir_path}' for input '{os.path.basename(file_path)}'")
            os.makedirs(part_subdir_path, exist_ok=True)

            run_shell(command, cwd=abs_task_work_dir)

            with open(abs_count_output_file, 'r') as f_count:
                count_str = f_count.read().strip()
                if not count_str: raise ValueError("Word count script produced empty output.")
                count = int(count_str)

            print(f"  [Task Logic] run_word_count_on_list: Count for '{os.path.basename(file_path)}' is {count} (result in '{abs_count_output_file}')")
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


# --- sum_counts remains the same ---
def sum_counts(counts_list, final_output_filename, task_work_dir, config):
    # ... (no changes here) ...
    print(f"  [Task Logic] sum_counts: Summing list: {counts_list}"); total = sum(counts_list)
    print(f"  [Task Logic] sum_counts: Total count is {total}")
    global_params = config.get("global_params", {}); output_dir_rel = global_params.get("output_dir")
    if not output_dir_rel: raise ValueError("'output_dir' not found in global_params")
    if not final_output_filename: raise ValueError("Final output filename parameter missing")
    output_dir_abs = os.path.abspath(output_dir_rel); final_output_path = os.path.join(output_dir_abs, final_output_filename)
    try:
        os.makedirs(output_dir_abs, exist_ok=True); print(f"  [Task Logic] sum_counts: Writing total count to {final_output_path}")
        with open(final_output_path, 'w') as f_out: f_out.write(str(total) + "\n")
    except Exception as e: print(f"  [Task Logic] sum_counts: ERROR writing output: {e}"); raise
    return final_output_path