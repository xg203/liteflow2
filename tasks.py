# File: tasks.py
import os
import math
import subprocess
import hashlib
# Import the modified run_shell
from pyflow_core import run_shell, _create_input_symlink # Also import the helper

# --- split_file remains the same ---
def split_file(input_path, num_splits, task_work_dir, config):
    # ... (no changes) ...
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
    Runs word count SCRIPT on each file in the list. For each part:
    - Creates a subdirectory named by input hash.
    - Creates symlink to the input split file inside the subdir.
    - Saves the executed command to .command.sh inside the subdir.
    - Places count result (e.g., count_01.txt) inside the subdir.
    Returns a list of counts.
    """
    counts = []
    script_path = config.get("tasks", {}).get("word_counter", {}).get("params", {}).get("word_count_script_path")

    if not script_path or not os.path.exists(script_path):
         print(f"  [Task Logic] run_word_count_on_list: ERROR - Word count script path not found ('{script_path}')")
         raise FileNotFoundError(f"Word count script not found or configured: {script_path}")

    print(f"  [Task Logic] run_word_count_on_list: Processing {len(split_files_list)} files using script '{script_path}'.")
    # task_work_dir is already absolute as passed by _run_task_in_process

    for i, file_path in enumerate(split_files_list):
        abs_file_path = os.path.abspath(file_path)
        input_hash = hashlib.md5(abs_file_path.encode('utf-8')).hexdigest()[:10]
        part_subdir_path = os.path.join(task_work_dir, input_hash) # Use task_work_dir directly

        count_output_filename = f"count_{i+1:02d}.txt"
        abs_count_output_file = os.path.join(part_subdir_path, count_output_filename)

        # Define path for the command log file for this specific part
        command_log_file_path = os.path.join(part_subdir_path, ".command.sh")

        command = f"bash \"{script_path}\" \"{file_path}\" > \"{abs_count_output_file}\""

        try:
            # Create the hash-named subdirectory
            print(f"  [Task Logic] run_word_count_on_list: Creating subdirectory '{part_subdir_path}' for input '{os.path.basename(file_path)}'")
            os.makedirs(part_subdir_path, exist_ok=True)

            # Create symlink for the specific input file for this part
            _create_input_symlink(file_path, part_subdir_path, link_prefix="input_split")

            # Call run_shell, passing the path for the command log file
            run_shell(
                command,
                cwd=part_subdir_path, # Run the command within the specific part's directory
                command_log_file=command_log_file_path
            )

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


# --- sum_counts ---
# Doesn't call run_shell, so no command log needed here unless we add one.
# Input linking is handled by _run_task_in_process.
def sum_counts(counts_list, final_output_filename, task_work_dir, config):
    # ... (no changes needed here, input links created by core) ...
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