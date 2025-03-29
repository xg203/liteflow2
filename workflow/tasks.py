# File: workflow/tasks.py
import os
import math
import subprocess
import hashlib
import shlex # Ensure shlex is imported if used for quoting

# Updated relative import since tasks.py and pyflow_core.py are in the same dir
from .pyflow_core import run_shell, _create_input_symlink

# --- Task 1: Split File ---
def split_file(input_path, num_splits, task_work_dir, config):
    """
    Splits an input file into N roughly equal parts.
    Input symlinks are created by the core executor (`_run_task_in_process`).
    Writes split files into its own task_work_dir.
    Returns a list of absolute paths to the split files.
    """
    abs_input_path = os.path.abspath(input_path) # Work with absolute path
    print(f"  [Task Logic] split_file: Starting to split '{abs_input_path}' into {num_splits} parts.")
    print(f"  [Task Logic] split_file: Output splits will be in '{task_work_dir}'")

    output_files = []
    try:
        # Ensure input exists before proceeding
        if not os.path.exists(abs_input_path):
             raise FileNotFoundError(f"Input file for splitting not found: {abs_input_path}")

        with open(abs_input_path, 'r') as f_in:
            lines = f_in.readlines()

        total_lines = len(lines)
        if total_lines == 0:
             print("  [Task Logic] split_file: Input file is empty.")
             lines_per_split = 0
        else:
             lines_per_split = math.ceil(total_lines / num_splits)
             if lines_per_split == 0: lines_per_split = 1 # Ensure at least 1 line per split if possible

        print(f"  [Task Logic] split_file: Total lines: {total_lines}, Lines per split: {lines_per_split}")

        for i in range(num_splits):
            start_line = i * lines_per_split
            # Stop if start_line exceeds total lines (can happen if num_splits is large)
            if start_line >= total_lines and total_lines > 0:
                 print(f"  [Task Logic] split_file: No more lines left for split {i+1}.")
                 break # Stop creating splits

            end_line = min((i + 1) * lines_per_split, total_lines)
            split_lines = lines[start_line:end_line]

            # Define output path within this task's working directory
            output_filename = os.path.join(task_work_dir, f"split_{i+1:02d}.txt")
            # Return absolute paths for downstream tasks
            abs_output_filename = os.path.abspath(output_filename)

            print(f"  [Task Logic] split_file: Writing split {i+1} ({len(split_lines)} lines) to '{abs_output_filename}'")
            with open(abs_output_filename, 'w') as f_out:
                f_out.writelines(split_lines)
            output_files.append(abs_output_filename)

        # Ensure we always return a list of length num_splits if requested, padding with empty files
        while len(output_files) < num_splits:
            i = len(output_files)
            output_filename = os.path.join(task_work_dir, f"split_{i+1:02d}.txt")
            abs_output_filename = os.path.abspath(output_filename)
            print(f"  [Task Logic] split_file: Creating empty padding file '{abs_output_filename}'")
            with open(abs_output_filename, 'w') as f_out:
                 pass # Create empty file
            output_files.append(abs_output_filename)

    except FileNotFoundError as e:
        print(f"  [Task Logic] split_file: ERROR - {e}")
        raise # Re-raise to fail the task properly
    except Exception as e:
        print(f"  [Task Logic] split_file: ERROR during splitting: {e}")
        # Consider printing traceback for unexpected errors
        # import traceback; traceback.print_exc()
        raise

    print(f"  [Task Logic] split_file: Finished splitting. Returning {len(output_files)} file paths.")
    return output_files


# --- Task 2: Run Word Count ---
def run_word_count_on_list(split_files_list, task_work_dir, config):
    """
    Runs word count on each file in the list using either host shell script
    or Docker, based on config.
    Creates subdirectories for each part's intermediate results.
    Returns a list of integer counts.
    Input symlinks (for the list itself) are created by the core executor.
    """
    counts = []
    # --- Get Configuration ---
    task_conf = config.get("tasks", {}).get("word_counter", {})
    params = task_conf.get("params", {})
    docker_conf = task_conf.get("docker", {})
    use_docker = docker_conf.get("enabled", False)
    docker_image = docker_conf.get("image") if use_docker else None

    # Get script path (needed for both modes) - injected by pipeline.py
    script_path_in_config = params.get("word_count_script_path")
    if not script_path_in_config or not os.path.exists(script_path_in_config):
         print(f"  [Task Logic] run_word_count_on_list: ERROR - Word count script path not found or missing ('{script_path_in_config}')")
         raise FileNotFoundError(f"Word count script not found or configured: {script_path_in_config}")
    host_script_path = os.path.abspath(script_path_in_config)
    host_script_dir = os.path.dirname(host_script_path)
    script_basename = os.path.basename(host_script_path)

    if use_docker and not docker_image:
        print("  [Task Logic] run_word_count_on_list: ERROR - Docker enabled but no image specified.")
        raise ValueError("Docker image missing in config for word_counter task")

    print(f"  [Task Logic] run_word_count_on_list: Processing {len(split_files_list)} files.")
    print(f"  [Task Logic] run_word_count_on_list: Using Docker: {use_docker}" + (f" (Image: {docker_image})" if use_docker else ""))
    # task_work_dir is already absolute path passed by executor

    # --- Loop through input files ---
    for i, file_path in enumerate(split_files_list):
        # Use absolute path for input file consistency
        abs_file_path = os.path.abspath(file_path)
        input_filename = os.path.basename(abs_file_path)
        host_input_dir = os.path.dirname(abs_file_path) # Host dir containing input

        # Define subdirectory for this part using input hash
        input_hash = hashlib.md5(abs_file_path.encode('utf-8')).hexdigest()[:10]
        # Subdirectory path within this task's work dir
        part_subdir_path = os.path.join(task_work_dir, input_hash)

        # Define output count file path within the part's subdirectory
        count_output_filename = f"count_{i+1:02d}.txt"
        host_abs_count_output_file = os.path.join(part_subdir_path, count_output_filename)

        # Define path for the command script log
        command_log_file_path = os.path.join(part_subdir_path, ".command.sh")

        # --- Prepare Command ---
        command_to_run = ""
        run_cwd = part_subdir_path # Default CWD is the part's specific subdir

        if use_docker:
            # Define container paths
            container_input_dir = "/inputs"
            container_output_dir = "/outputs" # This is the part_subdir mounted
            container_script_dir = "/scripts"
            container_script_path = f"{container_script_dir}/{script_basename}"
            container_input_path = f"{container_input_dir}/{input_filename}"

            # Prepare docker run arguments using shlex.quote for safety
            docker_cmd_parts = [
                "docker", "run", "--rm",
                f"--user=$(id -u):$(id -g)",
                f"-v {shlex.quote(host_input_dir)}:{container_input_dir}:ro",
                f"-v {shlex.quote(part_subdir_path)}:{container_output_dir}", # Mount part subdir
                f"-v {shlex.quote(host_script_dir)}:{container_script_dir}:ro",
                f"-w {container_output_dir}", # Work inside the mounted output dir
                shlex.quote(docker_image),
                "bash", container_script_path, container_input_path
            ]
            docker_run_command = " ".join(docker_cmd_parts)
            # Redirect Docker stdout on the HOST to the final count file
            command_to_run = f"{docker_run_command} > {shlex.quote(host_abs_count_output_file)}"
            # Run the docker command from the main task work dir to simplify paths?
            # Let's try running from the part_subdir_path to keep logic similar
            run_cwd = part_subdir_path

        else: # Host execution
            # Command runs script directly, redirecting output to count file
            command_to_run = f"bash {shlex.quote(host_script_path)} {shlex.quote(abs_file_path)} > {shlex.quote(host_abs_count_output_file)}"
            # Run directly within the part's subdir
            run_cwd = part_subdir_path

        # --- Execute Command ---
        current_count = 0 # Initialize count for this part
        try:
            print(f"  [Task Logic] run_word_count_on_list: Creating subdirectory '{part_subdir_path}' for input '{input_filename}'")
            os.makedirs(part_subdir_path, exist_ok=True)

            # Create host-side symlink to the input file within the part subdir
            _create_input_symlink(file_path, part_subdir_path, link_prefix="input_split")

            # Execute the command (Docker or Host)
            run_shell(
                command_to_run,
                cwd=run_cwd, # Use the determined CWD
                command_log_file=command_log_file_path
            )

            # Read the resulting count file
            print(f"  [Task Logic] run_word_count_on_list: Reading count from '{host_abs_count_output_file}'")
            with open(host_abs_count_output_file, 'r') as f_count:
                count_str = f_count.read().strip()
                if not count_str:
                     print(f"  [Task Logic] run_word_count_on_list: WARNING - Count file empty for '{input_filename}'. Assuming 0.")
                     current_count = 0
                else:
                    try:
                        current_count = int(count_str)
                    except ValueError:
                         print(f"  [Task Logic] run_word_count_on_list: WARNING - Invalid integer in count file for '{input_filename}': '{count_str}'. Assuming 0.")
                         current_count = 0

            print(f"  [Task Logic] run_word_count_on_list: Count for '{input_filename}' is {current_count}")

        except FileNotFoundError as e:
             # Catch potential errors finding input files or the script itself during run_shell
             print(f"  [Task Logic] run_word_count_on_list: ERROR - File not found during processing '{input_filename}': {e}. Assuming 0 count.")
             current_count = 0
        except subprocess.CalledProcessError as e:
             # Catch errors from the script/docker command execution
             print(f"  [Task Logic] run_word_count_on_list: ERROR - Command failed for '{input_filename}': {e}. Assuming 0 count.")
             current_count = 0
        except Exception as e:
             # Catch any other unexpected errors during this part's processing
             print(f"  [Task Logic] run_word_count_on_list: ERROR - Unexpected error for '{input_filename}': {e}. Assuming 0 count.")
             # import traceback; traceback.print_exc() # Optional detailed traceback
             current_count = 0

        # Append the count for this part (even if it's 0 due to errors)
        counts.append(current_count)
    # --- End of loop ---

    print(f"  [Task Logic] run_word_count_on_list: Finished processing all parts. Returning counts: {counts}")
    return counts


# --- Task 3: Sum Counts ---
def sum_counts(counts_list, final_output_filename, task_work_dir, config):
    """
    Sums a list of numbers and writes the result to the final output file
    specified in the configuration (global_params.output_dir).
    Input symlinks (for counts_list) are created by the core executor.
    Returns the absolute path to the final output file.
    """
    # Ensure counts_list is valid (list of numbers)
    if not isinstance(counts_list, list):
        print(f"  [Task Logic] sum_counts: ERROR - Input 'counts_list' is not a list: {type(counts_list)}")
        raise TypeError("Input to sum_counts must be a list.")
    # Ensure all items are numbers (or coerce if possible, safer to check)
    try:
        # Convert possible string numbers from file reads etc.
        numeric_counts = [int(c) for c in counts_list]
    except (ValueError, TypeError) as e:
        print(f"  [Task Logic] sum_counts: ERROR - Input list contains non-numeric values: {counts_list} ({e})")
        raise TypeError(f"Input list for sum_counts contained non-numeric values: {e}")

    print(f"  [Task Logic] sum_counts: Summing numeric list: {numeric_counts}")
    total = sum(numeric_counts)
    print(f"  [Task Logic] sum_counts: Total count is {total}")

    # Get final output directory and filename from config
    global_params = config.get("global_params", {})
    output_dir_rel = global_params.get("output_dir") # Path relative to original CWD
    if not output_dir_rel:
        print("  [Task Logic] sum_counts: ERROR - 'output_dir' not specified in global_params.")
        raise ValueError("'output_dir' not found in global_params")
    if not final_output_filename:
        print("  [Task Logic] sum_counts: ERROR - 'final_output_filename' parameter not provided.")
        raise ValueError("final_output_filename parameter missing")

    # Construct absolute path for the final output file
    # Assume output_dir_rel is relative to original CWD where pipeline was launched
    output_dir_abs = os.path.abspath(output_dir_rel)
    # Define final_output_path BEFORE the try block
    final_output_path = os.path.join(output_dir_abs, final_output_filename)

    try:
        # Ensure the final output directory exists
        print(f"  [Task Logic] sum_counts: Ensuring final output directory exists: {output_dir_abs}")
        os.makedirs(output_dir_abs, exist_ok=True)

        # Write the total count to the final output file
        print(f"  [Task Logic] sum_counts: Writing total count to {final_output_path}")
        with open(final_output_path, 'w') as f_out:
            f_out.write(str(total) + "\n")

        print(f"  [Task Logic] sum_counts: Successfully wrote final output.")

    except Exception as e:
        # Catch errors during directory creation or file writing
        print(f"  [Task Logic] sum_counts: ERROR writing final output file '{final_output_path}': {e}")
        # import traceback; traceback.print_exc() # Optional detailed traceback
        raise # Re-raise the exception to fail the task

    # Return the absolute path ONLY if successful
    return final_output_path