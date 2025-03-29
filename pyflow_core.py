# File: pyflow_core.py

import functools
import subprocess
import inspect
import os
import hashlib
import json
import concurrent.futures
import time
import traceback
from enum import Enum
import pickle
import shlex # For quoting paths safely

print("--- pyflow_core.py: Starting import ---")

# --- Task State Enum ---
class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    CANCELLED = 5

# --- Helper for Shell Commands ---
def run_shell(command, cwd=None, command_log_file=None):
    """
    Runs a shell command, raises error if it fails.
    Optionally saves the command to a log file.
    """
    # Log the command before execution
    log_entry = f"#!/bin/bash\n\n# --- Execution Environment ---\n# Host CWD: {os.path.abspath(cwd) if cwd else os.path.abspath('.')}\n# User: $(id -u):$(id -g)\n\n# --- Command ---\n{command}\n"
    print(f"  Executing in '{os.path.abspath(cwd) if cwd else '.'}': {command}") # Log absolute CWD

    if command_log_file:
        try:
            log_dir = os.path.dirname(command_log_file)
            # Ensure directory for log file exists (handle cwd case)
            if log_dir and not os.path.isabs(log_dir):
                 log_dir_abs = os.path.abspath(os.path.join(cwd or '.', log_dir))
            elif log_dir:
                 log_dir_abs = log_dir
            else: # log file is in cwd
                 log_dir_abs = os.path.abspath(cwd or '.')

            os.makedirs(log_dir_abs, exist_ok=True)
            abs_command_log_file = os.path.join(log_dir_abs, os.path.basename(command_log_file))

            with open(abs_command_log_file, 'w') as f_cmd:
                f_cmd.write(log_entry)
            os.chmod(abs_command_log_file, 0o755) # Make it executable
            print(f"  Command saved to: {abs_command_log_file}")
        except Exception as e:
            print(f"  Warning: Failed to save command to {command_log_file}: {e}")

    # Execute the command
    try:
        abs_cwd = os.path.abspath(cwd) if cwd else None
        if abs_cwd:
            os.makedirs(abs_cwd, exist_ok=True) # Ensure CWD exists

        result = subprocess.run(
            command,
            shell=True, # Needs shell=True for redirection, complex commands
            check=True,
            capture_output=True,
            text=True,
            cwd=abs_cwd, # Use absolute CWD
            # executable='/bin/bash' # Consider uncommenting for consistency
        )
        # Optionally print stdout/stderr, can be verbose
        # if result.stdout: print(f"  STDOUT:\n{result.stdout}")
        # if result.stderr: print(f"  STDERR:\n{result.stderr}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT:\n{e.stdout}")
        print(f"  STDERR:\n{e.stderr}")
        raise # Re-raise the exception to stop the workflow

# --- Task Output Placeholder ---
class TaskOutput:
    """Represents the future output of a task call."""
    def __init__(self, workflow, task_func, call_args, call_kwargs):
        self.workflow = workflow # Reference to parent workflow
        self.task_func = task_func # The actual user function object
        self.call_args = tuple(call_args) # Ensure hashable args
        self.call_kwargs = tuple(sorted(call_kwargs.items())) # Ensure hashable kwargs
        self.id = self._generate_id()

    def _generate_id(self):
        """Creates a unique ID for a specific task call."""
        try:
            # Prepare items for hashing, using IDs for nested TaskOutputs
            def prep_for_hash(item):
                if isinstance(item, TaskOutput):
                    return f"<TaskOutput:{item.id}>" # Use ID string representation
                # Basic check for unhashable types that json can handle
                if isinstance(item, (list, dict)):
                     try:
                         # Sort dicts for consistency
                         return json.dumps(item, sort_keys=True)
                     except TypeError:
                         return str(item) # Fallback for complex unhashable items in list/dict
                # Attempt to handle other potentially unhashable types
                try:
                     hash(item)
                     return item
                except TypeError:
                     return str(item)

            args_for_hash = tuple(prep_for_hash(a) for a in self.call_args)
            kwargs_for_hash = tuple((k, prep_for_hash(v)) for k, v in self.call_kwargs)

            # Use JSON dumps for serialization robustness if possible
            try:
                 arg_string = json.dumps((args_for_hash, kwargs_for_hash), sort_keys=True)
            except TypeError:
                 # Fallback to string representation if JSON fails
                 arg_string = str((args_for_hash, kwargs_for_hash))

        except Exception as e:
             # Catch-all during hashing generation - less robust ID
             print(f"Warning: Error generating hash components for {self.task_func.__name__}: {e}. Using fallback ID.")
             arg_string = str((self.call_args, self.call_kwargs))

        # Combine function name and serialized args for final ID string
        id_string = f"{self.task_func.__name__}:{arg_string}"
        return hashlib.md5(id_string.encode('utf-8', errors='replace')).hexdigest()[:10] # 10-char hash


    def get_dependencies(self):
        """Find TaskOutput instances within the call arguments."""
        deps = set()
        # Check positional arguments
        for arg in self.call_args:
            if isinstance(arg, TaskOutput):
                deps.add(arg.id)
            elif isinstance(arg, (list, tuple)): # Check inside lists/tuples
                 for item in arg:
                      if isinstance(item, TaskOutput):
                          deps.add(item.id)
            # Could add dict checks too if needed

        # Check keyword arguments
        for _, value in self.call_kwargs:
            if isinstance(value, TaskOutput):
                deps.add(value.id)
            elif isinstance(value, (list, tuple)): # Check inside lists/tuples
                 for item in value:
                      if isinstance(item, TaskOutput):
                          deps.add(item.id)
            # Could add dict checks too if needed
        return deps

    def __repr__(self):
        return f"<TaskOutput of {self.task_func.__name__} (ID: {self.id})>"

    # Need hash and eq for using TaskOutput in sets/dicts potentially
    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, TaskOutput) and self.id == other.id

# --- Input Symlink Helper ---
def _create_input_symlink(source_path, link_dir, link_prefix="input"):
    """Helper function to create a symlink for an input file/dir."""
    # Ensure source is a string representing a path
    if not isinstance(source_path, str):
        return # Skip non-string inputs

    # Use lexists for symlinks, exists for regular files/dirs
    if not os.path.lexists(source_path):
        # print(f"  [Linker] Skipping non-existent source path: {source_path}")
        return # Skip things that aren't existing paths

    try:
        # Make paths absolute for robustness
        abs_source_path = os.path.abspath(source_path)
        abs_link_dir = os.path.abspath(link_dir)

        # Create a safe basename for the link (replace slashes etc.)
        link_basename = os.path.basename(abs_source_path) or "source"
        safe_basename = "".join(c if c.isalnum() or c in ('-', '_', '.') else '_' for c in link_basename)
        link_name = os.path.join(abs_link_dir, f"{link_prefix}_{safe_basename}")

        # Avoid creating link loops (linking work dir inside itself)
        if abs_source_path.startswith(abs_link_dir) and abs_source_path != abs_link_dir:
             print(f"  [Linker] Warning: Skipping potentially recursive link for {abs_source_path} in {abs_link_dir}")
             return

        # Ensure target directory exists
        os.makedirs(abs_link_dir, exist_ok=True)
        # Create symlink - remove existing if it's already there (e.g., from retry)
        if os.path.lexists(link_name):
             # print(f"  [Linker] Removing existing link: {link_name}")
             os.remove(link_name)

        os.symlink(abs_source_path, link_name)
        # print(f"  [Linker] Linked input: '{link_name}' -> '{abs_source_path}'") # Verbose

    except Exception as e:
        print(f"  [Linker] Warning: Failed to link input '{source_path}' to '{link_name}': {e}")


# --- Top-Level Function for Executor ---
# Must be defined at module level for pickling.
def _run_task_in_process(user_func, task_id, func_name, args, kwargs, work_dir, config):
    """
    Executes the user's task function in a separate process.
    Creates input symlinks before running.
    """
    abs_work_dir = os.path.abspath(work_dir) # Ensure work_dir is absolute
    print(f"  [Executor PID {os.getpid()}] Preparing task: {func_name} (ID: {task_id})")
    print(f"  [Executor PID {os.getpid()}] Working directory: {abs_work_dir}")
    os.makedirs(abs_work_dir, exist_ok=True)

    # --- Create Input Symlinks ---
    print(f"  [Executor PID {os.getpid()}] Creating input symlinks in {abs_work_dir}...")
    # Link positional arguments
    for i, arg in enumerate(args):
        if isinstance(arg, (list, tuple)):
            for item_idx, item in enumerate(arg):
                 _create_input_symlink(item, abs_work_dir, link_prefix=f"input_arg{i}_item{item_idx}")
        else:
            _create_input_symlink(arg, abs_work_dir, link_prefix=f"input_arg{i}")
    # Link keyword arguments
    for key, value in kwargs.items():
         if isinstance(value, (list, tuple)):
             for item_idx, item in enumerate(value):
                 _create_input_symlink(item, abs_work_dir, link_prefix=f"input_{key}_item{item_idx}")
         else:
            _create_input_symlink(value, abs_work_dir, link_prefix=f"input_{key}")
    print(f"  [Executor PID {os.getpid()}] Input symlinking complete.")


    # Inject task_work_dir (absolute path) and config if function expects them
    final_kwargs = dict(kwargs)
    sig = inspect.signature(user_func)
    if "task_work_dir" in sig.parameters:
        final_kwargs["task_work_dir"] = abs_work_dir
    if "config" in sig.parameters:
        final_kwargs["config"] = config

    print(f"  [Executor PID {os.getpid()}] Executing task function: {func_name} (ID: {task_id})")
    try:
        # CALL THE ACTUAL USER FUNCTION
        result = user_func(*args, **final_kwargs)
        print(f"  [Executor PID {os.getpid()}] Finished task: {func_name} (ID: {task_id}) Result: {result}")
        return result
    except Exception as e:
        print(f"  [Executor PID {os.getpid()}] FAILED task: {func_name} (ID: {task_id})")
        tb_str = traceback.format_exc()
        # Raise a new exception containing the original traceback string
        raise RuntimeError(f"Task {func_name} (ID: {task_id}) failed in executor process.\nTraceback:\n{tb_str}") from e


# --- Workflow Class ---
print("--- pyflow_core.py: Defining Workflow class ---")
class Workflow:
    """Manages task registration, dependencies, and execution."""
    def __init__(self, work_dir="_pyflow_work", max_workers=None):
        """
        Initializes the workflow.
        Args:
            work_dir (str): The base directory for intermediate files.
                            Will be created if it doesn't exist. Made absolute.
            max_workers (int, optional): Max parallel processes. Defaults to CPU count.
        """
        print(f"--- pyflow_core.py: Workflow.__init__ called ---")
        self.task_registry = {}
        # Ensure work_dir is absolute and stored
        self._work_dir = os.path.abspath(work_dir)
        self.max_workers = max_workers or os.cpu_count()
        # Config is loaded externally and assigned to self.config later
        self.config = {}

        # Execution State (reset before each run)
        self.task_calls = {} # id -> TaskOutput object (stores definition)
        self.task_results = {} # id -> actual result (path, value, etc.)
        self.task_status = {} # id -> TaskStatus enum
        self.task_dependencies = {} # id -> set(dependency_ids)
        self.task_dependents = {} # id -> set(dependent_ids)

        print(f"Workflow initialized: work_dir='{self._work_dir}', max_workers={self.max_workers}")
        print(f"--- pyflow_core.py: Workflow.__init__ finished ---")


    def _load_config(self, config_file):
        """Loads configuration from a JSON file (helper method)."""
        # print("--- pyflow_core.py: Workflow._load_config called ---") # Debug
        if config_file:
            abs_config_path = os.path.abspath(config_file)
            if os.path.exists(abs_config_path):
                with open(abs_config_path, 'r') as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from {abs_config_path}: {e}")
                        raise
            else:
                raise FileNotFoundError(f"Config file not found: {abs_config_path}")
        return {} # Return empty dict if no config file specified


    def task(self, func):
        """Decorator to register a task function and create TaskOutput placeholders."""
        # print(f"--- pyflow_core.py: Workflow.task decorator registering: {func.__name__} ---") # Debug
        if func.__name__ not in self.task_registry:
             self.task_registry[func.__name__] = func
        # else: # Optional: Handle re-registration warning/error
             # print(f"Warning: Task '{func.__name__}' already registered.")

        @functools.wraps(func) # Preserves metadata
        def wrapper(*args, **kwargs):
            """Creates and returns a TaskOutput object when the decorated function is called."""
            # print(f"--- pyflow_core.py: Wrapper called for {func.__name__} ---") # Debug
            task_output = TaskOutput(self, func, args, kwargs)
            # Store the definition of this call for later lookup by the runner
            if task_output.id not in self.task_calls:
                 self.task_calls[task_output.id] = task_output
            return task_output # Return the placeholder

        # The decorator returns the wrapper function
        return wrapper


    def _build_dag(self, final_target_id):
        """Builds dependency graph (task_dependencies, task_dependents)."""
        # print("--- pyflow_core.py: Workflow._build_dag called ---") # Debug
        queue = [final_target_id]
        visited = set()
        processed_deps = set() # Track dependencies added to queue

        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            if current_id not in self.task_calls:
                 # This ID doesn't correspond to a defined task call
                 # Could be the initial target if it wasn't a TaskOutput, or an error
                 # print(f"Warning: ID {current_id} in DAG is not a known task call.")
                 continue

            task_output = self.task_calls[current_id]
            deps = task_output.get_dependencies()
            self.task_dependencies[current_id] = deps
            self.task_status[current_id] = TaskStatus.PENDING # Initialize

            for dep_id in deps:
                # Build reverse dependencies (dependents)
                if dep_id not in self.task_dependents:
                    self.task_dependents[dep_id] = set()
                self.task_dependents[dep_id].add(current_id)
                # Add dependency to queue if not already processed/visited
                if dep_id not in visited and dep_id not in processed_deps:
                    if dep_id in self.task_calls: # Only add known tasks
                         queue.append(dep_id)
                         processed_deps.add(dep_id)
                    # else: # Optional: Warn about missing dependency definitions
                         # print(f"Warning: Dependency {dep_id} for task {current_id} not defined via @task.")


    def run(self, final_task_output):
        """Runs the workflow to produce the final_task_output."""
        print("--- pyflow_core.py: Workflow.run called ---")
        if not isinstance(final_task_output, TaskOutput):
            print("Final target is not a TaskOutput. Nothing to run.")
            return final_task_output
        if final_task_output.workflow is not self:
            raise ValueError("TaskOutput belongs to a different Workflow instance.")

        # --- Reset state for this specific run ---
        self.task_results = {}
        self.task_status = {}
        self.task_dependencies = {}
        self.task_dependents = {}
        # Ensure the main workflow work directory exists
        os.makedirs(self._work_dir, exist_ok=True)

        target_id = final_task_output.id
        print(f"\n--- Building Workflow DAG for target: {target_id} ---")
        self._build_dag(target_id)
        required_tasks = {tid for tid in self.task_status if tid in self.task_calls}
        if not required_tasks:
             print("No runnable tasks found in the DAG leading to the target.")
             return None # Or raise error?

        print(f"Tasks involved: {len(required_tasks)}")
        print(f"\n--- Starting Parallel Execution (max_workers={self.max_workers}) ---")
        start_time = time.time()
        tasks_failed = set()
        tasks_completed = set()
        active_futures = {} # future -> task_id mapping

        # Using ProcessPoolExecutor for better CPU/external call parallelism
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            while len(tasks_completed) + len(tasks_failed) < len(required_tasks):
                ready_to_submit = []
                # Find tasks ready to run (PENDING and all deps COMPLETED)
                for task_id in list(required_tasks - tasks_completed - tasks_failed):
                    if self.task_status.get(task_id) == TaskStatus.PENDING:
                        deps = self.task_dependencies.get(task_id, set())
                        # Check if all dependencies are in task_results (meaning completed)
                        if all(dep_id in self.task_results for dep_id in deps):
                             ready_to_submit.append(task_id)
                        # elif not deps: # Handle tasks with no dependencies
                        #     ready_to_submit.append(task_id)

                # Submit ready tasks to the executor
                for task_id in ready_to_submit:
                    task_output = self.task_calls[task_id]
                    func = task_output.task_func; func_name = func.__name__
                    args = task_output.call_args; kwargs = dict(task_output.call_kwargs)

                    # Resolve dependencies using results from completed tasks
                    resolved_args = []; resolved_kwargs = {}
                    try:
                        for arg in args: resolved_args.append(self.task_results[arg.id] if isinstance(arg, TaskOutput) else arg)
                        for key, value in kwargs.items(): resolved_kwargs[key] = self.task_results[value.id] if isinstance(value, TaskOutput) else value
                    except KeyError as e:
                         print(f"Internal Error: Dependency result key missing for {task_id}. Key: {e}")
                         self.task_status[task_id] = TaskStatus.FAILED; tasks_failed.add(task_id); continue # Skip submission

                    # Define the specific work directory for this task execution (absolute path)
                    task_work_dir = os.path.join(self._work_dir, func_name, task_id)

                    print(f"Submitting task: {func_name} (ID: {task_id})")
                    self.task_status[task_id] = TaskStatus.RUNNING

                    # Submit the top-level executor function with all necessary data
                    future = executor.submit(
                        _run_task_in_process, # The pickleable function
                        user_func=func, task_id=task_id, func_name=func_name,
                        args=tuple(resolved_args), kwargs=resolved_kwargs,
                        work_dir=task_work_dir, # Pass absolute task work dir
                        config=self.config # Pass the workflow's config object
                    )
                    active_futures[future] = task_id # Map future back to ID

                # --- Wait for and process completed futures ---
                if not active_futures and len(tasks_completed) + len(tasks_failed) < len(required_tasks):
                    # Check if workflow should be finished or if there's a deadlock/error
                    if (len(tasks_completed) + len(tasks_failed)) != len(required_tasks):
                         print("Warning: No tasks running, but workflow not complete. Check DAG/errors.")
                         # Log pending tasks for debugging
                         for tid in required_tasks - tasks_completed - tasks_failed:
                             if self.task_status.get(tid) == TaskStatus.PENDING:
                                 unmet = [dep for dep in self.task_dependencies.get(tid, set()) if dep not in self.task_results]
                                 print(f"  - Task {tid} ({self.task_calls[tid].task_func.__name__}) pending. Unmet deps: {unmet}")
                    break # Exit loop if nothing is running
                if not active_futures: break # Exit loop if nothing to wait for

                # Wait for at least one task to complete, fail, or be cancelled
                done, _ = concurrent.futures.wait(
                    active_futures.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Process the futures that have completed
                for future in done:
                    task_id = active_futures.pop(future) # Get ID and remove from active
                    task_name = self.task_calls[task_id].task_func.__name__
                    try:
                        result = future.result() # Get result or raise exception from worker
                        self.task_results[task_id] = result
                        self.task_status[task_id] = TaskStatus.COMPLETED
                        tasks_completed.add(task_id)
                        print(f"Task Completed: {task_name} (ID: {task_id})")

                    except Exception as e:
                        # Task failed in the executor process
                        self.task_status[task_id] = TaskStatus.FAILED
                        tasks_failed.add(task_id)
                        print(f"!!! Task FAILED: {task_name} (ID: {task_id}) !!!")
                        # Error 'e' should include traceback string from _run_task_in_process
                        print(f"  Error: {e}")

                        # --- Failure Propagation: Cancel downstream tasks ---
                        cancel_queue = list(self.task_dependents.get(task_id, set()))
                        visited_cancel = {task_id}
                        while cancel_queue:
                            dependent_id = cancel_queue.pop(0)
                            if dependent_id in visited_cancel: continue
                            visited_cancel.add(dependent_id)

                            if dependent_id in self.task_calls: # Ensure it's a known task
                                # Only cancel PENDING tasks
                                if self.task_status.get(dependent_id) == TaskStatus.PENDING:
                                    self.task_status[dependent_id] = TaskStatus.CANCELLED
                                    # Add cancelled to failed set to ensure loop terminates correctly
                                    tasks_failed.add(dependent_id)
                                    print(f"  -> Cancelling downstream: {self.task_calls[dependent_id].task_func.__name__} (ID: {dependent_id})")
                                    # Add its dependents to the queue for recursive cancellation
                                    for next_dep_id in self.task_dependents.get(dependent_id, set()):
                                          if next_dep_id not in visited_cancel:
                                              cancel_queue.append(next_dep_id)
                            # else: # Optional: Warn about cancelling unknown ID
                                # print(f"Warning: Attempting to cancel unknown downstream ID: {dependent_id}")

                # time.sleep(0.05) # Small optional sleep

        # --- End of Execution ---
        end_time = time.time()
        print(f"\n--- Workflow Execution Summary ---")
        print(f"Total time: {end_time - start_time:.2f} seconds")
        print(f"Tasks Completed: {len(tasks_completed)}")
        print(f"Tasks Failed/Cancelled: {len(tasks_failed)}")

        # Check final status and return result or raise error
        if target_id in tasks_failed or self.task_status.get(target_id) != TaskStatus.COMPLETED:
             print("\n!!! Workflow did not complete successfully !!!")
             # Log failed/cancelled tasks for clarity
             for tid in sorted(list(tasks_failed)):
                 if tid in self.task_calls:
                     status = self.task_status.get(tid, "UNKNOWN")
                     print(f"  - {self.task_calls[tid].task_func.__name__} (ID: {tid}): {status.name}")
                 else: print(f"  - Unknown Task (ID: {tid}): FAILED/CANCELLED")
             raise RuntimeError("Workflow execution failed.")
        else:
             print("\n--- Workflow finished successfully ---")
             # Return the result associated with the final target task ID
             return self.task_results.get(target_id)


    def cleanup(self):
        """Removes the main workflow working directory."""
        import shutil
        # Use the absolute path stored during init
        if os.path.exists(self._work_dir):
            print(f"Cleaning up working directory: {self._work_dir}")
            try:
                shutil.rmtree(self._work_dir)
            except OSError as e:
                print(f"Warning: Error removing work directory '{self._work_dir}': {e}")
        else:
            print(f"Working directory not found, skipping cleanup: {self._work_dir}")

print("--- pyflow_core.py: Workflow class defined ---")
print("--- pyflow_core.py: End of file ---")