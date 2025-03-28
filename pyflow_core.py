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
import pickle # Added for potential complex object handling if needed later

# --- Task State Enum ---
# (Same as before)
class TaskStatus(Enum):
    PENDING = 1
    RUNNING = 2
    COMPLETED = 3
    FAILED = 4
    CANCELLED = 5

# --- Helper for Shell Commands ---
# (Same as before)
def run_shell(command, cwd=None):
    """Runs a shell command, raises error if it fails."""
    print(f"  Executing in '{cwd or '.'}': {command}")
    try:
        if cwd:
            os.makedirs(cwd, exist_ok=True)
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True, cwd=cwd
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT:\n{e.stdout}")
        print(f"  STDERR:\n{e.stderr}")
        raise

# --- Task Output Placeholder ---
# (Mostly same, workflow reference less critical now for execution)
class TaskOutput:
    """Represents the future output of a task call."""
    def __init__(self, workflow, task_func, call_args, call_kwargs):
        self.workflow = workflow # Still useful for definition phase
        self.task_func = task_func # The actual user function object
        self.call_args = tuple(call_args)
        self.call_kwargs = tuple(sorted(call_kwargs.items()))
        self.id = self._generate_id()

    def _generate_id(self):
        """Creates a unique ID for a specific task call."""
        try:
            def prep_for_hash(item):
                if isinstance(item, TaskOutput):
                    return item.id
                # Basic check for unhashable types that json can handle
                if isinstance(item, (list, dict)):
                     return json.dumps(item, sort_keys=True)
                return item

            args_for_hash = tuple(prep_for_hash(a) for a in self.call_args)
            kwargs_for_hash = tuple((k, prep_for_hash(v)) for k, v in self.call_kwargs)
            arg_string = json.dumps((args_for_hash, kwargs_for_hash), sort_keys=True)
        except TypeError:
             arg_string = str((args_for_hash, kwargs_for_hash))

        id_string = f"{self.task_func.__name__}:{arg_string}"
        return hashlib.md5(id_string.encode()).hexdigest()[:10]

    def get_dependencies(self):
        """Find TaskOutput instances within the call arguments."""
        deps = set()
        for arg in self.call_args:
            if isinstance(arg, TaskOutput):
                deps.add(arg.id)
        for _, value in self.call_kwargs:
            if isinstance(value, TaskOutput):
                deps.add(value.id)
        return deps

    def __repr__(self):
        return f"<TaskOutput of {self.task_func.__name__} (ID: {self.id})>"

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, TaskOutput) and self.id == other.id


# ****** NEW: Top-Level Function for Executor ******
# This function MUST be defined at the module level so pickle can find it.
def _run_task_in_process(user_func, task_id, func_name, args, kwargs, work_dir, config):
    """
    Executes the user's task function in a separate process.
    Receives all necessary data directly, avoiding reliance on Workflow state.
    """
    print(f"  [Executor PID {os.getpid()}] Preparing task: {func_name} (ID: {task_id})")
    print(f"  [Executor PID {os.getpid()}] Working directory: {work_dir}")
    os.makedirs(work_dir, exist_ok=True) # Ensure dir exists

    # Inject task_work_dir and config if function expects them
    final_kwargs = dict(kwargs) # Copy original kwargs
    sig = inspect.signature(user_func)
    if "task_work_dir" in sig.parameters:
        final_kwargs["task_work_dir"] = work_dir
    if "config" in sig.parameters:
        final_kwargs["config"] = config # Pass the config dictionary

    print(f"  [Executor PID {os.getpid()}] Executing task: {func_name} (ID: {task_id}) with args={args}, kwargs={final_kwargs}")
    try:
        # CALL THE ACTUAL USER FUNCTION with resolved arguments
        result = user_func(*args, **final_kwargs)
        print(f"  [Executor PID {os.getpid()}] Finished task: {func_name} (ID: {task_id}) Result: {result}")
        # Attempt to pickle result - can fail for complex non-pickleable objects
        # try:
        #      pickle.dumps(result)
        # except Exception as pickle_err:
        #      print(f"Warning: Result of task {func_name} (ID: {task_id}) might not be pickleable: {pickle_err}")
        #      # Decide how to handle: convert to string? return None? raise specific error?
        #      # For now, let the ProcessPoolExecutor handle the potential error during result transfer
        return result
    except Exception as e:
        print(f"  [Executor PID {os.getpid()}] FAILED task: {func_name} (ID: {task_id})")
        tb_str = traceback.format_exc()
        # Raise a new exception containing the original traceback string
        # This helps ensure the error information propagates back reliably
        raise RuntimeError(f"Task {func_name} (ID: {task_id}) failed in executor process.\nTraceback:\n{tb_str}") from e


# --- Workflow Class ---
class Workflow:
    def __init__(self, work_dir="_pyflow_work", max_workers=None, config_file=None):
        # ... (init registry, work_dir, max_workers, config - same as before) ...
        self.task_registry = {}
        self._work_dir = work_dir
        self.max_workers = max_workers or os.cpu_count()
        self.config = self._load_config(config_file)

        # Execution State (reset before each run)
        self.task_calls = {} # id -> TaskOutput object
        self.task_results = {} # id -> actual result (path, value, etc.)
        self.task_status = {} # id -> TaskStatus
        # self.task_futures = {} # No longer needed to map future -> id directly here
        self.task_dependencies = {} # id -> set(dependency_ids)
        self.task_dependents = {} # id -> set(dependent_ids)

        print(f"Workflow initialized: work_dir='{self._work_dir}', max_workers={self.max_workers}")
        if self.config:
             print(f"Loaded config from '{config_file}'")


    # _load_config method remains the same
    def _load_config(self, config_file):
        """Loads configuration from a JSON file."""
        if config_file:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    try:
                        return json.load(f)
                    except json.JSONDecodeError as e:
                        print(f"Error decoding JSON from {config_file}: {e}")
                        raise
            else:
                raise FileNotFoundError(f"Config file not found: {config_file}")
        return {} # Return empty dict if no config file specified


    # task decorator remains the same
    def task(self, func):
        """Decorator to register a task with this workflow instance."""
        if func.__name__ in self.task_registry:
            print(f"Warning: Task '{func.__name__}' already registered. Overwriting.")
        self.task_registry[func.__name__] = func

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            task_output = TaskOutput(self, func, args, kwargs)
            if task_output.id not in self.task_calls:
                 self.task_calls[task_output.id] = task_output
            # Add task to registry if not already there (idempotent)
            if func.__name__ not in self.task_registry:
                 self.task_registry[func.__name__] = func
            return task_output
        return wrapper

    # _execute_task method is REMOVED.

    # _build_dag method remains the same
    def _build_dag(self, final_target_id):
        """Traverse dependencies from the target to build the DAG."""
        queue = [final_target_id]
        visited = set()
        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)

            if current_id not in self.task_calls:
                 # Handle case where a non-TaskOutput might be part of the graph (e.g., initial value)
                 # Or if the target itself wasn't a TaskOutput initially passed to run()
                 if current_id == final_target_id and not isinstance(final_target_id, TaskOutput):
                     print(f"Target {final_target_id} is not a task output.")
                     # Decide if this is an error or just means no tasks need to run
                 else:
                     # This shouldn't normally happen if all dependencies are TaskOutputs
                     print(f"Warning: ID {current_id} required by DAG but not found in task calls.")
                 continue # Skip nodes that aren't defined tasks

            task_output = self.task_calls[current_id]
            deps = task_output.get_dependencies()
            self.task_dependencies[current_id] = deps
            self.task_status[current_id] = TaskStatus.PENDING # Initialize status

            for dep_id in deps:
                if dep_id not in self.task_dependents:
                    self.task_dependents[dep_id] = set()
                self.task_dependents[dep_id].add(current_id)
                if dep_id not in visited:
                    # Ensure the dependency itself is a task we know about
                    if dep_id in self.task_calls and dep_id not in queue:
                         queue.append(dep_id)
                    elif dep_id not in self.task_calls:
                         # This dependency refers to something not defined via @task
                         print(f"Warning: Dependency ID {dep_id} needed by {current_id} not found in defined task calls.")


    def run(self, final_task_output):
        """Runs the workflow required to produce the final_task_output using parallelism."""
        if not isinstance(final_task_output, TaskOutput):
            print("Final target is not a TaskOutput. Nothing to run.")
            return final_task_output
        if final_task_output.workflow is not self:
            raise ValueError("TaskOutput belongs to a different Workflow instance.")

        # --- Reset state for this run ---
        self.task_results = {}
        self.task_status = {}
        # self.task_futures = {} # Removed
        self.task_dependencies = {}
        self.task_dependents = {}
        os.makedirs(self._work_dir, exist_ok=True)

        target_id = final_task_output.id
        print(f"\n--- Building Workflow DAG for target: {target_id} ---")
        self._build_dag(target_id)
        # Filter out any IDs that aren't actually tasks (could happen with warnings above)
        required_tasks = {tid for tid in self.task_status if tid in self.task_calls}
        if not required_tasks:
             print("No tasks found in the DAG leading to the target.")
             # Need to decide what to return - maybe the target if it wasn't a TaskOutput?
             # For now, assume target requires tasks if it's a TaskOutput.
             if target_id in self.task_calls:
                 print("Error: Target is a task but no DAG path found?") # Should not happen if _build_dag is correct
                 raise RuntimeError("Internal DAG building error.")
             else:
                  return final_task_output # Target was likely a primitive value

        print(f"Tasks involved: {len(required_tasks)}")

        print(f"\n--- Starting Parallel Execution (max_workers={self.max_workers}) ---")
        start_time = time.time()
        tasks_failed = set()
        tasks_completed = set()
        active_futures = {} # future -> task_id (map from future back to task)

        # Use ProcessPoolExecutor for CPU-bound or external calls
        # Use 'spawn' context if needed on macOS/Windows for compatibility, though default usually works
        # import multiprocessing
        # context = multiprocessing.get_context('spawn')
        # with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers, mp_context=context) as executor:
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:

            while len(tasks_completed) + len(tasks_failed) < len(required_tasks):
                ready_to_submit = []
                # Find tasks ready to run
                for task_id in list(required_tasks - tasks_completed - tasks_failed):
                    if self.task_status.get(task_id) == TaskStatus.PENDING:
                        deps = self.task_dependencies.get(task_id, set())
                        # Ensure all dependencies exist in results (are completed)
                        if all(dep_id in self.task_results for dep_id in deps):
                             ready_to_submit.append(task_id)

                # Submit ready tasks
                for task_id in ready_to_submit:
                    task_output = self.task_calls[task_id]
                    func = task_output.task_func # The actual user function object
                    func_name = func.__name__
                    args = task_output.call_args
                    kwargs = dict(task_output.call_kwargs) # Need dict for processing

                    # Resolve dependencies using already computed results
                    resolved_args = []
                    try:
                        for arg in args:
                            if isinstance(arg, TaskOutput):
                                resolved_args.append(self.task_results[arg.id])
                            else:
                                resolved_args.append(arg)
                    except KeyError as e:
                         print(f"Error: Dependency result missing for task {task_id} -> {func_name}. Missing key: {e}")
                         # This indicates a logic error in dependency tracking or completion status
                         self.task_status[task_id] = TaskStatus.FAILED
                         tasks_failed.add(task_id)
                         # Need to decide how to handle this - stop workflow? Continue?
                         # For now, mark as failed and let failure propagation handle downstream
                         continue # Don't submit this task

                    resolved_kwargs = {}
                    try:
                        for key, value in kwargs.items():
                            if isinstance(value, TaskOutput):
                                resolved_kwargs[key] = self.task_results[value.id]
                            else:
                                resolved_kwargs[key] = value
                    except KeyError as e:
                         print(f"Error: Dependency result missing for task {task_id} -> {func_name}. Missing key: {e}")
                         self.task_status[task_id] = TaskStatus.FAILED
                         tasks_failed.add(task_id)
                         continue

                    # Define the specific work directory for this task execution
                    task_work_dir = os.path.join(self._work_dir, func_name, task_id)

                    print(f"Submitting task: {func_name} (ID: {task_id})")
                    self.task_status[task_id] = TaskStatus.RUNNING

                    # Submit the NEW top-level execution function
                    future = executor.submit(
                        _run_task_in_process, # The pickleable top-level function
                        user_func=func,       # The user's task function object
                        task_id=task_id,
                        func_name=func_name,
                        args=tuple(resolved_args), # Pass resolved args directly
                        kwargs=resolved_kwargs,    # Pass resolved kwargs directly
                        work_dir=task_work_dir,    # Pass specific work dir
                        config=self.config         # Pass config dict
                    )
                    active_futures[future] = task_id # Map future back to task_id

                # --- Wait for results (same logic as before) ---
                if not active_futures and len(tasks_completed) + len(tasks_failed) < len(required_tasks):
                    # Check if maybe some tasks failed to submit due to missing results
                    # Or if the loop termination condition is met
                    all_tasks_accounted_for = (len(tasks_completed) + len(tasks_failed)) == len(required_tasks)
                    if not all_tasks_accounted_for:
                        print("Warning: No tasks running, but workflow not complete. Check for errors.")
                        # Log pending tasks and their unmet dependencies
                        for tid in required_tasks - tasks_completed - tasks_failed:
                             if self.task_status.get(tid) == TaskStatus.PENDING:
                                 unmet_deps = [dep for dep in self.task_dependencies.get(tid, set()) if dep not in self.task_results]
                                 print(f"  - Task {tid} ({self.task_calls[tid].task_func.__name__}) pending. Unmet deps: {unmet_deps}")
                    break # Avoid busy loop

                if not active_futures:
                    break # Exit outer loop if nothing more is running

                # Wait for at least one future to complete
                done, _ = concurrent.futures.wait(
                    active_futures.keys(),
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Process completed futures
                for future in done:
                    task_id = active_futures.pop(future) # Remove from active map
                    task_name = self.task_calls[task_id].task_func.__name__
                    try:
                        result = future.result() # Get result or raise exception from worker process
                        self.task_results[task_id] = result
                        self.task_status[task_id] = TaskStatus.COMPLETED
                        tasks_completed.add(task_id)
                        print(f"Task Completed: {task_name} (ID: {task_id})")

                    except Exception as e:
                        self.task_status[task_id] = TaskStatus.FAILED
                        tasks_failed.add(task_id)
                        print(f"!!! Task FAILED: {task_name} (ID: {task_id}) !!!")
                        # Error message 'e' should now include the traceback from the executor process
                        print(f"  Error: {e}")

                        # --- Failure Propagation (same logic as before) ---
                        cancel_queue = list(self.task_dependents.get(task_id, set()))
                        visited_cancel = {task_id} # Start with the failed task
                        while cancel_queue:
                            dependent_id = cancel_queue.pop(0)
                            if dependent_id in visited_cancel: continue
                            visited_cancel.add(dependent_id)

                            if dependent_id in self.task_calls: # Ensure it's a known task
                                # Only cancel PENDING tasks
                                if self.task_status.get(dependent_id) == TaskStatus.PENDING:
                                    self.task_status[dependent_id] = TaskStatus.CANCELLED
                                    tasks_failed.add(dependent_id) # Count cancelled as failed for completion
                                    print(f"  -> Cancelling downstream: {self.task_calls[dependent_id].task_func.__name__} (ID: {dependent_id})")
                                    # Add its dependents to the queue
                                    for next_dep_id in self.task_dependents.get(dependent_id, set()):
                                          if next_dep_id not in visited_cancel:
                                              cancel_queue.append(next_dep_id)
                            else:
                                print(f"Warning: Trying to cancel unknown downstream ID: {dependent_id}")


        # --- End of Execution (Summary) ---
        # (Same summary logic as before)
        end_time = time.time()
        print(f"\n--- Workflow Execution Summary ---")
        print(f"Total time: {end_time - start_time:.2f} seconds")
        print(f"Tasks Completed: {len(tasks_completed)}")
        print(f"Tasks Failed/Cancelled: {len(tasks_failed)}")

        if target_id in tasks_failed or self.task_status.get(target_id) != TaskStatus.COMPLETED:
             print("\n!!! Workflow did not complete successfully !!!")
             for tid in sorted(list(tasks_failed)): # Sort for consistent output
                 if tid in self.task_calls: # Check if it's a known task
                     status = self.task_status.get(tid, "UNKNOWN")
                     print(f"  - {self.task_calls[tid].task_func.__name__} (ID: {tid}): {status.name}")
                 else:
                     print(f"  - Unknown Task (ID: {tid}): FAILED/CANCELLED")

             # Optionally print results dict for debugging
             # print("\nTask Results:")
             # for tid, res in self.task_results.items():
             #     print(f"  - {tid}: {res}")

             raise RuntimeError("Workflow execution failed.")
        else:
             print("\n--- Workflow finished successfully ---")
             return self.task_results.get(target_id) # Return the final result

    # cleanup method remains the same
    def cleanup(self):
        """Removes the working directory."""
        import shutil
        if os.path.exists(self._work_dir):
            print(f"Cleaning up working directory: {self._work_dir}")
            shutil.rmtree(self._work_dir)