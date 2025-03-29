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
import shlex # Import shlex for safer command construction/logging

# --- Task State Enum (remains the same) ---
class TaskStatus(Enum): PENDING = 1; RUNNING = 2; COMPLETED = 3; FAILED = 4; CANCELLED = 5

# --- MODIFIED run_shell Helper ---
def run_shell(command, cwd=None, command_log_file=None):
    """
    Runs a shell command, raises error if it fails.
    Optionally saves the command to a log file.
    """
    # Log the command before execution
    log_entry = f"#!/bin/bash\n# CWD: {os.path.abspath(cwd) if cwd else os.path.abspath('.')}\n\n{command}\n"
    print(f"  Executing in '{cwd or '.'}': {command}") # Keep console log

    if command_log_file:
        try:
            # Ensure directory for log file exists
            log_dir = os.path.dirname(command_log_file)
            if log_dir: # Avoid error if path is just filename in CWD
                 os.makedirs(log_dir, exist_ok=True)
            with open(command_log_file, 'w') as f_cmd:
                f_cmd.write(log_entry)
            os.chmod(command_log_file, 0o755) # Make it executable
            print(f"  Command saved to: {command_log_file}")
        except Exception as e:
            # Don't fail the task just because command logging failed
            print(f"  Warning: Failed to save command to {command_log_file}: {e}")

    # Execute the command
    try:
        if cwd:
            os.makedirs(cwd, exist_ok=True)
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True, cwd=cwd,
            # Use executable=/bin/bash ? Might improve consistency
            # executable='/bin/bash'
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"  Return Code: {e.returncode}")
        print(f"  STDOUT:\n{e.stdout}")
        print(f"  STDERR:\n{e.stderr}")
        raise

# --- TaskOutput class (remains the same) ---
class TaskOutput:
    # ... (no changes needed) ...
    def __init__(self, workflow, task_func, call_args, call_kwargs):
        self.workflow = workflow; self.task_func = task_func
        self.call_args = tuple(call_args); self.call_kwargs = tuple(sorted(call_kwargs.items()))
        self.id = self._generate_id()
    def _generate_id(self):
        try:
            def prep_for_hash(item):
                if isinstance(item, TaskOutput): return item.id
                if isinstance(item, (list, dict)): return json.dumps(item, sort_keys=True)
                return item
            args_for_hash = tuple(prep_for_hash(a) for a in self.call_args)
            kwargs_for_hash = tuple((k, prep_for_hash(v)) for k, v in self.call_kwargs)
            arg_string = json.dumps((args_for_hash, kwargs_for_hash), sort_keys=True)
        except TypeError: arg_string = str((args_for_hash, kwargs_for_hash))
        id_string = f"{self.task_func.__name__}:{arg_string}"; return hashlib.md5(id_string.encode()).hexdigest()[:10]
    def get_dependencies(self):
        deps = set();
        for arg in self.call_args:
            if isinstance(arg, TaskOutput): deps.add(arg.id)
        for _, value in self.call_kwargs:
            if isinstance(value, TaskOutput): deps.add(value.id)
        return deps
    def __repr__(self): return f"<TaskOutput of {self.task_func.__name__} (ID: {self.id})>"
    def __hash__(self): return hash(self.id)
    def __eq__(self, other): return isinstance(other, TaskOutput) and self.id == other.id


# --- MODIFIED Top-Level Function for Executor (_run_task_in_process) ---
def _create_input_symlink(source_path, link_dir, link_prefix="input"):
    """Helper function to create a symlink for an input file/dir."""
    # Check if source exists and is a string path
    if not isinstance(source_path, str) or not os.path.exists(source_path):
        # print(f"  [Linker] Skipping non-existent or non-path input: {source_path}")
        return # Skip things that aren't existing paths

    abs_source_path = os.path.abspath(source_path)
    link_basename = os.path.basename(abs_source_path)
    link_name = os.path.join(link_dir, f"{link_prefix}_{link_basename}")

    # Avoid creating link loops (linking work dir inside itself) - simple check
    abs_link_dir = os.path.abspath(link_dir)
    if abs_source_path.startswith(abs_link_dir):
         print(f"  [Linker] Warning: Skipping potentially recursive link for {abs_source_path} in {abs_link_dir}")
         return

    try:
        # Ensure target directory exists
        os.makedirs(link_dir, exist_ok=True)
        # Create symlink - remove existing if it's already there (e.g., from retry)
        if os.path.lexists(link_name):
             os.remove(link_name)
        os.symlink(abs_source_path, link_name)
        print(f"  [Linker] Linked input: '{link_name}' -> '{abs_source_path}'")
    except Exception as e:
        # Don't fail task for linking error, just warn
        print(f"  [Linker] Warning: Failed to link input '{abs_source_path}' to '{link_name}': {e}")


def _run_task_in_process(user_func, task_id, func_name, args, kwargs, work_dir, config):
    """
    Executes the user's task function in a separate process.
    Creates input symlinks before running.
    """
    print(f"  [Executor PID {os.getpid()}] Preparing task: {func_name} (ID: {task_id})")
    print(f"  [Executor PID {os.getpid()}] Working directory: {work_dir}")
    os.makedirs(work_dir, exist_ok=True) # Ensure dir exists

    # --- Create Input Symlinks ---
    print(f"  [Executor PID {os.getpid()}] Creating input symlinks...")
    input_counter = 0
    # Link positional arguments
    for i, arg in enumerate(args):
        if isinstance(arg, (list, tuple)):
            for item_idx, item in enumerate(arg):
                 _create_input_symlink(item, work_dir, link_prefix=f"input_arg{i}_item{item_idx}")
        else:
            _create_input_symlink(arg, work_dir, link_prefix=f"input_arg{i}")

    # Link keyword arguments
    for key, value in kwargs.items():
         if isinstance(value, (list, tuple)):
             for item_idx, item in enumerate(value):
                 _create_input_symlink(item, work_dir, link_prefix=f"input_{key}_item{item_idx}")
         else:
            _create_input_symlink(value, work_dir, link_prefix=f"input_{key}")


    # Inject task_work_dir and config if function expects them
    final_kwargs = dict(kwargs) # Copy original kwargs
    sig = inspect.signature(user_func)
    if "task_work_dir" in sig.parameters:
        # Pass the absolute path for consistency
        final_kwargs["task_work_dir"] = os.path.abspath(work_dir)
    if "config" in sig.parameters:
        final_kwargs["config"] = config

    print(f"  [Executor PID {os.getpid()}] Executing task: {func_name} (ID: {task_id})") # Simplified log
    try:
        # CALL THE ACTUAL USER FUNCTION with resolved arguments
        result = user_func(*args, **final_kwargs)
        print(f"  [Executor PID {os.getpid()}] Finished task: {func_name} (ID: {task_id}) Result: {result}")
        return result
    except Exception as e:
        print(f"  [Executor PID {os.getpid()}] FAILED task: {func_name} (ID: {task_id})")
        tb_str = traceback.format_exc()
        raise RuntimeError(f"Task {func_name} (ID: {task_id}) failed in executor process.\nTraceback:\n{tb_str}") from e


# --- Workflow Class (remains largely the same, minor adjustments) ---
class Workflow:
    def __init__(self, work_dir="_pyflow_work", max_workers=None, config_file=None):
        self.task_registry = {}
        # Ensure work_dir is absolute at init time
        self._work_dir = os.path.abspath(work_dir)
        self.max_workers = max_workers or os.cpu_count()
        # Config is now loaded externally in pipeline.py before init,
        # but keep _load_config helper method internally.
        self.config = {} # Initialize as empty dict

        self.task_calls = {}
        self.task_results = {}
        self.task_status = {}
        self.task_dependencies = {}
        self.task_dependents = {}

        # No need to pass config_file here anymore
        print(f"Workflow initialized: work_dir='{self._work_dir}', max_workers={self.max_workers}")

    # _load_config method remains available but called externally now
    def _load_config(self, config_file):
        """Loads configuration from a JSON file."""
        # ... (same as before) ...
        if config_file:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    try: return json.load(f)
                    except json.JSONDecodeError as e: print(f"Error decoding JSON from {config_file}: {e}"); raise
            else: raise FileNotFoundError(f"Config file not found: {config_file}")
        return {}

    # task decorator remains the same
    def task(self, func):
        # ... (same as before) ...
        if func.__name__ in self.task_registry: print(f"Warning: Task '{func.__name__}' registered.")
        self.task_registry[func.__name__] = func
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            task_output = TaskOutput(self, func, args, kwargs)
            if task_output.id not in self.task_calls: self.task_calls[task_output.id] = task_output
            if func.__name__ not in self.task_registry: self.task_registry[func.__name__] = func
            return task_output
        return wrapper


    # _build_dag method remains the same
    def _build_dag(self, final_target_id):
        # ... (same as before) ...
        queue = [final_target_id]; visited = set()
        while queue:
            current_id = queue.pop(0)
            if current_id in visited: continue
            visited.add(current_id)
            if current_id not in self.task_calls: continue
            task_output = self.task_calls[current_id]; deps = task_output.get_dependencies()
            self.task_dependencies[current_id] = deps; self.task_status[current_id] = TaskStatus.PENDING
            for dep_id in deps:
                if dep_id not in self.task_dependents: self.task_dependents[dep_id] = set()
                self.task_dependents[dep_id].add(current_id)
                if dep_id not in visited:
                    if dep_id in self.task_calls and dep_id not in queue: queue.append(dep_id)
                    elif dep_id not in self.task_calls: print(f"Warning: Dependency ID {dep_id} missing.")


    # run method remains largely the same (submission part)
    def run(self, final_task_output):
        # ... (Setup, state reset, DAG build - same as before) ...
        if not isinstance(final_task_output, TaskOutput): print("Final target is not a TaskOutput."); return final_task_output
        if final_task_output.workflow is not self: raise ValueError("TaskOutput belongs to different Workflow.")
        self.task_results = {}; self.task_status = {}; self.task_dependencies = {}; self.task_dependents = {}
        os.makedirs(self._work_dir, exist_ok=True)
        target_id = final_task_output.id
        print(f"\n--- Building Workflow DAG for target: {target_id} ---")
        self._build_dag(target_id)
        required_tasks = {tid for tid in self.task_status if tid in self.task_calls}
        if not required_tasks: print("No tasks found in DAG."); return final_task_output
        print(f"Tasks involved: {len(required_tasks)}")
        print(f"\n--- Starting Parallel Execution (max_workers={self.max_workers}) ---")
        start_time = time.time(); tasks_failed = set(); tasks_completed = set()
        active_futures = {}

        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            while len(tasks_completed) + len(tasks_failed) < len(required_tasks):
                ready_to_submit = []
                for task_id in list(required_tasks - tasks_completed - tasks_failed):
                    if self.task_status.get(task_id) == TaskStatus.PENDING:
                        deps = self.task_dependencies.get(task_id, set())
                        if all(dep_id in self.task_results for dep_id in deps):
                             ready_to_submit.append(task_id)

                for task_id in ready_to_submit:
                    task_output = self.task_calls[task_id]
                    func = task_output.task_func; func_name = func.__name__
                    args = task_output.call_args; kwargs = dict(task_output.call_kwargs)
                    resolved_args = []; resolved_kwargs = {}
                    try: # Resolve dependencies
                        for arg in args: resolved_args.append(self.task_results[arg.id] if isinstance(arg, TaskOutput) else arg)
                        for key, value in kwargs.items(): resolved_kwargs[key] = self.task_results[value.id] if isinstance(value, TaskOutput) else value
                    except KeyError as e:
                         print(f"Error: Dependency result missing for task {task_id} -> {func_name}. Missing key: {e}");
                         self.task_status[task_id] = TaskStatus.FAILED; tasks_failed.add(task_id); continue

                    # Define the specific work directory for this task execution (absolute path)
                    task_work_dir = os.path.join(self._work_dir, func_name, task_id)

                    print(f"Submitting task: {func_name} (ID: {task_id})")
                    self.task_status[task_id] = TaskStatus.RUNNING

                    future = executor.submit(
                        _run_task_in_process, # The pickleable top-level function
                        user_func=func, task_id=task_id, func_name=func_name,
                        args=tuple(resolved_args), kwargs=resolved_kwargs,
                        work_dir=task_work_dir, # Pass absolute path to task work dir
                        config=self.config
                    )
                    active_futures[future] = task_id

                # --- Wait for results (same logic as before) ---
                if not active_futures and len(tasks_completed) + len(tasks_failed) < len(required_tasks):
                    if (len(tasks_completed) + len(tasks_failed)) != len(required_tasks):
                         print("Warning: No tasks running, but workflow not complete.")
                         for tid in required_tasks - tasks_completed - tasks_failed:
                             if self.task_status.get(tid) == TaskStatus.PENDING:
                                 unmet = [dep for dep in self.task_dependencies.get(tid, set()) if dep not in self.task_results]
                                 print(f"  - Task {tid} ({self.task_calls[tid].task_func.__name__}) pending. Unmet: {unmet}")
                    break
                if not active_futures: break

                done, _ = concurrent.futures.wait(active_futures.keys(), return_when=concurrent.futures.FIRST_COMPLETED)

                for future in done:
                    task_id = active_futures.pop(future)
                    task_name = self.task_calls[task_id].task_func.__name__
                    try:
                        result = future.result()
                        self.task_results[task_id] = result; self.task_status[task_id] = TaskStatus.COMPLETED
                        tasks_completed.add(task_id); print(f"Task Completed: {task_name} (ID: {task_id})")
                    except Exception as e:
                        self.task_status[task_id] = TaskStatus.FAILED; tasks_failed.add(task_id)
                        print(f"!!! Task FAILED: {task_name} (ID: {task_id}) !!!\n  Error: {e}")
                        # --- Failure Propagation (same logic) ---
                        cancel_queue = list(self.task_dependents.get(task_id, set())); visited_cancel = {task_id}
                        while cancel_queue:
                            dependent_id = cancel_queue.pop(0)
                            if dependent_id in visited_cancel: continue
                            visited_cancel.add(dependent_id)
                            if dependent_id in self.task_calls:
                                if self.task_status.get(dependent_id) == TaskStatus.PENDING:
                                    self.task_status[dependent_id] = TaskStatus.CANCELLED; tasks_failed.add(dependent_id)
                                    print(f"  -> Cancelling downstream: {self.task_calls[dependent_id].task_func.__name__} (ID: {dependent_id})")
                                    for next_dep_id in self.task_dependents.get(dependent_id, set()):
                                          if next_dep_id not in visited_cancel: cancel_queue.append(next_dep_id)
                            else: print(f"Warning: Trying to cancel unknown ID: {dependent_id}")

        # --- End of Execution (Summary - same logic) ---
        end_time = time.time(); print(f"\n--- Workflow Execution Summary ---")
        print(f"Total time: {end_time - start_time:.2f} seconds")
        print(f"Tasks Completed: {len(tasks_completed)}"); print(f"Tasks Failed/Cancelled: {len(tasks_failed)}")
        if target_id in tasks_failed or self.task_status.get(target_id) != TaskStatus.COMPLETED:
             print("\n!!! Workflow did not complete successfully !!!")
             for tid in sorted(list(tasks_failed)):
                 if tid in self.task_calls: print(f"  - {self.task_calls[tid].task_func.__name__} (ID: {tid}): {self.task_status.get(tid, 'UNKNOWN').name}")
                 else: print(f"  - Unknown Task (ID: {tid}): FAILED/CANCELLED")
             raise RuntimeError("Workflow execution failed.")
        else:
             print("\n--- Workflow finished successfully ---")
             return self.task_results.get(target_id)

    # cleanup method remains the same
    def cleanup(self):
        import shutil
        if os.path.exists(self._work_dir):
            print(f"Cleaning up working directory: {self._work_dir}")
            shutil.rmtree(self._work_dir)
        else:
            print(f"Working directory not found, skipping cleanup: {self._work_dir}")