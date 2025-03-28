# PyFlow Prototype (LiteFlow)

## Overview

This project is a prototype for a simple, Python-based workflow management system inspired by [Nextflow](https://www.nextflow.io/). It aims to provide a more "Pythonic" way to define and execute computational pipelines, particularly for users who find Nextflow's Groovy DSL a barrier.

The core idea is to define pipeline tasks using Python functions and decorators, automatically handle dependencies based on function inputs/outputs, and manage parallel execution.

**Status:** Early Prototype

## Motivation

Nextflow is an incredibly powerful and widely adopted workflow system, especially in bioinformatics. However, its reliance on the Groovy language can be a hurdle for teams primarily working within the Python ecosystem. While tools like Snakemake offer a Python-based alternative, this project explores a different approach focusing on:

1.  **Simplicity:** Aiming for a lower learning curve compared to the full feature set and DSL of Nextflow or Snakemake.
2.  **Pythonic Interface:** Leveraging Python decorators (`@task`) and standard Python functions for defining workflows, making it feel natural for Python developers.
3.  **Avoiding Groovy:** Providing an alternative for users who prefer to stay entirely within Python.
4.  **Core Workflow Needs:** Focusing initially on essential features like dependency management, parallelism, basic configuration, and shell command execution.

This prototype serves as a proof-of-concept for such a system.

## Key Features (Current Prototype)

*   **Pythonic Task Definition:** Define pipeline steps as standard Python functions using the `@workflow.task` decorator.
*   **Automatic Dependency Management:** The workflow engine automatically determines the execution order based on how the output of one task (`TaskOutput` object) is used as input to another.
*   **File-Based Data Flow:** Primarily designed for tasks that consume and produce files. Tasks return absolute paths to their outputs, which are passed to downstream tasks.
*   **Parallel Execution:** Utilizes Python's `concurrent.futures.ProcessPoolExecutor` to run independent tasks (tasks whose dependencies are met) concurrently, leveraging multiple CPU cores.
*   **Basic Error Handling:**
    *   Detects exceptions raised within tasks.
    *   Reports errors clearly, including tracebacks from the execution process.
    *   Automatically cancels downstream tasks that depend on a failed task.
    *   Propagates failure to the main workflow run.
*   **External Configuration:** Load workflow parameters (e.g., tool paths, sample names, flags) from an external JSON file (`--config config.json`). Tasks can access this configuration dictionary.
*   **Shell Command Integration:** Provides a simple `run_shell` helper function within `pyflow_core.py` to easily execute external command-line tools within tasks, handling errors.
*   **Isolated Task Workspaces:** Each specific execution of a task function gets its own unique working directory created under the main workflow work directory (e.g., `_pyflow_work_separate_tasks/task_name/task_hash/`), helping to prevent output file collisions.

## Installation

Currently, this is a prototype consisting of Python script files.

1.  **Clone/Download:** Get the files (`pyflow_core.py`, `my_tasks.py`, `my_pipeline.py`, `config.json`).
    ```bash
    git clone <repository_url> # Or download ZIP
    cd <repository_directory>
    ```
2.  **Python Version:** Requires Python 3.7+ (due to `concurrent.futures`, f-strings, etc.).
3.  **Dependencies:** No external libraries are required beyond the Python standard library.

## Usage

1.  **Define Task Logic:** Implement the core logic of your pipeline steps as plain Python functions in `my_tasks.py`. These functions can accept parameters, configuration, and a `task_work_dir`. They should typically return the absolute path(s) to output files.
2.  **Define Workflow:**
    *   In `my_pipeline.py`, import the task functions from `my_tasks.py`.
    *   Create a `Workflow` instance from `pyflow_core`.
    *   Decorate the imported task functions using `@workflow.task`.
    *   Define the pipeline structure by calling the decorated functions, passing the output of one task as input to others. This builds the dependency graph.
3.  **Configure (Optional):** Create a `config.json` file with key-value pairs for parameters you want to use in your tasks.
4.  **Run:** Execute the main pipeline script from your terminal.


**Example Commands:**

```bash
# Basic run (uses default config values in tasks)
python my_pipeline.py

# Run with an external configuration file
python my_pipeline.py --config config.json

# Intentionally fail one branch (if configured in my_tasks.py and my_pipeline.py)
python my_pipeline.py --fail-b

# Clean up the working directory before running
python my_pipeline.py --cleanup --config config.json
```

## Current Limitations:
* No Caching: Tasks are re-executed every time the workflow runs, even if inputs haven't changed. There is no persistent caching between runs.
* No Container Support: Tasks execute directly on the host system. Integration with Docker or Singularity is not yet implemented.
* Basic Input/Output Handling: Primarily focused on passing file paths (as strings). No explicit system for handling directories robustly or passing complex Python objects between processes reliably.
* Basic Scheduling: Dependency-driven execution only. No support for task retries, time limits, conditional execution based on output content, etc.
* Local Execution Only: Designed to run on a single machine using multiple processes. No support for submitting jobs to HPC schedulers (SLURM, SGE, LSF, etc.) or cloud batch systems (AWS Batch, Google Cloud Batch).
* Rudimentary DAG: The dependency graph is built implicitly and used for execution, but there's no upfront DAG analysis, cycle detection, or visualization capability.
* Error Propagation: Basic cancellation of downstream tasks on failure. More sophisticated strategies are not implemented.

## Next Steps / Roadmap
* Robust Caching: Implement persistent caching based on hashing inputs (arguments, config, input file content/timestamps) and checking output existence/integrity. This is crucial for efficient re-runs.
* Container Integration: Allow tasks to specify a Docker (or Singularity) image (@task(image="...")) and execute the task command within that container, managing volume mounts automatically.
* Input/Output Type System: Introduce explicit types (e.g., File, Directory, Str, Int) for task inputs/outputs to enable better validation, handling (e.g., ensuring directories exist), and potentially more robust serialization.
* Enhanced Error Handling: Implement task retry mechanisms (e.g., @task(retries=3)).
* Explicit DAG Management: Build the Directed Acyclic Graph explicitly before execution, allowing for cycle detection and potentially visualization (e.g., using Graphviz).
* Resource Specification: Allow tasks to declare resource needs (e.g., @task(cpus=4, memory="8G")) – primarily useful for future scheduler integration.
* (Ambitious) Basic Executor Plugins: Develop support for different execution environments, starting potentially with a simple SLURM backend.

