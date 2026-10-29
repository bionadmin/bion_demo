# Databricks notebook source
# MAGIC %md
# MAGIC # Functions — SimpleAW
# MAGIC
# MAGIC Shared Python utility functions for parallel notebook execution,
# MAGIC retry logic, and common SparkSQL helpers.
# MAGIC
# MAGIC ``%run`` this notebook from any process notebook that needs
# MAGIC these utilities.

# COMMAND ----------

# Parallel notebook runner
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterable
import time

def run_notebook(
    path: str,
    timeout: int = 3600,
    arguments: dict | None = None,
    retries: int = 0,
) -> str:
    """Run a single notebook via dbutils.notebook.run() with optional retry.

    Args:
        path:      Workspace path to the notebook.
        timeout:   Timeout in seconds.
        arguments: Key/value arguments passed to the notebook.
        retries:   Number of retry attempts on failure.

    Returns:
        The notebook's exit value string.
    """
    args = arguments or {}
    for _attempt in range(retries + 1):
        try:
            return dbutils.notebook.run(path, timeout, args)
        except Exception as _exc:
            if _attempt == retries:
                raise
            print(f'[run_notebook] {path}: attempt {_attempt + 1} failed, retrying in 5s...')
            time.sleep(5)


def run_notebooks_parallel(
    notebooks: list[tuple[str, str]],
    max_workers: int = 4,
    timeout: int = 3600,
    retries: int = 0,
) -> dict[str, str]:
    """Run multiple notebooks in parallel using a thread pool.

    Args:
        notebooks:   List of (name, path) tuples.
        max_workers: Maximum concurrent workers.
        timeout:     Per-notebook timeout in seconds.
        retries:     Retry attempts per notebook.

    Returns:
        Dict mapping notebook name to its exit value.

    Raises:
        RuntimeError: If any notebook fails after all retries.
    """
    results: dict[str, str] = {}
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(run_notebook, path, timeout, {}, retries): name
            for name, path in notebooks
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
                print(f'[OK] {name}: {results[name]}')
            except Exception as exc:
                errors.append(f'{name}: {exc}')
                print(f'[FAIL] {name}: {exc}')

    if errors:
        raise RuntimeError(
            f'{len(errors)} notebook(s) failed:\n' + '\n'.join(errors)
        )
    return results


def get_row_count(catalog: str, database: str, table: str) -> int:
    """Return the current row count of a Delta table."""
    return spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {catalog}.{database}.{table}"
    ).collect()[0]['cnt']


def table_exists(catalog: str, database: str, table: str) -> bool:
    """Return True if the Unity Catalog table exists."""
    try:
        spark.sql(f"DESCRIBE TABLE {catalog}.{database}.{table}")
        return True
    except Exception:
        return False