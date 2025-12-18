"""
Simple metrics calculator for columnar validation results.

Works directly on Snowflake DataFrames without complex parsing.
"""

import pandas as pd
from typing import Dict, List, Any
from core.expectation_metadata import lookup_expectation_metadata


def calculate_validation_metrics(
    df: pd.DataFrame,
    yaml_path: str = None,
    suite_config: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Calculate simple metrics from columnar validation DataFrame.

    Takes the raw Snowflake DataFrame and computes summary statistics
    without complex parsing or restructuring.

    Args:
        df: Columnar DataFrame from Snowflake with exp_* and derived_* columns
        yaml_path: Optional path to YAML for metadata lookups
        suite_config: Optional suite configuration dict for metadata

    Returns:
        Simple metrics dictionary:
        {
            "total_rows": int,
            "total_materials": int,
            "expectation_metrics": {
                "exp_a3f": {"total": 100, "failures": 5, "pass_rate": 95.0},
                ...
            },
            "derived_metrics": {
                "derived_abp_incomplete": {"total": 100, "failures": 10, "pass_rate": 90.0},
                ...
            },
            "overall_pass_rate": float
        }
    """
    if df.empty:
        return {
            "total_rows": 0,
            "total_materials": 0,
            "expectation_metrics": {},
            "derived_metrics": {},
            "overall_pass_rate": 100.0,
        }

    # Basic counts
    total_rows = len(df)

    # Count unique materials (if material_number column exists)
    index_column = "material_number"
    if suite_config:
        index_column = suite_config.get("metadata", {}).get("index_column", "material_number").lower()

    total_materials = 0
    if index_column in df.columns:
        total_materials = df[index_column].nunique()

    # Calculate metrics for expectation columns
    exp_columns = [col for col in df.columns if col.startswith("exp_")]
    expectation_metrics = {}

    for exp_col in exp_columns:
        failures = (df[exp_col] == 'FAIL').sum()
        passes = (df[exp_col] == 'PASS').sum()
        total = failures + passes
        pass_rate = (passes / total * 100) if total > 0 else 0.0

        expectation_metrics[exp_col] = {
            "total": int(total),
            "failures": int(failures),
            "passes": int(passes),
            "pass_rate": round(pass_rate, 2),
        }

    # Calculate metrics for derived status columns
    derived_columns = [col for col in df.columns if col.startswith("derived_")]
    derived_metrics = {}

    for derived_col in derived_columns:
        failures = (df[derived_col] == 'FAIL').sum()
        passes = (df[derived_col] == 'PASS').sum()
        total = failures + passes
        pass_rate = (passes / total * 100) if total > 0 else 0.0

        derived_metrics[derived_col] = {
            "total": int(total),
            "failures": int(failures),
            "passes": int(passes),
            "pass_rate": round(pass_rate, 2),
        }

    # Calculate overall pass rate (all expectations + derived)
    all_validation_cols = exp_columns + derived_columns
    if all_validation_cols:
        total_checks = sum(expectation_metrics.get(col, {}).get("total", 0) for col in exp_columns)
        total_checks += sum(derived_metrics.get(col, {}).get("total", 0) for col in derived_columns)

        total_failures = sum(expectation_metrics.get(col, {}).get("failures", 0) for col in exp_columns)
        total_failures += sum(derived_metrics.get(col, {}).get("failures", 0) for col in derived_columns)

        overall_pass_rate = ((total_checks - total_failures) / total_checks * 100) if total_checks > 0 else 100.0
    else:
        overall_pass_rate = 100.0

    return {
        "total_rows": total_rows,
        "total_materials": total_materials,
        "expectation_metrics": expectation_metrics,
        "derived_metrics": derived_metrics,
        "overall_pass_rate": round(overall_pass_rate, 2),
    }


def get_failed_materials(
    df: pd.DataFrame,
    exp_id: str = None,
    derived_id: str = None,
    index_column: str = "material_number",
) -> pd.DataFrame:
    """
    Get materials that failed a specific expectation or derived status.

    Args:
        df: Columnar DataFrame from Snowflake
        exp_id: Expectation ID to filter by (e.g., "exp_a3f_841e")
        derived_id: Derived status ID to filter by (e.g., "derived_abp_incomplete")
        index_column: Name of the index column

    Returns:
        Filtered DataFrame with only failed rows
    """
    if exp_id:
        return df[df[exp_id] == 'FAIL']
    elif derived_id:
        return df[df[derived_id] == 'FAIL']
    else:
        # Return all failures (any exp_* or derived_* = FAIL)
        exp_cols = [col for col in df.columns if col.startswith("exp_")]
        derived_cols = [col for col in df.columns if col.startswith("derived_")]

        mask = pd.Series([False] * len(df), index=df.index)
        for col in exp_cols + derived_cols:
            mask |= (df[col] == 'FAIL')

        return df[mask]


def get_summary_table(metrics: Dict[str, Any]) -> pd.DataFrame:
    """
    Convert metrics dictionary to a summary DataFrame for display.

    Args:
        metrics: Output from calculate_validation_metrics()

    Returns:
        DataFrame with columns: [Validation, Total, Failures, Passes, Pass Rate]
    """
    rows = []

    # Add expectation metrics
    for exp_id, stats in metrics.get("expectation_metrics", {}).items():
        rows.append({
            "Validation": exp_id,
            "Type": "Expectation",
            "Total": stats["total"],
            "Failures": stats["failures"],
            "Passes": stats["passes"],
            "Pass Rate": f"{stats['pass_rate']}%",
        })

    # Add derived metrics
    for derived_id, stats in metrics.get("derived_metrics", {}).items():
        # Clean up the label
        label = derived_id.replace("derived_", "").replace("_", " ").title()
        rows.append({
            "Validation": label,
            "Type": "Derived Status",
            "Total": stats["total"],
            "Failures": stats["failures"],
            "Passes": stats["passes"],
            "Pass Rate": f"{stats['pass_rate']}%",
        })

    return pd.DataFrame(rows)


__all__ = [
    "calculate_validation_metrics",
    "get_failed_materials",
    "get_summary_table",
]
