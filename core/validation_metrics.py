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
                "exp_a3f": {
                    "total": 100,  # Total unique materials
                    "failures": 5,  # Unique materials with FAIL
                    "passes": 95,  # Unique materials with PASS
                    "pass_rate": 95.0
                },
                ...
            },
            "derived_metrics": {
                "derived_abp_incomplete": {
                    "total": 100,  # Total unique materials
                    "failures": 10,  # Unique materials with FAIL
                    "passes": 90,  # Unique materials with PASS
                    "pass_rate": 90.0
                },
                ...
            },
            "overall_pass_rate": float  # % of materials passing ALL expectations
        }

        Note: All counts are based on UNIQUE MATERIALS, not row-level checks.
        Overall pass rate = (materials with zero failures / total materials) * 100
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

    # Calculate metrics for expectation columns (count UNIQUE MATERIALS with failures)
    exp_columns = [col for col in df.columns if col.startswith("exp_")]
    expectation_metrics = {}

    for exp_col in exp_columns:
        # Count unique materials with FAIL for this expectation
        if index_column in df.columns:
            failed_materials = df[df[exp_col] == 'FAIL'][index_column].nunique()
            total = total_materials
        else:
            # Fallback if no index column
            failed_materials = (df[exp_col] == 'FAIL').sum()
            total = len(df)

        passed_materials = total - failed_materials
        pass_rate = (passed_materials / total * 100) if total > 0 else 0.0

        expectation_metrics[exp_col] = {
            "total": int(total),
            "failures": int(failed_materials),
            "passes": int(passed_materials),
            "pass_rate": round(pass_rate, 2),
        }

    # Calculate metrics for derived status columns (count UNIQUE MATERIALS with failures)
    derived_columns = [col for col in df.columns if col.startswith("derived_")]
    derived_metrics = {}

    for derived_col in derived_columns:
        # Count unique materials with FAIL for this derived status
        if index_column in df.columns:
            failed_materials = df[df[derived_col] == 'FAIL'][index_column].nunique()
            total = total_materials
        else:
            # Fallback if no index column
            failed_materials = (df[derived_col] == 'FAIL').sum()
            total = len(df)

        passed_materials = total - failed_materials
        pass_rate = (passed_materials / total * 100) if total > 0 else 0.0

        derived_metrics[derived_col] = {
            "total": int(total),
            "failures": int(failed_materials),
            "passes": int(passed_materials),
            "pass_rate": round(pass_rate, 2),
        }

    # Calculate overall pass rate: materials that pass ALL expectations (have zero failures)
    all_validation_cols = exp_columns + derived_columns
    if all_validation_cols and total_materials > 0 and index_column in df.columns:
        # Find all unique materials that have at least one FAIL in any column
        materials_with_failures = set()
        for col in all_validation_cols:
            failed_mats = set(df[df[col] == 'FAIL'][index_column].unique())
            materials_with_failures.update(failed_mats)

        # Materials passing all = total - materials with any failures
        materials_passing_all = total_materials - len(materials_with_failures)
        overall_pass_rate = (materials_passing_all / total_materials * 100) if total_materials > 0 else 100.0
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
