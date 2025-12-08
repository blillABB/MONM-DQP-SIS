"""
Unified Snowflake-Native Validation Runner

This module provides the main entry point for running validations entirely in Snowflake.
Uses the SQL generator to create queries dynamically from YAML configuration.

This is the new unified approach that replaces the separate query builder + suite editor workflow.
"""

import time
import yaml
from pathlib import Path
from typing import Dict, Any, Union
import pandas as pd

from validations.sql_generator import ValidationSQLGenerator
from core.queries import run_query
from core.grain_mapping import get_grain_for_column


def run_validation_from_yaml_snowflake(yaml_path: Union[str, Path], limit: int = None) -> Dict[str, Any]:
    """
    Run validation using Snowflake-native SQL generated from YAML configuration.

    This is the main entry point for the new Snowflake-native validation framework.
    It generates SQL on-the-fly from YAML rules (no persisted queries), executes
    in Snowflake, and returns GX-compatible results.

    Args:
        yaml_path: Path to YAML validation configuration file
        limit: Optional row limit for testing

    Returns:
        Dictionary with structure:
        {
            "results": [
                {
                    "expectation_type": "...",
                    "column": "...",
                    "success": bool,
                    "element_count": int,
                    "unexpected_count": int,
                    "unexpected_percent": float,
                    "failed_materials": [...],
                    "table_grain": "...",
                    "unique_by": [...]
                }
            ],
            "validated_materials": []
        }

    Example:
        >>> results = run_validation_from_yaml_snowflake("validation_yaml/my_suite.yaml")
        >>> print(f"Ran {len(results['results'])} validations")
    """
    print(f"▶ Running Snowflake-native validation from: {yaml_path}")

    # Load YAML configuration
    with open(yaml_path, 'r') as f:
        suite_config = yaml.safe_load(f)

    suite_name = suite_config.get("metadata", {}).get("suite_name", "Unknown")
    print(f"▶ Suite: {suite_name}")

    # Generate SQL
    start_time = time.time()
    generator = ValidationSQLGenerator(suite_config)
    sql = generator.generate_sql(limit=limit)

    print(f"▶ Generated SQL query ({len(sql)} chars)")
    print(f"▶ Executing in Snowflake...")

    # Execute query
    try:
        df = run_query(sql)
        execution_time = time.time() - start_time
        print(f"✅ Query executed in {execution_time:.2f} seconds")
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        raise

    # Parse results
    results = _parse_sql_results(df, suite_config)

    print(f"✅ Validation complete: {len(results['results'])} rules checked")

    return results


def _parse_sql_results(df: pd.DataFrame, suite_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse Snowflake query results into GX-compatible format.

    Args:
        df: DataFrame with one row containing validation metrics
        suite_config: Original suite configuration

    Returns:
        Dictionary with 'results' and 'validated_materials' keys
    """
    if df.empty:
        return {"results": [], "validated_materials": []}

    # Normalize column names to lowercase for easier access
    df.columns = df.columns.str.lower()
    row = df.iloc[0]

    results = []
    validations = suite_config.get("validations", [])

    for validation in validations:
        val_type = validation.get("type", "")

        if val_type == "expect_column_values_to_not_be_null":
            results.extend(_parse_not_null_results(row, validation))
        elif val_type == "expect_column_values_to_be_in_set":
            results.extend(_parse_value_in_set_results(row, validation))
        elif val_type == "expect_column_values_to_not_be_in_set":
            results.extend(_parse_value_not_in_set_results(row, validation))
        elif val_type == "expect_column_values_to_match_regex":
            results.extend(_parse_regex_results(row, validation))
        elif val_type == "expect_column_pair_values_to_be_equal":
            results.append(_parse_column_pair_equal_result(row, validation))
        elif val_type == "expect_column_pair_values_a_to_be_greater_than_b":
            results.append(_parse_column_pair_greater_result(row, validation))
        elif val_type == "custom:conditional_required":
            results.append(_parse_conditional_required_result(row, validation))
        elif val_type == "custom:conditional_value_in_set":
            results.append(_parse_conditional_value_in_set_result(row, validation))

    return {
        "results": results,
        "validated_materials": []  # Could be populated if needed
    }


def _parse_not_null_results(row: pd.Series, validation: Dict) -> list:
    """Parse not-null validation results."""
    results = []
    columns = validation.get("columns", [])

    for col in columns:
        safe_col_name = col.lower().replace('"', '')

        # Extract metrics
        element_count = int(row[f"{safe_col_name}_total"])
        unexpected_count = int(row[f"{safe_col_name}_null_count"])
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        # Parse failures JSON
        failures_json = row[f"{safe_col_name}_failures"]
        failed_materials = _parse_json_array(failures_json)

        # Get grain info
        table_grain, unique_by = get_grain_for_column(col)

        results.append({
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": col,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "failed_materials": failed_materials,
            "table_grain": table_grain,
            "unique_by": unique_by
        })

    return results


def _parse_value_in_set_results(row: pd.Series, validation: Dict) -> list:
    """Parse value-in-set validation results."""
    results = []
    rules = validation.get("rules", {})

    for column, allowed_values in rules.items():
        safe_col_name = column.lower().replace('"', '')

        # Note: SQL generator uses _invalid_count for this validation type
        # Extract metrics
        element_count = int(row.get(f"{safe_col_name}_total", 0))
        unexpected_count = int(row[f"{safe_col_name}_invalid_count"])
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        # Parse failures
        failures_json = row[f"{safe_col_name}_failures"]
        failed_materials = _parse_json_array(failures_json)

        # Get grain info
        table_grain, unique_by = get_grain_for_column(column)

        results.append({
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": column,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "failed_materials": failed_materials,
            "table_grain": table_grain,
            "unique_by": unique_by
        })

    return results


def _parse_value_not_in_set_results(row: pd.Series, validation: Dict) -> list:
    """Parse value-not-in-set validation results."""
    column = validation.get("column")
    if not column:
        return []

    safe_col_name = column.lower().replace('"', '')

    element_count = int(row.get(f"{safe_col_name}_total", 0))
    unexpected_count = int(row[f"{safe_col_name}_forbidden_count"])
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    failures_json = row[f"{safe_col_name}_failures"]
    failed_materials = _parse_json_array(failures_json)

    table_grain, unique_by = get_grain_for_column(column)

    return [{
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "column": column,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "failed_materials": failed_materials,
        "table_grain": table_grain,
        "unique_by": unique_by
    }]


def _parse_regex_results(row: pd.Series, validation: Dict) -> list:
    """Parse regex validation results."""
    results = []
    columns = validation.get("columns", [])

    for column in columns:
        safe_col_name = column.lower().replace('"', '')

        element_count = int(row.get(f"{safe_col_name}_total", 0))
        unexpected_count = int(row[f"{safe_col_name}_regex_fail_count"])
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        failures_json = row[f"{safe_col_name}_failures"]
        failed_materials = _parse_json_array(failures_json)

        table_grain, unique_by = get_grain_for_column(column)

        results.append({
            "expectation_type": "expect_column_values_to_match_regex",
            "column": column,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "failed_materials": failed_materials,
            "table_grain": table_grain,
            "unique_by": unique_by
        })

    return results


def _parse_column_pair_equal_result(row: pd.Series, validation: Dict) -> Dict:
    """Parse column pair equality validation result."""
    col_a = validation.get("column_a")
    col_b = validation.get("column_b")

    safe_name = f"{col_a}_{col_b}_equal".lower().replace('"', '')

    element_count = int(row.get(f"{safe_name}_total", 0))
    unexpected_count = int(row[f"{safe_name}_mismatch_count"])
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    failures_json = row[f"{safe_name}_failures"]
    failed_materials = _parse_json_array(failures_json)

    # Use grain from first column
    table_grain, unique_by = get_grain_for_column(col_a)

    return {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "column": f"{col_a}|{col_b}",  # Combined column name
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "failed_materials": failed_materials,
        "table_grain": table_grain,
        "unique_by": unique_by
    }


def _parse_column_pair_greater_result(row: pd.Series, validation: Dict) -> Dict:
    """Parse column pair greater-than validation result."""
    col_a = validation.get("column_a")
    col_b = validation.get("column_b")

    safe_name = f"{col_a}_{col_b}_greater".lower().replace('"', '')

    element_count = int(row.get(f"{safe_name}_total", 0))
    unexpected_count = int(row[f"{safe_name}_fail_count"])
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    failures_json = row[f"{safe_name}_failures"]
    failed_materials = _parse_json_array(failures_json)

    table_grain, unique_by = get_grain_for_column(col_a)

    return {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "column": f"{col_a}|{col_b}",
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "failed_materials": failed_materials,
        "table_grain": table_grain,
        "unique_by": unique_by
    }


def _parse_conditional_required_result(row: pd.Series, validation: Dict) -> Dict:
    """Parse conditional required validation result."""
    condition_col = validation.get("condition_column")
    required_col = validation.get("required_column")

    safe_name = f"{condition_col}_{required_col}_conditional".lower().replace('"', '')

    element_count = int(row.get(f"{safe_name}_total", 0))
    unexpected_count = int(row[f"{safe_name}_violation_count"])
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    failures_json = row[f"{safe_name}_failures"]
    failed_materials = _parse_json_array(failures_json)

    table_grain, unique_by = get_grain_for_column(required_col)

    return {
        "expectation_type": "custom:conditional_required",
        "column": required_col,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "failed_materials": failed_materials,
        "table_grain": table_grain,
        "unique_by": unique_by
    }


def _parse_conditional_value_in_set_result(row: pd.Series, validation: Dict) -> Dict:
    """Parse conditional value in set validation result."""
    condition_col = validation.get("condition_column")
    target_col = validation.get("target_column")

    safe_name = f"{condition_col}_{target_col}_conditional_set".lower().replace('"', '')

    element_count = int(row.get(f"{safe_name}_total", 0))
    unexpected_count = int(row[f"{safe_name}_violation_count"])
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    failures_json = row[f"{safe_name}_failures"]
    failed_materials = _parse_json_array(failures_json)

    table_grain, unique_by = get_grain_for_column(target_col)

    return {
        "expectation_type": "custom:conditional_value_in_set",
        "column": target_col,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "failed_materials": failed_materials,
        "table_grain": table_grain,
        "unique_by": unique_by
    }


def _parse_json_array(json_data) -> list:
    """
    Parse JSON array from Snowflake result.

    Snowflake returns JSON as string, need to parse it.
    Filters out None values from the array.
    """
    if json_data is None:
        return []

    import json
    if isinstance(json_data, str):
        try:
            parsed = json.loads(json_data)
            if isinstance(parsed, list):
                return [item for item in parsed if item is not None]
        except:
            return []

    if isinstance(json_data, list):
        return [item for item in json_data if item is not None]

    return []
