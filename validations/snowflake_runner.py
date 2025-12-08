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
from core.grain_mapping import (
    get_context_columns_for_columns,
    get_grain_for_column,
)


def run_validation_from_yaml_snowflake(
    yaml_path: Union[str, Path],
    limit: int = None,
    include_failure_details: bool = False,
) -> Dict[str, Any]:
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
                    "table_grain": "...",
                    "unique_by": [...],
                    "flag_column": "...",
                    "context_columns": [...]
                }
            ],
            "validated_materials": [],
            "full_results_df": pd.DataFrame
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
    except RuntimeError:
        # Propagate user-facing runtime errors (e.g., SSO mismatch) unchanged so the
        # UI can display the friendly guidance and halt gracefully.
        raise
    except Exception as e:
        # Wrap any other errors to keep the error type consistent for the UI.
        print(f"❌ Query execution failed: {e}")
        raise RuntimeError(f"❌ Query execution failed: {e}") from e

    # Parse results
    results = _parse_sql_results(df, suite_config, include_failure_details)

    print(f"✅ Validation complete: {len(results['results'])} rules checked")

    return results


def _parse_sql_results(
    df: pd.DataFrame,
    suite_config: Dict[str, Any],
    include_failure_details: bool = False,
) -> Dict[str, Any]:
    """
    Parse Snowflake query results into GX-compatible format.

    Args:
        df: DataFrame containing full-width validation rows (source columns + flags)
        suite_config: Original suite configuration

    Returns:
        Dictionary with 'results' and 'validated_materials' keys
    """
    if df.empty:
        return {"results": [], "validated_materials": []}

    # Normalize column names to lowercase for easier access
    df = df.copy()
    df.columns = df.columns.str.lower()

    results = []
    validations = suite_config.get("validations", [])

    for validation in validations:
        val_type = validation.get("type", "")

        if val_type == "expect_column_values_to_not_be_null":
            results.extend(_parse_not_null_results(df, validation, include_failure_details))
        elif val_type == "expect_column_values_to_be_in_set":
            results.extend(_parse_value_in_set_results(df, validation, include_failure_details))
        elif val_type == "expect_column_values_to_not_be_in_set":
            results.extend(_parse_value_not_in_set_results(df, validation, include_failure_details))
        elif val_type == "expect_column_values_to_match_regex":
            results.extend(_parse_regex_results(df, validation, include_failure_details))
        elif val_type == "expect_column_pair_values_to_be_equal":
            results.append(_parse_column_pair_equal_result(df, validation, include_failure_details))
        elif val_type == "expect_column_pair_values_a_to_be_greater_than_b":
            results.append(_parse_column_pair_greater_result(df, validation, include_failure_details))
        elif val_type == "custom:conditional_required":
            results.append(_parse_conditional_required_result(df, validation, include_failure_details))
        elif val_type == "custom:conditional_value_in_set":
            results.append(_parse_conditional_value_in_set_result(df, validation, include_failure_details))

    index_column = (
        suite_config.get("metadata", {}).get("index_column", "material_number")
    )
    validated_materials = []
    if index_column.lower() in df.columns:
        validated_materials = (
            df[index_column.lower()].dropna().unique().tolist()
        )

    return {
        "results": results,
        "validated_materials": validated_materials,
        "full_results_df": df,
    }


def _parse_not_null_results(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> list:
    """Parse not-null validation results from full-width rows."""
    results = []
    columns = validation.get("columns", [])
    element_count = len(df)

    for col in columns:
        safe_col_name = col.lower().replace('"', '')
        flag_col = f"{safe_col_name}_null_flag"

        unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(col)
        result = {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": col,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": flag_col,
            "context_columns": get_context_columns_for_columns([col]),
        }

        if include_failure_details:
            result["failed_materials"] = _build_failure_records(
                df, flag_col, result["context_columns"], extra_fields={"Unexpected Value": col}
            )

        results.append(result)

    return results


def _parse_value_in_set_results(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> list:
    """Parse value-in-set validation results from full-width rows."""
    results = []
    rules = validation.get("rules", {})
    element_count = len(df)

    for column, allowed_values in rules.items():
        safe_col_name = column.lower().replace('"', '')
        flag_col = f"{safe_col_name}_invalid_flag"

        unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(column)
        result = {
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": column,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": flag_col,
            "context_columns": get_context_columns_for_columns([column]),
        }

        if include_failure_details:
            result["failed_materials"] = _build_failure_records(
                df, flag_col, result["context_columns"], extra_fields={"Unexpected Value": column}
            )

        results.append(result)

    return results


def _parse_value_not_in_set_results(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> list:
    """Parse value-not-in-set validation results from full-width rows."""
    column = validation.get("column")
    if not column:
        return []

    safe_col_name = column.lower().replace('"', '')
    flag_col = f"{safe_col_name}_forbidden_flag"

    element_count = len(df)
    unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(column)
    result = {
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "column": column,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": flag_col,
        "context_columns": get_context_columns_for_columns([column]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records(
            df, flag_col, result["context_columns"], extra_fields={"Unexpected Value": column}
        )

    return [result]


def _parse_regex_results(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> list:
    """Parse regex validation results from full-width rows."""
    results = []
    columns = validation.get("columns", [])
    element_count = len(df)

    for column in columns:
        safe_col_name = column.lower().replace('"', '')
        flag_col = f"{safe_col_name}_regex_fail_flag"

        unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(column)
        result = {
            "expectation_type": "expect_column_values_to_match_regex",
            "column": column,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": flag_col,
            "context_columns": get_context_columns_for_columns([column]),
        }

        if include_failure_details:
            result["failed_materials"] = _build_failure_records(
                df, flag_col, result["context_columns"], extra_fields={"Unexpected Value": column}
            )

        results.append(result)

    return results


def _parse_column_pair_equal_result(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> Dict:
    """Parse column pair equality validation result from full-width rows."""
    col_a = validation.get("column_a")
    col_b = validation.get("column_b")

    safe_name = f"{col_a}_{col_b}_equal".lower().replace('"', '')
    flag_col = f"{safe_name}_mismatch_flag"

    element_count = len(df)
    unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(col_a)
    result = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "column": f"{col_a}|{col_b}",  # Combined column name
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": flag_col,
        "context_columns": get_context_columns_for_columns([col_a, col_b]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records(
            df, flag_col, result["context_columns"], extra_fields={col_a: col_a, col_b: col_b}
        )

    return result


def _parse_column_pair_greater_result(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> Dict:
    """Parse column pair greater-than validation result from full-width rows."""
    col_a = validation.get("column_a")
    col_b = validation.get("column_b")

    safe_name = f"{col_a}_{col_b}_greater".lower().replace('"', '')
    flag_col = f"{safe_name}_fail_flag"

    element_count = len(df)
    unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(col_a)
    result = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "column": f"{col_a}|{col_b}",
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": flag_col,
        "context_columns": get_context_columns_for_columns([col_a, col_b]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records(
            df, flag_col, result["context_columns"], extra_fields={col_a: col_a, col_b: col_b}
        )

    return result


def _parse_conditional_required_result(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> Dict:
    """Parse conditional required validation result from full-width rows."""
    condition_col = validation.get("condition_column")
    required_col = validation.get("required_column")

    safe_name = f"{condition_col}_{required_col}_conditional".lower().replace('"', '')
    flag_col = f"{safe_name}_violation_flag"

    element_count = len(df)
    unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(required_col)
    result = {
        "expectation_type": "custom:conditional_required",
        "column": required_col,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": flag_col,
        "context_columns": get_context_columns_for_columns([condition_col, required_col]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records(
            df,
            flag_col,
            result["context_columns"],
            extra_fields={condition_col: condition_col, required_col: required_col},
        )

    return result


def _parse_conditional_value_in_set_result(
    df: pd.DataFrame, validation: Dict, include_failure_details: bool
) -> Dict:
    """Parse conditional value in set validation result from full-width rows."""
    condition_col = validation.get("condition_column")
    target_col = validation.get("target_column")

    safe_name = f"{condition_col}_{target_col}_conditional_set".lower().replace('"', '')
    flag_col = f"{safe_name}_violation_flag"

    element_count = len(df)
    unexpected_count = int(df[flag_col].sum()) if flag_col in df.columns else 0
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(target_col)
    result = {
        "expectation_type": "custom:conditional_value_in_set",
        "column": target_col,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": flag_col,
        "context_columns": get_context_columns_for_columns([condition_col, target_col]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records(
            df,
            flag_col,
            result["context_columns"],
            extra_fields={condition_col: condition_col, target_col: target_col},
        )

    return result


def _build_failure_records(
    df: pd.DataFrame,
    flag_column: str,
    context_columns: list[str],
    extra_fields: Dict[str, str] | None = None,
) -> list[dict]:
    """Construct failure detail dictionaries from flag columns."""
    extra_fields = extra_fields or {}
    flag_key = flag_column.lower()

    if flag_key not in df.columns:
        return []

    failures: list[dict] = []
    flagged_rows = df[df[flag_key] == 1]

    for _, row in flagged_rows.iterrows():
        record: dict = {}

        for col in context_columns:
            record[col] = _get_row_value(row, col)

        for label, source_col in extra_fields.items():
            record[label] = _get_row_value(row, source_col)

        failures.append(record)

    return failures


def _get_row_value(row: pd.Series, column_name: str):
    column_key = column_name.lower().replace('"', '')
    return row.get(column_key)


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
