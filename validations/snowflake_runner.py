"""
Unified Snowflake-Native Validation Runner

This module provides the main entry point for running validations entirely in Snowflake.
Uses the SQL generator to create queries dynamically from YAML configuration.

This is the new unified approach that replaces the separate query builder + suite editor workflow.
"""

import time
import yaml
from pathlib import Path
from typing import Dict, Any, Union, List
import pandas as pd

from validations.sql_generator import (
    ValidationSQLGenerator,
    _annotate_expectation_ids,
    build_scoped_expectation_id,
)
from validations.derived_status_resolver import DerivedStatusResolver
from core.queries import run_query
from core.grain_mapping import (
    get_context_columns_for_columns,
    get_grain_for_column,
)
from core.validation_metrics import calculate_validation_metrics


def run_validation_simple(
    yaml_path: Union[str, Path],
    limit: int = None,
) -> Dict[str, Any]:
    """
    Run validation and return simple DataFrame + metrics (no complex parsing).

    This is the simplified approach that returns raw Snowflake results
    with lightweight metrics calculated on top.

    Args:
        yaml_path: Path to YAML validation configuration file
        limit: Optional row limit for testing

    Returns:
        Dictionary with:
        {
            "df": pd.DataFrame,  # Raw columnar results from Snowflake
            "metrics": dict,      # Summary statistics
            "suite_name": str,
            "suite_config": dict  # For metadata lookups if needed
        }

    Example:
        >>> result = run_validation_simple("suite.yaml", limit=100)
        >>> df = result["df"]
        >>> metrics = result["metrics"]
        >>>
        >>> # Filter failures directly
        >>> failures = df[df["exp_a3f_841e"] == "FAIL"]
        >>>
        >>> # Get metrics
        >>> print(f"Overall pass rate: {metrics['overall_pass_rate']}%")
    """
    print(f"▶ Running simple validation from: {yaml_path}")

    # Load YAML configuration
    with open(yaml_path, 'r') as f:
        suite_config = yaml.safe_load(f)

    suite_name = suite_config.get("metadata", {}).get("suite_name", "Unknown")
    print(f"▶ Suite: {suite_name}")

    # Attach stable expectation IDs
    suite_config["validations"] = _annotate_expectation_ids(
        suite_config.get("validations", []), suite_name
    )

    # Generate columnar SQL
    start_time = time.time()
    generator = ValidationSQLGenerator(suite_config, columnar_format=True)
    sql = generator.generate_sql(limit=limit)

    print(f"▶ Generated SQL query ({len(sql)} chars)")
    print(f"▶ Executing in Snowflake...")

    # Execute query
    try:
        df = _normalize_dataframe_columns(run_query(sql))
        execution_time = time.time() - start_time
        print(f"✅ Query executed in {execution_time:.2f} seconds")
    except RuntimeError:
        raise
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        raise RuntimeError(f"❌ Query execution failed: {e}") from e

    # Calculate simple metrics
    metrics = calculate_validation_metrics(df, suite_config=suite_config)

    print(f"✅ Validation complete")
    print(f"   - Total rows: {metrics['total_rows']}")
    print(f"   - Total materials: {metrics['total_materials']}")
    print(f"   - Overall pass rate: {metrics['overall_pass_rate']}%")

    return {
        "df": df,
        "metrics": metrics,
        "suite_name": suite_name,
        "suite_config": suite_config,
    }


def run_validation_from_yaml_snowflake(
    yaml_path: Union[str, Path],
    limit: int = None,
    include_failure_details: bool = False,
    columnar_format: bool = True,
) -> Dict[str, Any]:
    """
    Run validation using Snowflake-native SQL generated from YAML configuration.

    This is the main entry point for the new Snowflake-native validation framework.
    It generates SQL on-the-fly from YAML rules (no persisted queries), executes
    in Snowflake, and returns GX-compatible results.

    Args:
        yaml_path: Path to YAML validation configuration file
        limit: Optional row limit for testing
        include_failure_details: Include detailed failure information
        columnar_format: If True, use columnar format (one column per expectation).
                        If False, use legacy JSON array format.

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

    # Attach stable expectation IDs used by both SQL and parser
    suite_config["validations"] = _annotate_expectation_ids(
        suite_config.get("validations", []), suite_name
    )

    # Generate SQL
    start_time = time.time()
    generator = ValidationSQLGenerator(suite_config, columnar_format=columnar_format)
    sql = generator.generate_sql(limit=limit)

    print(f"▶ Generated SQL query ({len(sql)} chars)")
    print(f"▶ Format: {'Columnar' if columnar_format else 'Legacy JSON'}")
    print(f"▶ Executing in Snowflake...")

    # Execute query
    try:
        df = _normalize_dataframe_columns(run_query(sql))
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

    # Parse results based on format
    if columnar_format:
        results = _parse_columnar_results(df, suite_config, include_failure_details)
    else:
        results = _parse_sql_results(df, suite_config, include_failure_details)

    print(f"✅ Validation complete: {len(results['results'])} rules checked")

    return results


def _normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with all column names normalized to lowercase strings."""

    normalized = df.copy()
    normalized.columns = [str(col).lower() for col in normalized.columns]
    return normalized


def _parse_columnar_results(
    df: pd.DataFrame,
    suite_config: Dict[str, Any],
    include_failure_details: bool = False,
) -> Dict[str, Any]:
    """
    Parse columnar format results where each expectation is a separate column.

    In columnar format, expectation columns are named like:
    - exp_c49b5f_841e = 'PASS' or 'FAIL'
    - derived_abp_data_incomplete = 'PASS' or 'FAIL'

    Args:
        df: DataFrame with one column per expectation
        suite_config: Original suite configuration
        include_failure_details: Include detailed failure information

    Returns:
        Dictionary with same structure as legacy format for backward compatibility
    """
    from core.expectation_metadata import build_expectation_catalog

    if df.empty:
        return {
            "results": [],
            "derived_status_results": [],
            "validated_materials": [],
            "all_validated_materials": [],
            "total_validated_count": 0,
            "full_results_df": df,
        }

    # Get index column
    index_column = (
        suite_config.get("metadata", {}).get("index_column", "material_number").lower()
    )

    # Calculate metadata from DataFrame
    total_validated = 0
    all_validated_materials = []
    if index_column in df.columns and not df.empty:
        all_validated_materials = df[index_column].dropna().unique().tolist()
        total_validated = len(all_validated_materials)

    element_count = len(df)

    # Build expectation catalog from YAML to map IDs to metadata
    # We'll construct a temporary YAML path if needed, or use suite_config directly
    suite_name = suite_config.get("metadata", {}).get("suite_name", "")
    validations = suite_config.get("validations", [])

    # Build catalog mapping exp_id -> {type, column}
    catalog = {}
    for validation in validations:
        val_type = validation.get("type")
        base_id = validation.get("expectation_id")

        # Extract targets
        targets = _extract_validation_targets_from_config(validation)

        for target in targets:
            scoped_id = build_scoped_expectation_id(validation, target)
            catalog[scoped_id] = {
                "expectation_type": val_type,
                "column": target,
                "base_id": base_id,
            }

    # Identify expectation columns in DataFrame
    exp_columns = [col for col in df.columns if col.startswith("exp_")]

    # Build results for each expectation
    results = []
    for exp_id in exp_columns:
        if exp_id not in catalog:
            print(f"Warning: Expectation ID {exp_id} not found in catalog")
            continue

        metadata = catalog[exp_id]

        # Count failures
        unexpected_count = (df[exp_id] == 'FAIL').sum()
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(metadata["column"])

        result = {
            "expectation_type": metadata["expectation_type"],
            "column": metadata["column"],
            "expectation_id": exp_id,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": exp_id,  # The column itself is the flag
            "context_columns": get_context_columns_for_columns([metadata["column"]]),
        }

        # Add failure details if requested
        if include_failure_details and unexpected_count > 0:
            failed_rows = df[df[exp_id] == 'FAIL']
            result["failed_materials"] = _build_columnar_failure_records(
                failed_rows,
                result["context_columns"],
                metadata["column"],
                index_column,
            )

        results.append(result)

    # Handle derived statuses
    derived_status_results = []
    derived_columns = [col for col in df.columns if col.startswith("derived_")]

    for derived_col in derived_columns:
        # Extract status label from column name
        status_label = derived_col.replace("derived_", "").replace("_", " ").title()

        # Count failures
        unexpected_count = (df[derived_col] == 'FAIL').sum()
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        # Get context columns from the first related expectation (approximation)
        context_columns = []
        if results:
            context_columns = results[0].get("context_columns", [])

        result = {
            "expectation_type": "custom:derived_status",
            "column": status_label,
            "status_label": status_label,
            "expectation_id": derived_col,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": None,
            "unique_by": [],
            "flag_column": derived_col,
            "context_columns": context_columns,
        }

        if include_failure_details and unexpected_count > 0:
            failed_rows = df[df[derived_col] == 'FAIL']
            result["failed_materials"] = _build_columnar_failure_records(
                failed_rows,
                context_columns,
                status_label,
                index_column,
            )

        derived_status_results.append(result)

    return {
        "results": results,
        "derived_status_results": derived_status_results,
        "validated_materials": all_validated_materials,
        "all_validated_materials": all_validated_materials,
        "total_validated_count": total_validated,
        "full_results_df": df,
    }


def _extract_validation_targets_from_config(validation: Dict[str, Any]) -> List[str]:
    """Extract target columns from a validation definition."""
    val_type = validation.get("type", "")

    # Single column
    if "column" in validation:
        return [validation["column"]]

    # Multiple columns
    if "columns" in validation:
        return validation["columns"]

    # Column pair
    if val_type.startswith("expect_column_pair"):
        col_a = validation.get("column_a")
        col_b = validation.get("column_b")
        if col_a and col_b:
            return [f"{col_a}|{col_b}"]

    # Rules-based (value_in_set)
    if val_type == "expect_column_values_to_be_in_set":
        rules = validation.get("rules", {})
        return list(rules.keys())

    # Conditional
    if "condition_column" in validation and "required_column" in validation:
        return [f"{validation['condition_column']}|{validation['required_column']}"]

    if "condition_column" in validation and "target_column" in validation:
        return [f"{validation['condition_column']}|{validation['target_column']}"]

    return []


def _build_columnar_failure_records(
    failed_rows: pd.DataFrame,
    context_columns: List[str],
    column_name: str,
    index_column: str,
) -> List[Dict[str, Any]]:
    """Build failure detail records from columnar format."""
    failures = []

    for _, row in failed_rows.iterrows():
        record = {}

        # Add context columns
        for col in context_columns:
            col_lower = col.lower()
            if col_lower in row.index:
                record[col] = row[col_lower]

        # Add material number
        if index_column in row.index:
            record["material_number"] = row[index_column]

        # Add unexpected value (the actual column value)
        col_lower = column_name.lower()
        if col_lower in row.index:
            record["Unexpected Value"] = row[col_lower]

        failures.append(record)

    return failures


def _parse_sql_results(
    df: pd.DataFrame,
    suite_config: Dict[str, Any],
    include_failure_details: bool = False,
) -> Dict[str, Any]:
    """
    Parse Snowflake query results into GX-compatible format.

    Args:
        df: DataFrame containing all validation rows from Snowflake
        suite_config: Original suite configuration

    Returns:
        Dictionary with keys:
        - 'results': list of regular expectation results
        - 'derived_status_results': list of derived status results (aggregated expectations)
        - 'validated_materials': list of all validated material numbers (for backward compat)
        - 'all_validated_materials': list of all validated material numbers (for derived lists)
        - 'total_validated_count': total count of validated materials
        - 'full_results_df': DataFrame from Snowflake (all rows)
    """
    if df.empty:
        return {
            "results": [],
            "derived_status_results": [],
            "validated_materials": [],
            "all_validated_materials": [],
            "total_validated_count": 0,
            "full_results_df": df,
        }

    # Normalize column names to lowercase for easier access
    df = df.copy()
    df.columns = df.columns.str.lower()

    # Get index column for metadata calculation
    index_column = (
        suite_config.get("metadata", {}).get("index_column", "MATERIAL_NUMBER").lower()
    )

    # Calculate metadata from DataFrame (much faster than SQL ARRAY_AGG)
    total_validated = 0
    all_validated_materials = []
    if index_column in df.columns and not df.empty:
        # Get all unique materials from DataFrame
        all_validated_materials = df[index_column].dropna().unique().tolist()
        total_validated = len(all_validated_materials)

    validations = suite_config.get("validations", [])
    derived_statuses = suite_config.get("derived_statuses", [])

    # Initialize the resolver - single source of truth for catalog and derived status resolution
    resolver = DerivedStatusResolver(validations, derived_statuses)

    # Extract catalog from resolver
    expectation_catalog = [
        {"expectation_id": entry["scoped_id"], "type": entry["type"]}
        for entry in resolver.catalog
    ]

    # Build context map from resolver's catalog
    expectation_context_map = {}
    for entry in resolver.catalog:
        scoped_id = entry["scoped_id"]
        targets = entry["targets"]
        if targets:
            expectation_context_map[scoped_id] = get_context_columns_for_columns(targets)

    element_count = len(df)
    counts_map, failure_rows_map = _collect_validation_failures(
        df, expectation_catalog, include_failure_details
    )

    results = []
    for validation in validations:
        val_type = validation.get("type", "")

        if val_type == "expect_column_values_to_not_be_null":
            results.extend(
                _parse_not_null_results(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "expect_column_values_to_be_in_set":
            results.extend(
                _parse_value_in_set_results(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "expect_column_values_to_not_be_in_set":
            results.extend(
                _parse_value_not_in_set_results(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "expect_column_values_to_match_regex":
            results.extend(
                _parse_regex_results(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "expect_column_pair_values_to_be_equal":
            results.append(
                _parse_column_pair_equal_result(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "expect_column_pair_values_a_to_be_greater_than_b":
            results.append(
                _parse_column_pair_greater_result(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "custom:conditional_required":
            results.append(
                _parse_conditional_required_result(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )
        elif val_type == "custom:conditional_value_in_set":
            results.append(
                _parse_conditional_value_in_set_result(
                    df,
                    validation,
                    include_failure_details,
                    counts_map,
                    failure_rows_map,
                    element_count,
                )
            )

    index_column = (
        suite_config.get("metadata", {}).get("index_column", "material_number")
    )

    # validated_materials is same as all_validated_materials since we return all rows
    # Kept for backward compatibility with existing code
    validated_materials = all_validated_materials

    # Build derived status results using the resolver (kept separate from regular results)
    derived_status_results = []
    if derived_statuses:
        derived_status_results = _build_derived_status_results(
            resolver,
            counts_map,
            failure_rows_map,
            expectation_context_map,
            include_failure_details,
            element_count,
            index_column,
        )

    return {
        "results": results,
        "derived_status_results": derived_status_results,
        "validated_materials": validated_materials,  # All validated materials (for backward compat)
        "all_validated_materials": all_validated_materials,  # All validated materials (for derived lists)
        "total_validated_count": total_validated,  # Count of all validated materials
        "full_results_df": df,
    }


def _parse_not_null_results(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> list:
    """Parse not-null validation results from full-width rows."""
    results = []
    columns = validation.get("columns", [])

    for col in columns:
        expectation_id = build_scoped_expectation_id(validation, col)
        unexpected_count = counts_map.get(expectation_id, 0)
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(col)
        result = {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": col,
            "expectation_id": expectation_id,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": "validation_results",
            "context_columns": get_context_columns_for_columns([col]),
        }

        if include_failure_details:
            result["failed_materials"] = _build_failure_records_from_rows(
                failure_rows_map.get(expectation_id, []),
                result["context_columns"],
                extra_fields={"Unexpected Value": col},
            )

        results.append(result)

    return results


def _parse_value_in_set_results(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> list:
    """Parse value-in-set validation results from full-width rows."""
    results = []
    rules = validation.get("rules", {})

    for column, allowed_values in rules.items():
        expectation_id = build_scoped_expectation_id(validation, column)
        unexpected_count = counts_map.get(expectation_id, 0)
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(column)
        result = {
            "expectation_type": "expect_column_values_to_be_in_set",
            "column": column,
            "expectation_id": expectation_id,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": "validation_results",
            "context_columns": get_context_columns_for_columns([column]),
        }

        if include_failure_details:
            result["failed_materials"] = _build_failure_records_from_rows(
                failure_rows_map.get(expectation_id, []),
                result["context_columns"],
                extra_fields={"Unexpected Value": column},
            )

        results.append(result)

    return results


def _parse_value_not_in_set_results(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> list:
    """Parse value-not-in-set validation results from full-width rows."""
    column = validation.get("column")
    if not column:
        return []

    expectation_id = build_scoped_expectation_id(validation, column)
    unexpected_count = counts_map.get(expectation_id, 0)
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(column)
    result = {
        "expectation_type": "expect_column_values_to_not_be_in_set",
        "column": column,
        "expectation_id": expectation_id,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": "validation_results",
        "context_columns": get_context_columns_for_columns([column]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records_from_rows(
            failure_rows_map.get(expectation_id, []),
            result["context_columns"],
            extra_fields={"Unexpected Value": column},
        )

    return [result]


def _parse_regex_results(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> list:
    """Parse regex validation results from full-width rows."""
    results = []
    columns = validation.get("columns", [])

    for column in columns:
        expectation_id = build_scoped_expectation_id(validation, column)
        unexpected_count = counts_map.get(expectation_id, 0)
        unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

        table_grain, unique_by = get_grain_for_column(column)
        result = {
            "expectation_type": "expect_column_values_to_match_regex",
            "column": column,
            "expectation_id": expectation_id,
            "success": unexpected_count == 0,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": "validation_results",
            "context_columns": get_context_columns_for_columns([column]),
        }

        if include_failure_details:
            result["failed_materials"] = _build_failure_records_from_rows(
                failure_rows_map.get(expectation_id, []),
                result["context_columns"],
                extra_fields={"Unexpected Value": column},
            )

        results.append(result)

    return results


def _parse_column_pair_equal_result(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> Dict:
    """Parse column pair equality validation result from full-width rows."""
    col_a = validation.get("column_a")
    col_b = validation.get("column_b")

    expectation_id = build_scoped_expectation_id(validation, f"{col_a}|{col_b}")
    unexpected_count = counts_map.get(expectation_id, 0)
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(col_a)
    result = {
        "expectation_type": "expect_column_pair_values_to_be_equal",
        "column": f"{col_a}|{col_b}",  # Combined column name
        "expectation_id": expectation_id,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": "validation_results",
        "context_columns": get_context_columns_for_columns([col_a, col_b]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records_from_rows(
            failure_rows_map.get(expectation_id, []),
            result["context_columns"],
            extra_fields={col_a: col_a, col_b: col_b},
        )

    return result


def _parse_column_pair_greater_result(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> Dict:
    """Parse column pair greater-than validation result from full-width rows."""
    col_a = validation.get("column_a")
    col_b = validation.get("column_b")

    expectation_id = build_scoped_expectation_id(validation, f"{col_a}|{col_b}")
    unexpected_count = counts_map.get(expectation_id, 0)
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(col_a)
    result = {
        "expectation_type": "expect_column_pair_values_a_to_be_greater_than_b",
        "column": f"{col_a}|{col_b}",
        "expectation_id": expectation_id,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": "validation_results",
        "context_columns": get_context_columns_for_columns([col_a, col_b]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records_from_rows(
            failure_rows_map.get(expectation_id, []),
            result["context_columns"],
            extra_fields={col_a: col_a, col_b: col_b},
        )

    return result


def _parse_conditional_required_result(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> Dict:
    """Parse conditional required validation result from full-width rows."""
    condition_col = validation.get("condition_column")
    required_col = validation.get("required_column")

    expectation_id = build_scoped_expectation_id(
        validation, f"{condition_col}|{required_col}"
    )
    unexpected_count = counts_map.get(expectation_id, 0)
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(required_col)
    result = {
        "expectation_type": "custom:conditional_required",
        "column": required_col,
        "expectation_id": expectation_id,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": "validation_results",
        "context_columns": get_context_columns_for_columns([condition_col, required_col]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records_from_rows(
            failure_rows_map.get(expectation_id, []),
            result["context_columns"],
            extra_fields={condition_col: condition_col, required_col: required_col},
        )

    return result


def _parse_conditional_value_in_set_result(
    df: pd.DataFrame,
    validation: Dict,
    include_failure_details: bool,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    element_count: int,
) -> Dict:
    """Parse conditional value in set validation result from full-width rows."""
    condition_col = validation.get("condition_column")
    target_col = validation.get("target_column")

    expectation_id = build_scoped_expectation_id(
        validation, f"{condition_col}|{target_col}"
    )
    unexpected_count = counts_map.get(expectation_id, 0)
    unexpected_percent = (unexpected_count / element_count * 100) if element_count > 0 else 0.0

    table_grain, unique_by = get_grain_for_column(target_col)
    result = {
        "expectation_type": "custom:conditional_value_in_set",
        "column": target_col,
        "expectation_id": expectation_id,
        "success": unexpected_count == 0,
        "element_count": element_count,
        "unexpected_count": unexpected_count,
        "unexpected_percent": round(unexpected_percent, 2),
        "table_grain": table_grain,
        "unique_by": unique_by,
        "flag_column": "validation_results",
        "context_columns": get_context_columns_for_columns([condition_col, target_col]),
    }

    if include_failure_details:
        result["failed_materials"] = _build_failure_records_from_rows(
            failure_rows_map.get(expectation_id, []),
            result["context_columns"],
            extra_fields={condition_col: condition_col, target_col: target_col},
        )

    return result


def _build_failure_records_from_rows(
    failure_rows: List[pd.Series],
    context_columns: list[str],
    extra_fields: Dict[str, str] | None = None,
) -> list[dict]:
    """Construct failure detail dictionaries from validation_results payloads."""
    extra_fields = extra_fields or {}

    failures: list[dict] = []
    for row in failure_rows:
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


# NOTE: Catalog building and context mapping have been moved to DerivedStatusResolver
# to eliminate code duplication and ensure UI and runtime stay in sync.


def _collect_validation_failures(
    df: pd.DataFrame,
    expectation_catalog: List[Dict[str, Any]],
    include_failure_details: bool,
) -> tuple[Dict[str, int], Dict[str, List[pd.Series]]]:
    """Aggregate unexpected counts and optional failing rows keyed by expectation id."""

    counts_map: Dict[str, int] = {
        entry["expectation_id"]: 0 for entry in expectation_catalog
    }
    failure_rows_map: Dict[str, List[pd.Series]] = {
        entry["expectation_id"]: [] for entry in expectation_catalog
    }

    if "validation_results" not in df.columns:
        return counts_map, failure_rows_map

    for _, row in df.iterrows():
        entries = _parse_json_array(row.get("validation_results"))
        for entry in entries:
            exp_id = entry.get("expectation_id") if isinstance(entry, dict) else None
            if exp_id and exp_id in counts_map:
                counts_map[exp_id] += 1
                if include_failure_details:
                    failure_rows_map[exp_id].append(row)

    return counts_map, failure_rows_map


def _build_derived_status_results(
    resolver: DerivedStatusResolver,
    counts_map: Dict[str, int],
    failure_rows_map: Dict[str, List[pd.Series]],
    expectation_context_map: Dict[str, list[str]],
    include_failure_details: bool,
    element_count: int,
    index_column: str = "material_number",
) -> list[dict]:
    """
    Create synthesized results for derived status labels.

    Uses the DerivedStatusResolver to get pre-resolved scoped IDs,
    eliminating the need for runtime string matching.

    Properly deduplicates materials and tracks which expectations/columns
    each material failed.
    """

    derived_results: list[dict] = []

    # Get all resolved derived statuses from the resolver
    for resolved_status in resolver.get_all_resolved_derived_statuses():
        # Extract pre-resolved scoped IDs (no string matching needed!)
        resolved_ids = resolved_status.get("resolved_scoped_ids", [])
        missing_ids = resolved_status.get("missing_ids", [])

        status_label = resolved_status.get("status") or resolved_status.get("status_label") or "Derived Status"

        if not resolved_ids:
            continue

        # Build a map of material -> {expectations failed, columns failed, row data}
        material_failures: Dict[str, Dict[str, Any]] = {}

        for exp_id in resolved_ids:
            failure_rows = failure_rows_map.get(exp_id, [])

            # Extract column name from scoped expectation ID (if available from catalog)
            failed_column = None
            for catalog_entry in resolver.catalog:
                if catalog_entry["scoped_id"] == exp_id and catalog_entry["targets"]:
                    failed_column = catalog_entry["targets"][0] if len(catalog_entry["targets"]) == 1 else "|".join(catalog_entry["targets"])
                    break

            for row in failure_rows:
                material_id = _get_row_value(row, index_column)
                if not material_id:
                    continue

                if material_id not in material_failures:
                    material_failures[material_id] = {
                        "material": material_id,
                        "failed_expectations": set(),
                        "failed_columns": set(),
                        "row": row,  # Keep first row for context data
                    }

                material_failures[material_id]["failed_expectations"].add(exp_id)
                if failed_column:
                    material_failures[material_id]["failed_columns"].add(failed_column)

        # Count unique materials (FIX: was summing all failure counts before!)
        unexpected_count = len(material_failures)

        if unexpected_count == 0:
            continue

        expectation_type = resolved_status.get(
            "expectation_type", "custom:derived_null_group"
        )
        expectation_id = resolved_status.get("expectation_id") or f"derived::{status_label}"

        # Aggregate context columns from all resolved expectations
        context_columns: set[str] = set()
        for exp_id in resolved_ids:
            context_columns.update(expectation_context_map.get(exp_id, []))

        sorted_context_columns = sorted(context_columns)
        table_grain = None
        unique_by: list[str] = []
        if sorted_context_columns:
            table_grain, unique_by = get_grain_for_column(sorted_context_columns[0])

        unexpected_percent = (
            (unexpected_count / element_count * 100) if element_count > 0 else 0.0
        )

        result = {
            "expectation_type": expectation_type,
            "column": status_label,
            "status_label": status_label,
            "expectation_id": expectation_id,
            "success": False,
            "element_count": element_count,
            "unexpected_count": unexpected_count,
            "unexpected_percent": round(unexpected_percent, 2),
            "table_grain": table_grain,
            "unique_by": unique_by,
            "flag_column": "validation_results",
            "context_columns": sorted_context_columns,
        }

        # Build enriched failure details with expectation/column tracking
        if include_failure_details:
            enriched_failures = []
            for material_id, failure_data in material_failures.items():
                failure_record = {}

                # Add context columns from the row
                row = failure_data["row"]
                for col in sorted_context_columns:
                    failure_record[col] = _get_row_value(row, col)

                # Add the new tracking fields
                failure_record["failed_expectations"] = sorted(failure_data["failed_expectations"])
                failure_record["failed_columns"] = sorted(failure_data["failed_columns"])
                failure_record["failure_count"] = len(failure_data["failed_expectations"])

                enriched_failures.append(failure_record)

            # Sort by failure_count descending (most issues first)
            enriched_failures.sort(key=lambda x: x["failure_count"], reverse=True)
            result["failed_materials"] = enriched_failures

        derived_results.append(result)

    return derived_results


# NOTE: ID resolution has been moved to DerivedStatusResolver.resolve_expectation_ids()
# This eliminates fragile string prefix matching in favor of explicit mappings.


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
