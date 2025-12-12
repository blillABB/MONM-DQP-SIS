"""Lightweight base validation utilities for YAML-driven suites.

This class preserves the minimal API surface that the Streamlit UI and
unit tests rely on after removing the GX-based runner. It focuses on:
- DataFrame column validation
- YAML schema validation
- Converting Snowflake-native results into a reporting-friendly DataFrame
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import yaml


class BaseValidationSuite:
    """Minimal base class to keep validation helpers consistent.

    The class no longer wraps Great Expectations; it provides utilities
    used by the Streamlit UI, YAML validator, and unit tests.
    """

    SUITE_NAME: str = "BaseValidationSuite"
    INDEX_COLUMN: str = "MATERIAL_NUMBER"

    # Supported expectation types for YAML suites
    SUPPORTED_EXPECTATION_TYPES: List[str] = [
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_not_be_in_set",
        "expect_column_values_to_match_regex",
        "expect_column_values_to_not_match_regex",
        "expect_column_pair_values_a_to_be_greater_than_b",
        "expect_column_pair_values_to_be_equal",
        "expect_column_value_lengths_to_equal",
        "expect_column_value_lengths_to_be_between",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_unique",
        "expect_compound_columns_to_be_unique",
    ]

    DEFAULT_RESULT_FORMAT: Dict[str, Any] = {
        "result_format": "COMPLETE",
        "unexpected_index_column_names": ["Material Number"],
        "include_unexpected_rows": True,
        "partial_unexpected_list_size": 0,
    }

    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")

        self.df = df
        self._yaml_validations: List[Dict[str, Any]] = []

        # Ensure the configured INDEX_COLUMN exists
        if self.INDEX_COLUMN not in df.columns:
            available = ", ".join(df.columns)
            raise ValueError(
                f"INDEX_COLUMN '{self.INDEX_COLUMN}' not found in DataFrame. "
                f"Available columns: {available}"
            )

    # ------------------------------------------------------------------
    # Column validation helpers
    # ------------------------------------------------------------------
    def _validate_columns(self, columns: Iterable[str], expectation_type: str) -> None:
        """Validate that required columns are present."""

        if not columns:
            return

        missing = [col for col in columns if col not in self.df.columns]
        if missing:
            available = ", ".join(self.df.columns)
            missing_str = ", ".join(missing)
            raise ValueError(
                f"Columns not found for '{expectation_type}': {missing_str}. "
                f"Available columns: {available}"
            )

    # ------------------------------------------------------------------
    # YAML schema validation
    # ------------------------------------------------------------------
    @classmethod
    def from_yaml(cls, yaml_path: Path) -> "BaseValidationSuite":
        """Load and validate a YAML config, returning an empty suite instance."""

        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)

        cls._validate_yaml_schema(config, str(yaml_path))
        return cls(pd.DataFrame(columns=[cls.INDEX_COLUMN]))

    @classmethod
    def _validate_yaml_schema(cls, config: Any, yaml_path: str) -> None:
        """Validate the structure of a YAML validation configuration."""

        errors: List[str] = []

        if not isinstance(config, dict):
            raise ValueError(
                f"YAML config for {yaml_path} must contain a mapping (dict), "
                f"got {type(config).__name__}"
            )

        metadata = config.get("metadata")
        if metadata is None:
            errors.append("Missing required 'metadata' section")
        elif not isinstance(metadata, dict):
            errors.append("'metadata' must be a mapping (dict)")
        else:
            if not metadata.get("suite_name"):
                errors.append("metadata.suite_name is required")
            if not metadata.get("data_source"):
                errors.append("metadata.data_source is required")

        validations = config.get("validations")
        if validations is None:
            errors.append("Missing required 'validations' section")
        elif not isinstance(validations, list):
            errors.append("'validations' must be a list")
        elif len(validations) == 0:
            errors.append("'validations' list is empty - no rules to execute")
        else:
            for i, validation in enumerate(validations):
                rule_errors = cls._validate_rule(validation, i)
                errors.extend(rule_errors)

        if errors:
            raise ValueError("; ".join(errors))

    @classmethod
    def _validate_rule(cls, validation: Any, index: int) -> List[str]:
        errors: List[str] = []
        prefix = f"validations[{index}]"

        if not isinstance(validation, dict):
            return [f"{prefix}: must be a mapping, got {type(validation).__name__}"]

        val_type = validation.get("type")
        if not val_type:
            errors.append(f"{prefix}: missing required 'type' field")
            return errors

        if val_type not in cls.SUPPORTED_EXPECTATION_TYPES:
            errors.append(
                f"{prefix}: unknown type '{val_type}'. Valid types: "
                + ", ".join(cls.SUPPORTED_EXPECTATION_TYPES)
            )

        requirements = {
            "expect_column_values_to_not_be_null": {"required": ["columns"], "optional": []},
            "expect_column_values_to_be_in_set": {"required": ["rules"], "optional": []},
            "expect_column_values_to_not_be_in_set": {"required": ["column", "value_set"], "optional": []},
            "expect_column_values_to_match_regex": {"required": ["columns", "regex"], "optional": []},
            "expect_column_values_to_not_match_regex": {"required": ["columns", "regex"], "optional": []},
            "expect_column_pair_values_a_to_be_greater_than_b": {"required": ["column_a", "column_b"], "optional": ["or_equal"]},
            "expect_column_pair_values_to_be_equal": {"required": ["column_a", "column_b"], "optional": []},
            "expect_column_value_lengths_to_equal": {"required": ["columns", "value"], "optional": []},
            "expect_column_value_lengths_to_be_between": {"required": ["columns", "min_value", "max_value"], "optional": []},
            "expect_column_values_to_be_between": {"required": ["columns", "min_value", "max_value"], "optional": []},
            "expect_column_values_to_be_unique": {"required": ["columns"], "optional": []},
            "expect_compound_columns_to_be_unique": {"required": ["column_list"], "optional": []},
        }

        required_fields = requirements.get(val_type, {}).get("required", [])
        for field in required_fields:
            if field not in validation or validation[field] is None:
                errors.append(f"{prefix}: '{val_type}' requires '{field}' field")
            elif field == "columns" and not isinstance(validation[field], list):
                errors.append(f"{prefix}: 'columns' must be a list")
            elif field == "column_list" and not isinstance(validation[field], list):
                errors.append(f"{prefix}: 'column_list' must be a list")
            elif field == "rules" and not isinstance(validation[field], dict):
                errors.append(f"{prefix}: 'rules' must be a mapping")
            elif field == "value_set" and not isinstance(validation[field], list):
                errors.append(f"{prefix}: 'value_set' must be a list")

        if val_type == "expect_column_values_to_be_in_set":
            rules = validation.get("rules", {})
            if isinstance(rules, dict):
                for col, values in rules.items():
                    if not isinstance(values, list):
                        errors.append(
                            f"{prefix}: rules['{col}'] must be a list of allowed values"
                        )

        if val_type in ["expect_column_value_lengths_to_equal"]:
            value = validation.get("value")
            if value is not None and not isinstance(value, int):
                errors.append(f"{prefix}: 'value' must be an integer")

        if val_type in ["expect_column_value_lengths_to_be_between", "expect_column_values_to_be_between"]:
            min_val = validation.get("min_value")
            max_val = validation.get("max_value")
            if min_val is not None and max_val is not None:
                if not isinstance(min_val, (int, float)) or not isinstance(max_val, (int, float)):
                    errors.append(f"{prefix}: 'min_value' and 'max_value' must be numbers")
                elif min_val > max_val:
                    errors.append(
                        f"{prefix}: 'min_value' ({min_val}) cannot be greater than "
                        f"'max_value' ({max_val})"
                    )

        return errors

    # ------------------------------------------------------------------
    # Execution placeholder
    # ------------------------------------------------------------------
    def run(self):
        """Placeholder run method."""

        if self.df.empty:
            raise ValueError("DataFrame is empty - nothing to validate")

        return []

    # ------------------------------------------------------------------
    # Result helpers
    # ------------------------------------------------------------------
    @staticmethod
    def results_to_dataframe(
        results: List[Dict[str, Any]],
        full_results_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Convert validation results into a tabular DataFrame."""

        rows: List[Dict[str, Any]] = []
        normalized_df = None

        if isinstance(full_results_df, pd.DataFrame):
            normalized_df = full_results_df.copy()
            normalized_df.columns = normalized_df.columns.str.lower()

        for result in results or []:
            failures = result.get("failed_materials")

            # Backward compatibility: if failure details were materialized, use them
            if failures is not None:
                # Handle both old format (simple strings) and new format (dicts with context)
                for failure in failures or [None]:
                    if isinstance(failure, dict):
                        # New format: extract material number and unexpected value from dict
                        material_number = failure.get("material_number") or failure.get("MATERIAL_NUMBER")
                        unexpected_value = failure.get("Unexpected Value")
                        # Add all context columns to the row
                        row = {
                            "Expectation Type": result.get("expectation_type"),
                            "Column": result.get("column"),
                            "Material Number": material_number,
                            "Unexpected Value": unexpected_value,
                            "Element Count": result.get("element_count", 0),
                            "Unexpected Count": result.get("unexpected_count", 0),
                            "Unexpected Percent": result.get("unexpected_percent", 0.0),
                            "Status": "Pass" if result.get("success") else "Fail",
                        }
                        # Add all other context fields from the failure dict
                        for key, value in failure.items():
                            if key not in row and key not in ["material_number", "MATERIAL_NUMBER", "Unexpected Value"]:
                                row[key] = value
                        rows.append(row)
                    else:
                        # Old format: simple material number string
                        rows.append({
                            "Expectation Type": result.get("expectation_type"),
                            "Column": result.get("column"),
                            "Material Number": failure,
                            "Unexpected Value": failure,
                            "Element Count": result.get("element_count", 0),
                            "Unexpected Count": result.get("unexpected_count", 0),
                            "Unexpected Percent": result.get("unexpected_percent", 0.0),
                            "Status": "Pass" if result.get("success") else "Fail",
                        })
                continue

            # New contract: derive failure rows from the full results DataFrame
            if normalized_df is not None:
                flag_col = (result.get("flag_column") or "").lower()
                context_columns = [c.lower() for c in (result.get("context_columns") or [])]

                if flag_col and flag_col in normalized_df.columns:
                    flagged_rows = normalized_df[normalized_df[flag_col] == 1]

                    if flagged_rows.empty:
                        rows.append({
                            "Expectation Type": result.get("expectation_type"),
                            "Column": result.get("column"),
                            "Material Number": None,
                            "Unexpected Value": None,
                            "Element Count": result.get("element_count", 0),
                            "Unexpected Count": result.get("unexpected_count", 0),
                            "Unexpected Percent": result.get("unexpected_percent", 0.0),
                            "Status": "Pass" if result.get("success") else "Fail",
                        })
                        continue

                    def create_record(row):
                        """Create a record from a flagged row."""
                        record = {
                            "Expectation Type": result.get("expectation_type"),
                            "Column": result.get("column"),
                            "Material Number": row.get("material_number"),
                            "Unexpected Value": row.get(result.get("column", "").lower()),
                            "Element Count": result.get("element_count", 0),
                            "Unexpected Count": result.get("unexpected_count", 0),
                            "Unexpected Percent": result.get("unexpected_percent", 0.0),
                            "Status": "Pass" if result.get("success") else "Fail",
                        }
                        for col in context_columns:
                            record[col] = row.get(col)
                        return record

                    # Use df.apply instead of iterrows for better performance
                    flagged_records = flagged_rows.apply(create_record, axis=1)
                    rows.extend(flagged_records.tolist())
            else:
                # No failure materialization available; record aggregate-level summary row
                rows.append({
                    "Expectation Type": result.get("expectation_type"),
                    "Column": result.get("column"),
                    "Material Number": None,
                    "Unexpected Value": None,
                    "Element Count": result.get("element_count", 0),
                    "Unexpected Count": result.get("unexpected_count", 0),
                    "Unexpected Percent": result.get("unexpected_percent", 0.0),
                    "Status": "Pass" if result.get("success") else "Fail",
                })

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)


__all__ = ["BaseValidationSuite"]
