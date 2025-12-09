"""
Dynamic SQL Generator for Snowflake-Native Validation

This module generates complete Snowflake SQL queries from YAML validation configurations.
Supports all standard GX expectations plus custom conditional validations.

Key features:
- Generates single SQL query that validates all rules
- Uses grain-based context columns (only includes what's needed)
- Produces GX-compatible output format
- No persisted queries - generated on-demand from rules
"""

import hashlib
from typing import Dict, List, Any
from core.grain_mapping import get_context_columns_for_columns


class ValidationSQLGenerator:
    """
    Generates Snowflake SQL from validation configuration.

    Takes a suite configuration (parsed from YAML) and produces a complete
    SQL query that performs all validations in a single execution.
    """

    def __init__(self, suite_config: Dict[str, Any]):
        """
        Initialize generator with suite configuration.

        Args:
            suite_config: Dictionary containing:
                - metadata: suite_name, description, index_column
                - data_source: table, filters
                - validations: list of validation rules
        """
        self.suite_config = suite_config
        self.metadata = suite_config.get("metadata", {})
        self.data_source = suite_config.get("data_source", {})
        self.validations = _annotate_expectation_ids(
            suite_config.get("validations", []), self.metadata.get("suite_name", "")
        )
        self.index_column = self.metadata.get("index_column", "MATERIAL_NUMBER")
        # Deprecated: failure arrays are no longer constructed in-SQL since we now
        # return full-width validation rows. Kept for backward compatibility with
        # legacy YAMLs but ignored by the generator.
        self.include_failure_arrays = self.metadata.get("include_failure_arrays", False)

    def generate_sql(self, limit: int = None) -> str:
        """
        Generate complete SQL query for all validations.

        Args:
            limit: Optional row limit for testing

        Returns:
            Complete SQL query string
        """
        # Collect all columns being validated
        validated_columns = self._collect_validated_columns()

        # Get minimal context columns needed (union of all grains)
        context_columns = get_context_columns_for_columns(validated_columns)

        # Build parts of the query
        table_name = self._get_table_name()
        where_clause = self._build_where_clause()
        validation_results_clause = self._build_validation_results_clause()
        select_columns = self._build_select_clause(
            validated_columns, context_columns, extra_columns=[validation_results_clause]
        )
        select_keyword = "SELECT DISTINCT" if self._use_distinct() else "SELECT"

        # Assemble complete query
        query = f"""
WITH base_data AS (
  {select_keyword}
    {select_columns}
  FROM {table_name}
  {where_clause}
  {f'LIMIT {limit}' if limit else ''}
)
SELECT *
FROM base_data
"""
        return query.strip()

    def _get_table_name(self) -> str:
        """Get source table name with default fallback."""
        if isinstance(self.data_source, str):
            # Old format: data_source is a query function name
            # For now, default to vw_ProductDataAll
            return 'PROD_MO_MONM.REPORTING."vw_ProductDataAll"'

        return self.data_source.get("table", 'PROD_MO_MONM.REPORTING."vw_ProductDataAll"')

    def _build_where_clause(self) -> str:
        """Build WHERE clause from data source filters."""
        if isinstance(self.data_source, str):
            # Old format - can't build WHERE clause
            return ""

        filters = self.data_source.get("filters", {})
        if not filters:
            return ""

        conditions = []
        for column, condition in filters.items():
            # Handle different condition formats
            if isinstance(condition, str):
                if condition.startswith(("LIKE", "IN", "=", "<", ">", "!=")):
                    # Already has operator
                    conditions.append(f"{column} {condition}")
                else:
                    # Assume equality
                    conditions.append(f"{column} = '{condition}'")
            elif isinstance(condition, list):
                # List implies IN clause
                values = ', '.join(f"'{v}'" for v in condition)
                conditions.append(f"{column} IN ({values})")

        if conditions:
            return "WHERE " + " AND ".join(conditions)
        return ""

    def _build_select_clause(self, validated_columns: List[str],
                            context_columns: List[str],
                            extra_columns: List[str] = None) -> str:
        """Build SELECT clause with validated columns + context."""
        all_columns = validated_columns + context_columns
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in all_columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)

        combined_columns = unique_columns + (extra_columns or [])

        return ",\n    ".join(combined_columns)

    def _use_distinct(self) -> bool:
        """Determine whether to apply SELECT DISTINCT for the base data set."""
        if isinstance(self.data_source, dict):
            return bool(self.data_source.get("distinct", False))
        return False

    def _collect_validated_columns(self) -> List[str]:
        """Collect all columns being validated."""
        columns = []
        for validation in self.validations:
            val_type = validation.get("type", "")

            if "columns" in validation:
                # Multiple columns
                columns.extend(validation["columns"])
            elif "column" in validation:
                # Single column
                columns.append(validation["column"])
            elif val_type.startswith("expect_column_pair"):
                # Column pair validations
                columns.append(validation.get("column_a"))
                columns.append(validation.get("column_b"))
            elif "condition_column" in validation:
                # Conditional validations
                columns.append(validation.get("condition_column"))
                if "required_column" in validation:
                    columns.append(validation["required_column"])
                if "target_column" in validation:
                    columns.append(validation["target_column"])

        # Remove None values and duplicates
        return list(set(col for col in columns if col))

    def _build_validation_results_clause(self) -> str:
        """Build ARRAY_CONSTRUCT of validation failure objects."""
        validation_objects: list[str] = []

        for validation in self.validations:
            val_type = validation.get("type", "")

            if val_type == "expect_column_values_to_not_be_null":
                validation_objects.extend(self._build_not_null_validation(validation))
            elif val_type == "expect_column_values_to_be_in_set":
                validation_objects.extend(self._build_value_in_set_validation(validation))
            elif val_type == "expect_column_values_to_not_be_in_set":
                validation_objects.extend(self._build_value_not_in_set_validation(validation))
            elif val_type == "expect_column_values_to_match_regex":
                validation_objects.extend(self._build_regex_validation(validation))
            elif val_type == "expect_column_pair_values_to_be_equal":
                validation_objects.extend(self._build_column_pair_equal_validation(validation))
            elif val_type == "expect_column_pair_values_a_to_be_greater_than_b":
                validation_objects.extend(self._build_column_pair_greater_validation(validation))
            elif val_type == "custom:conditional_required":
                validation_objects.extend(
                    self._build_conditional_required_validation(validation)
                )
            elif val_type == "custom:conditional_value_in_set":
                validation_objects.extend(
                    self._build_conditional_value_in_set_validation(validation)
                )

        if not validation_objects:
            return "ARRAY_CONSTRUCT() AS validation_results"

        objects_clause = ",\n    ".join(validation_objects)
        return f"ARRAY_COMPACT(ARRAY_CONSTRUCT(\n    {objects_clause}\n  )) AS validation_results"

    def _build_not_null_validation(self, validation: Dict) -> List[str]:
        """Build SQL for not-null validation flags."""
        columns = validation.get("columns", [])
        objects: List[str] = []

        for col in columns:
            quoted_col = f'"{col}"'
            expectation_id = build_scoped_expectation_id(validation, col)

            objects.append(
                "CASE WHEN {col} IS NULL THEN OBJECT_CONSTRUCT("
                "'expectation_id', '{expectation_id}', "
                "'expectation_type', 'expect_column_values_to_not_be_null', "
                "'column', '{col}', "
                "'failure_reason', 'NULL_VALUE', "
                "'unexpected_value', {quoted_col}"
                ") END".format(col=quoted_col, expectation_id=expectation_id, col_name=col)
            )

        return objects

    def _build_value_in_set_validation(self, validation: Dict) -> List[str]:
        """Build SQL for value-in-set validation flags."""
        rules = validation.get("rules", {})
        objects: List[str] = []

        for column, allowed_values in rules.items():
            quoted_col = f'"{column}"'
            expectation_id = build_scoped_expectation_id(validation, column)

            # Format value set for SQL
            if isinstance(allowed_values, list):
                value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                                     for v in allowed_values)
            else:
                value_set = f"'{allowed_values}'"

            objects.append(
                "CASE WHEN {col} NOT IN ({value_set}) THEN OBJECT_CONSTRUCT("
                "'expectation_id', '{expectation_id}', "
                "'expectation_type', 'expect_column_values_to_be_in_set', "
                "'column', '{column}', "
                "'failure_reason', 'VALUE_NOT_IN_SET', "
                "'unexpected_value', {col}, "
                "'allowed_values', ARRAY_CONSTRUCT({allowed_values})"
                ") END".format(
                    col=quoted_col,
                    value_set=value_set,
                    expectation_id=expectation_id,
                    column=column,
                    allowed_values=value_set,
                )
            )

        return objects

    def _build_value_not_in_set_validation(self, validation: Dict) -> List[str]:
        """Build SQL for value-not-in-set validation flags."""
        column = validation.get("column")
        forbidden_values = validation.get("value_set", [])

        if not column:
            return []

        quoted_col = f'"{column}"'
        expectation_id = build_scoped_expectation_id(validation, column)

        # Format value set for SQL
        value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                             for v in forbidden_values)

        object_expr = (
            "CASE WHEN {col} IN ({value_set}) THEN OBJECT_CONSTRUCT("
            "'expectation_id', '{expectation_id}', "
            "'expectation_type', 'expect_column_values_to_not_be_in_set', "
            "'column', '{column}', "
            "'failure_reason', 'VALUE_IN_FORBIDDEN_SET', "
            "'unexpected_value', {col}, "
            "'forbidden_values', ARRAY_CONSTRUCT({forbidden_values})"
            ") END".format(
                col=quoted_col,
                value_set=value_set,
                expectation_id=expectation_id,
                column=column,
                forbidden_values=value_set,
            )
        )

        return [object_expr]

    def _build_regex_validation(self, validation: Dict) -> List[str]:
        """Build SQL for regex validation flags."""
        columns = validation.get("columns", [])
        regex_pattern = validation.get("regex", "")
        objects: List[str] = []

        for column in columns:
            quoted_col = f'"{column}"'
            expectation_id = build_scoped_expectation_id(validation, column)

            # Get grain-specific context
            # Escape single quotes in regex pattern
            escaped_pattern = regex_pattern.replace("'", "''")

            objects.append(
                "CASE WHEN NOT RLIKE({col}, '{pattern}') THEN OBJECT_CONSTRUCT("
                "'expectation_id', '{expectation_id}', "
                "'expectation_type', 'expect_column_values_to_match_regex', "
                "'column', '{column}', "
                "'failure_reason', 'REGEX_MISMATCH', "
                "'unexpected_value', {col}, "
                "'regex', '{pattern}'"
                ") END".format(
                    col=quoted_col,
                    pattern=escaped_pattern,
                    expectation_id=expectation_id,
                    column=column,
                )
            )

        return objects

    def _build_column_pair_equal_validation(self, validation: Dict) -> List[str]:
        """Build SQL for column pair equality validation flags."""
        col_a = validation.get("column_a")
        col_b = validation.get("column_b")

        quoted_a = f'"{col_a}"'
        quoted_b = f'"{col_b}"'
        expectation_id = build_scoped_expectation_id(validation, f"{col_a}|{col_b}")

        object_expr = (
            "CASE\n    WHEN {a} != {b}\n      OR ({a} IS NULL AND {b} IS NOT NULL)\n      OR ({a} IS NOT NULL AND {b} IS NULL)\n    THEN OBJECT_CONSTRUCT("
            "'expectation_id', '{expectation_id}', "
            "'expectation_type', 'expect_column_pair_values_to_be_equal', "
            "'columns', ARRAY_CONSTRUCT('{col_a}', '{col_b}'), "
            "'failure_reason', 'VALUES_NOT_EQUAL', "
            "'unexpected_value_a', {a}, "
            "'unexpected_value_b', {b}"
            ") END\n  "
        ).format(a=quoted_a, b=quoted_b, expectation_id=expectation_id, col_a=col_a, col_b=col_b)

        return [object_expr]

    def _build_column_pair_greater_validation(self, validation: Dict) -> List[str]:
        """Build SQL for column pair greater-than validation flags."""
        col_a = validation.get("column_a")
        col_b = validation.get("column_b")
        or_equal = validation.get("or_equal", False)

        quoted_a = f'"{col_a}"'
        quoted_b = f'"{col_b}"'
        expectation_id = build_scoped_expectation_id(validation, f"{col_a}|{col_b}")

        # Build comparison operator
        operator = ">=" if or_equal else ">"

        object_expr = (
            "CASE\n    WHEN {a} < {b}\n      OR {a} IS NULL\n      OR {b} IS NULL\n    THEN OBJECT_CONSTRUCT("
            "'expectation_id', '{expectation_id}', "
            "'expectation_type', 'expect_column_pair_values_a_to_be_greater_than_b', "
            "'columns', ARRAY_CONSTRUCT('{col_a}', '{col_b}'), "
            "'failure_reason', 'VALUE_NOT_GREATER', "
            "'unexpected_value_a', {a}, "
            "'unexpected_value_b', {b}, "
            "'or_equal', {or_equal}""\n    ) END\n  "
        ).format(
            a=quoted_a,
            b=quoted_b,
            expectation_id=expectation_id,
            col_a=col_a,
            col_b=col_b,
            or_equal=str(or_equal).upper(),
        )

        return [object_expr]

    def _build_conditional_required_validation(self, validation: Dict) -> List[str]:
        """Build SQL for conditional required validation flags."""
        condition_col = validation.get("condition_column")
        condition_values = validation.get("condition_values", [])
        required_col = validation.get("required_column")

        quoted_condition = f'"{condition_col}"'
        quoted_required = f'"{required_col}"'
        expectation_id = build_scoped_expectation_id(validation, f"{condition_col}|{required_col}")

        # Format condition values
        value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                             for v in condition_values)

        object_expr = (
            "CASE\n    WHEN {condition} IN ({value_set}) AND {required} IS NULL\n    THEN OBJECT_CONSTRUCT("
            "'expectation_id', '{expectation_id}', "
            "'expectation_type', 'custom:conditional_required', "
            "'columns', ARRAY_CONSTRUCT('{condition_col}', '{required_col}'), "
            "'failure_reason', 'MISSING_REQUIRED_WHEN_CONDITION_MET', "
            "'condition_values', ARRAY_CONSTRUCT({condition_values}), "
            "'unexpected_condition_value', {condition}, "
            "'unexpected_required_value', {required}"
            ") END\n  "
        ).format(
            condition=quoted_condition,
            required=quoted_required,
            value_set=value_set,
            expectation_id=expectation_id,
            condition_col=condition_col,
            required_col=required_col,
            condition_values=value_set,
        )

        return [object_expr]

    def _build_conditional_value_in_set_validation(self, validation: Dict) -> List[str]:
        """Build SQL for conditional value in set validation flags."""
        condition_col = validation.get("condition_column")
        condition_values = validation.get("condition_values", [])
        target_col = validation.get("target_column")
        allowed_values = validation.get("allowed_values", [])

        quoted_condition = f'"{condition_col}"'
        quoted_target = f'"{target_col}"'
        expectation_id = build_scoped_expectation_id(validation, f"{condition_col}|{target_col}")

        # Format value sets
        condition_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                                 for v in condition_values)
        allowed_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                               for v in allowed_values)

        object_expr = (
            "CASE\n    WHEN {condition} IN ({condition_set})\n      AND {target} NOT IN ({allowed_set})\n    THEN OBJECT_CONSTRUCT("
            "'expectation_id', '{expectation_id}', "
            "'expectation_type', 'custom:conditional_value_in_set', "
            "'columns', ARRAY_CONSTRUCT('{condition_col}', '{target_col}'), "
            "'failure_reason', 'VALUE_NOT_IN_ALLOWED_SET_WHEN_CONDITION_MET', "
            "'condition_values', ARRAY_CONSTRUCT({condition_values}), "
            "'allowed_values', ARRAY_CONSTRUCT({allowed_values}), "
            "'unexpected_condition_value', {condition}, "
            "'unexpected_target_value', {target}"
            ") END\n  "
        ).format(
            condition=quoted_condition,
            target=quoted_target,
            condition_set=condition_set,
            allowed_set=allowed_set,
            expectation_id=expectation_id,
            condition_col=condition_col,
            target_col=target_col,
            condition_values=condition_set,
            allowed_values=allowed_set,
        )

        return [object_expr]

    def _build_context_fields(self, context_cols: List[str],
                             unexpected_col: str = None,
                             extra_fields: Dict[str, str] = None) -> str:
        """
        Build OBJECT_CONSTRUCT fields for failed materials.

        Args:
            context_cols: Grain-specific context columns
            unexpected_col: Column with unexpected value (quoted)
            extra_fields: Additional fields to include {name: quoted_column}
        """
        fields = []

        # Add context columns
        for col in context_cols:
            fields.append(f"'{col}', \"{col}\"")

        # Add unexpected value if provided
        if unexpected_col:
            fields.append(f"'Unexpected Value', {unexpected_col}")

        # Add any extra fields
        if extra_fields:
            for name, quoted_col in extra_fields.items():
                fields.append(f"'{name}', {quoted_col}")

        return ",\n        ".join(fields)


def _annotate_expectation_ids(validations: List[Dict[str, Any]], suite_name: str) -> List[Dict[str, Any]]:
    """Attach deterministic expectation IDs so SQL and parser stay aligned."""

    annotated = []
    for idx, validation in enumerate(validations):
        val_copy = dict(validation)
        raw_id = f"{suite_name}|{idx}|{validation.get('type', '')}"
        expectation_id = hashlib.md5(raw_id.encode()).hexdigest()[:12]
        val_copy["expectation_id"] = f"exp_{expectation_id}"
        annotated.append(val_copy)

    return annotated


def build_scoped_expectation_id(validation: Dict[str, Any], discriminator: str) -> str:
    """Create a stable expectation id for a specific validation target."""

    base_id = validation.get("expectation_id", "")
    raw_scope = f"{base_id}|{discriminator}"
    scoped_hash = hashlib.md5(raw_scope.encode()).hexdigest()[:8]
    return f"{base_id}_{scoped_hash}"
