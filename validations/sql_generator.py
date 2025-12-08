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
        self.validations = suite_config.get("validations", [])
        self.index_column = self.metadata.get("index_column", "MATERIAL_NUMBER")

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
        select_columns = self._build_select_clause(validated_columns, context_columns)
        validation_sql = self._build_validation_logic()
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
SELECT
  {validation_sql}
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
                            context_columns: List[str]) -> str:
        """Build SELECT clause with validated columns + context."""
        all_columns = validated_columns + context_columns
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in all_columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)

        return ",\n    ".join(unique_columns)

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

    def _build_validation_logic(self) -> str:
        """Build validation SQL for all rules."""
        validation_parts = []

        for validation in self.validations:
            val_type = validation.get("type", "")

            if val_type == "expect_column_values_to_not_be_null":
                validation_parts.extend(self._build_not_null_validation(validation))
            elif val_type == "expect_column_values_to_be_in_set":
                validation_parts.extend(self._build_value_in_set_validation(validation))
            elif val_type == "expect_column_values_to_not_be_in_set":
                validation_parts.extend(self._build_value_not_in_set_validation(validation))
            elif val_type == "expect_column_values_to_match_regex":
                validation_parts.extend(self._build_regex_validation(validation))
            elif val_type == "expect_column_pair_values_to_be_equal":
                validation_parts.append(self._build_column_pair_equal_validation(validation))
            elif val_type == "expect_column_pair_values_a_to_be_greater_than_b":
                validation_parts.append(self._build_column_pair_greater_validation(validation))
            elif val_type == "custom:conditional_required":
                validation_parts.append(self._build_conditional_required_validation(validation))
            elif val_type == "custom:conditional_value_in_set":
                validation_parts.append(self._build_conditional_value_in_set_validation(validation))

        return ",\n\n  ".join(validation_parts)

    def _build_not_null_validation(self, validation: Dict) -> List[str]:
        """Build SQL for not-null validation."""
        columns = validation.get("columns", [])
        parts = []

        for col in columns:
            safe_col_name = col.lower().replace('"', '')
            quoted_col = f'"{col}"'

            # Get grain-specific context for this column
            col_context = get_context_columns_for_columns([col])
            context_fields = self._build_context_fields(col_context, quoted_col)

            parts.append(f"""-- Validation for {col} (not null)
  COUNT(*) as {safe_col_name}_total,
  SUM(CASE WHEN {quoted_col} IS NULL THEN 1 ELSE 0 END) as {safe_col_name}_null_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_col} IS NULL
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_col_name}_failures""")

        return parts

    def _build_value_in_set_validation(self, validation: Dict) -> List[str]:
        """Build SQL for value-in-set validation."""
        rules = validation.get("rules", {})
        parts = []

        for column, allowed_values in rules.items():
            safe_col_name = column.lower().replace('"', '')
            quoted_col = f'"{column}"'

            # Format value set for SQL
            if isinstance(allowed_values, list):
                value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                                     for v in allowed_values)
            else:
                value_set = f"'{allowed_values}'"

            # Get grain-specific context
            col_context = get_context_columns_for_columns([column])
            context_fields = self._build_context_fields(col_context, quoted_col)

            parts.append(f"""-- Validation for {column} (value in set)
  SUM(CASE WHEN {quoted_col} NOT IN ({value_set}) THEN 1 ELSE 0 END) as {safe_col_name}_invalid_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_col} NOT IN ({value_set})
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_col_name}_failures""")

        return parts

    def _build_value_not_in_set_validation(self, validation: Dict) -> List[str]:
        """Build SQL for value-not-in-set validation."""
        column = validation.get("column")
        forbidden_values = validation.get("value_set", [])

        if not column:
            return []

        safe_col_name = column.lower().replace('"', '')
        quoted_col = f'"{column}"'

        # Format value set for SQL
        value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                             for v in forbidden_values)

        # Get grain-specific context
        col_context = get_context_columns_for_columns([column])
        context_fields = self._build_context_fields(col_context, quoted_col)

        return [f"""-- Validation for {column} (value not in set)
  SUM(CASE WHEN {quoted_col} IN ({value_set}) THEN 1 ELSE 0 END) as {safe_col_name}_forbidden_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_col} IN ({value_set})
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_col_name}_failures"""]

    def _build_regex_validation(self, validation: Dict) -> List[str]:
        """Build SQL for regex validation."""
        columns = validation.get("columns", [])
        regex_pattern = validation.get("regex", "")
        parts = []

        for column in columns:
            safe_col_name = column.lower().replace('"', '')
            quoted_col = f'"{column}"'

            # Get grain-specific context
            col_context = get_context_columns_for_columns([column])
            context_fields = self._build_context_fields(col_context, quoted_col)

            # Escape single quotes in regex pattern
            escaped_pattern = regex_pattern.replace("'", "''")

            parts.append(f"""-- Validation for {column} (regex match)
  SUM(CASE WHEN NOT RLIKE({quoted_col}, '{escaped_pattern}') THEN 1 ELSE 0 END) as {safe_col_name}_regex_fail_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN NOT RLIKE({quoted_col}, '{escaped_pattern}')
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_col_name}_failures""")

        return parts

    def _build_column_pair_equal_validation(self, validation: Dict) -> str:
        """Build SQL for column pair equality validation."""
        col_a = validation.get("column_a")
        col_b = validation.get("column_b")

        safe_name = f"{col_a}_{col_b}_equal".lower().replace('"', '')
        quoted_a = f'"{col_a}"'
        quoted_b = f'"{col_b}"'

        # Get context for both columns
        col_context = get_context_columns_for_columns([col_a, col_b])
        context_fields = self._build_context_fields(col_context, None,
                                                    extra_fields={col_a: quoted_a, col_b: quoted_b})

        return f"""-- Validation for {col_a} = {col_b}
  SUM(CASE
    WHEN {quoted_a} != {quoted_b}
      OR ({quoted_a} IS NULL AND {quoted_b} IS NOT NULL)
      OR ({quoted_a} IS NOT NULL AND {quoted_b} IS NULL)
    THEN 1 ELSE 0
  END) as {safe_name}_mismatch_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_a} != {quoted_b}
        OR ({quoted_a} IS NULL AND {quoted_b} IS NOT NULL)
        OR ({quoted_a} IS NOT NULL AND {quoted_b} IS NULL)
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_name}_failures"""

    def _build_column_pair_greater_validation(self, validation: Dict) -> str:
        """Build SQL for column pair greater-than validation."""
        col_a = validation.get("column_a")
        col_b = validation.get("column_b")
        or_equal = validation.get("or_equal", False)

        safe_name = f"{col_a}_{col_b}_greater".lower().replace('"', '')
        quoted_a = f'"{col_a}"'
        quoted_b = f'"{col_b}"'

        # Build comparison operator
        operator = ">=" if or_equal else ">"

        # Get context for both columns
        col_context = get_context_columns_for_columns([col_a, col_b])
        context_fields = self._build_context_fields(col_context, None,
                                                    extra_fields={col_a: quoted_a, col_b: quoted_b})

        return f"""-- Validation for {col_a} {operator} {col_b}
  SUM(CASE
    WHEN {quoted_a} < {quoted_b}
      OR {quoted_a} IS NULL
      OR {quoted_b} IS NULL
    THEN 1 ELSE 0
  END) as {safe_name}_fail_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_a} < {quoted_b}
        OR {quoted_a} IS NULL
        OR {quoted_b} IS NULL
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_name}_failures"""

    def _build_conditional_required_validation(self, validation: Dict) -> str:
        """Build SQL for conditional required validation."""
        condition_col = validation.get("condition_column")
        condition_values = validation.get("condition_values", [])
        required_col = validation.get("required_column")

        safe_name = f"{condition_col}_{required_col}_conditional".lower().replace('"', '')
        quoted_condition = f'"{condition_col}"'
        quoted_required = f'"{required_col}"'

        # Format condition values
        value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                             for v in condition_values)

        # Get context for both columns
        col_context = get_context_columns_for_columns([condition_col, required_col])
        context_fields = self._build_context_fields(col_context, None,
                                                    extra_fields={
                                                        condition_col: quoted_condition,
                                                        required_col: quoted_required
                                                    })

        return f"""-- Validation for conditional required: if {condition_col} in ({value_set}) then {required_col} required
  SUM(CASE
    WHEN {quoted_condition} IN ({value_set}) AND {quoted_required} IS NULL
    THEN 1 ELSE 0
  END) as {safe_name}_violation_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_condition} IN ({value_set}) AND {quoted_required} IS NULL
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_name}_failures"""

    def _build_conditional_value_in_set_validation(self, validation: Dict) -> str:
        """Build SQL for conditional value in set validation."""
        condition_col = validation.get("condition_column")
        condition_values = validation.get("condition_values", [])
        target_col = validation.get("target_column")
        allowed_values = validation.get("allowed_values", [])

        safe_name = f"{condition_col}_{target_col}_conditional_set".lower().replace('"', '')
        quoted_condition = f'"{condition_col}"'
        quoted_target = f'"{target_col}"'

        # Format value sets
        condition_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                                 for v in condition_values)
        allowed_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                               for v in allowed_values)

        # Get context for both columns
        col_context = get_context_columns_for_columns([condition_col, target_col])
        context_fields = self._build_context_fields(col_context, None,
                                                    extra_fields={
                                                        condition_col: quoted_condition,
                                                        target_col: quoted_target
                                                    })

        return f"""-- Validation for conditional value in set: if {condition_col} in ({condition_set}) then {target_col} in ({allowed_set})
  SUM(CASE
    WHEN {quoted_condition} IN ({condition_set})
      AND {quoted_target} NOT IN ({allowed_set})
    THEN 1 ELSE 0
  END) as {safe_name}_violation_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN {quoted_condition} IN ({condition_set})
        AND {quoted_target} NOT IN ({allowed_set})
      THEN OBJECT_CONSTRUCT(
        {context_fields}
      )
      ELSE NULL
    END
  )) as {safe_name}_failures"""

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
