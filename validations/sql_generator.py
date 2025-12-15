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

        # Build derived group CTEs for conditional validations
        derived_group_ctes = self._build_derived_group_ctes(table_name, where_clause)

        validation_results_clause = self._build_validation_results_clause()
        select_columns = self._build_select_clause(
            validated_columns, context_columns, extra_columns=[validation_results_clause]
        )
        select_keyword = "SELECT DISTINCT" if self._use_distinct() else "SELECT"

        # Get index column for metadata calculation
        index_column = self.index_column or "MATERIAL_NUMBER"

        # Assemble complete query with derived group CTEs if needed
        cte_prefix = derived_group_ctes + ",\n" if derived_group_ctes else ""

        query = f"""
WITH {cte_prefix}base_data AS (
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

    def _get_referenced_derived_groups(self) -> Dict[str, Dict[str, Any]]:
        """
        Identify all derived groups referenced in conditional validations.

        Returns:
            Dictionary mapping derived_group_id -> derived_status_config
        """
        referenced_groups = {}

        # Scan validations for conditional_on clauses
        for validation in self.validations:
            conditional_on = validation.get("conditional_on")
            if conditional_on:
                derived_group_id = conditional_on.get("derived_group")
                if derived_group_id and derived_group_id not in referenced_groups:
                    # Find the derived status configuration
                    for derived_status in self.suite_config.get("derived_statuses", []):
                        if derived_status.get("expectation_id") == derived_group_id:
                            referenced_groups[derived_group_id] = derived_status
                            break

        return referenced_groups

    def _build_derived_group_ctes(self, table_name: str, where_clause: str) -> str:
        """
        Build CTEs that identify materials belonging to each derived group.

        Args:
            table_name: Source table name
            where_clause: WHERE clause from main query

        Returns:
            SQL string with all derived group CTEs
        """
        referenced_groups = self._get_referenced_derived_groups()

        if not referenced_groups:
            return ""

        ctes = []
        for group_id, group_config in referenced_groups.items():
            cte_sql = self._build_single_derived_group_cte(
                group_id, group_config, table_name, where_clause
            )
            if cte_sql:
                ctes.append(cte_sql)

        return ",\n".join(ctes)

    def _build_single_derived_group_cte(
        self, group_id: str, group_config: Dict[str, Any], table_name: str, where_clause: str
    ) -> str:
        """
        Build a CTE for a single derived group.

        The CTE selects all materials (index column values) that fail
        any of the expectations in the derived group.

        Args:
            group_id: The expectation_id of the derived group
            group_config: The derived status configuration
            table_name: Source table name
            where_clause: WHERE clause from main query

        Returns:
            SQL CTE string
        """
        expectation_type = group_config.get("expectation_type")
        columns = group_config.get("columns", [])

        if not columns:
            return ""

        # Build conditions based on expectation type
        conditions = []

        if expectation_type == "expect_column_values_to_not_be_null":
            # Material is in group if ANY of the columns is NULL
            for col in columns:
                conditions.append(f"{col.upper()} IS NULL")

        elif expectation_type == "expect_column_values_to_match_regex":
            regex = group_config.get("regex", "")
            escaped_pattern = regex.replace("'", "''")
            for col in columns:
                conditions.append(f"NOT RLIKE({col.upper()}, '{escaped_pattern}')")

        elif expectation_type == "expect_column_values_to_be_in_set":
            rules = group_config.get("rules", {})
            for col, allowed_values in rules.items():
                value_set = ', '.join(
                    f"'{v}'" if isinstance(v, str) else str(v) for v in allowed_values
                )
                conditions.append(f"{col.upper()} NOT IN ({value_set})")

        elif expectation_type == "expect_column_values_to_not_be_in_set":
            forbidden_values = group_config.get("value_set", [])
            col = group_config.get("column")
            if col and forbidden_values:
                value_set = ', '.join(
                    f"'{v}'" if isinstance(v, str) else str(v) for v in forbidden_values
                )
                conditions.append(f"{col.upper()} IN ({value_set})")

        if not conditions:
            return ""

        # Join conditions with OR (material fails if ANY condition is true)
        condition_clause = " OR ".join(conditions)

        # Build CTE
        cte_name = f"{group_id}_materials"
        cte_sql = f"""{cte_name} AS (
  SELECT DISTINCT {self.index_column}
  FROM {table_name}
  {where_clause}
  {'WHERE ' if not where_clause else 'AND '}({condition_clause})
)"""

        return cte_sql

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

    def _get_conditional_check(self, validation: Dict) -> str:
        """
        Build SQL condition for conditional validation membership check.

        Args:
            validation: Validation configuration with optional conditional_on

        Returns:
            SQL condition string (e.g., "MATERIAL_NUMBER NOT IN (SELECT * FROM group_materials)")
            or empty string if no condition
        """
        conditional_on = validation.get("conditional_on")
        if not conditional_on:
            return ""

        derived_group = conditional_on.get("derived_group")
        membership = conditional_on.get("membership", "exclude")  # default to exclude

        if not derived_group:
            return ""

        cte_name = f"{derived_group}_materials"
        operator = "NOT IN" if membership == "exclude" else "IN"

        return f"{self.index_column} {operator} (SELECT {self.index_column} FROM {cte_name})"

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

        # Get conditional membership check (if any)
        conditional_check = self._get_conditional_check(validation)

        for col in columns:
            col_upper = col.upper()
            expectation_id = build_scoped_expectation_id(validation, col)

            # Build WHEN condition with optional membership check
            when_condition = f"{col_upper} IS NULL"
            if conditional_check:
                when_condition = f"({conditional_check}) AND {when_condition}"

            objects.append(
                "CASE WHEN {when_condition} THEN OBJECT_CONSTRUCT("
                "'expectation_id', '{expectation_id}', "
                "'expectation_type', 'expect_column_values_to_not_be_null', "
                "'column', '{col_name}', "
                "'failure_reason', 'NULL_VALUE', "
                "'unexpected_value', {col}"
                ") END".format(
                    when_condition=when_condition,
                    col=col_upper,
                    expectation_id=expectation_id,
                    col_name=col
                )
            )

        return objects

    def _build_value_in_set_validation(self, validation: Dict) -> List[str]:
        """Build SQL for value-in-set validation flags."""
        rules = validation.get("rules", {})
        objects: List[str] = []

        # Get conditional membership check (if any)
        conditional_check = self._get_conditional_check(validation)

        for column, allowed_values in rules.items():
            col_upper = column.upper()
            expectation_id = build_scoped_expectation_id(validation, column)

            # Format value set for SQL
            if isinstance(allowed_values, list):
                value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                                     for v in allowed_values)
            else:
                value_set = f"'{allowed_values}'"

            # Build WHEN condition with optional membership check
            when_condition = f"{col_upper} NOT IN ({value_set})"
            if conditional_check:
                when_condition = f"({conditional_check}) AND {when_condition}"

            objects.append(
                "CASE WHEN {when_condition} THEN OBJECT_CONSTRUCT("
                "'expectation_id', '{expectation_id}', "
                "'expectation_type', 'expect_column_values_to_be_in_set', "
                "'column', '{column}', "
                "'failure_reason', 'VALUE_NOT_IN_SET', "
                "'unexpected_value', {col}, "
                "'allowed_values', ARRAY_CONSTRUCT({allowed_values})"
                ") END".format(
                    when_condition=when_condition,
                    col=col_upper,
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

        col_upper = column.upper()
        expectation_id = build_scoped_expectation_id(validation, column)

        # Get conditional membership check (if any)
        conditional_check = self._get_conditional_check(validation)

        # Format value set for SQL
        value_set = ', '.join(f"'{v}'" if isinstance(v, str) else str(v)
                             for v in forbidden_values)

        # Build WHEN condition with optional membership check
        when_condition = f"{col_upper} IN ({value_set})"
        if conditional_check:
            when_condition = f"({conditional_check}) AND {when_condition}"

        object_expr = (
            "CASE WHEN {when_condition} THEN OBJECT_CONSTRUCT("
            "'expectation_id', '{expectation_id}', "
            "'expectation_type', 'expect_column_values_to_not_be_in_set', "
            "'column', '{column}', "
            "'failure_reason', 'VALUE_IN_FORBIDDEN_SET', "
            "'unexpected_value', {col}, "
            "'forbidden_values', ARRAY_CONSTRUCT({forbidden_values})"
            ") END".format(
                when_condition=when_condition,
                col=col_upper,
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

        # Get conditional membership check (if any)
        conditional_check = self._get_conditional_check(validation)

        for column in columns:
            col_upper = column.upper()
            expectation_id = build_scoped_expectation_id(validation, column)

            # Escape single quotes in regex pattern
            escaped_pattern = regex_pattern.replace("'", "''")

            # Build WHEN condition with optional membership check
            when_condition = f"NOT RLIKE({col_upper}, '{escaped_pattern}')"
            if conditional_check:
                when_condition = f"({conditional_check}) AND {when_condition}"

            objects.append(
                "CASE WHEN {when_condition} THEN OBJECT_CONSTRUCT("
                "'expectation_id', '{expectation_id}', "
                "'expectation_type', 'expect_column_values_to_match_regex', "
                "'column', '{column}', "
                "'failure_reason', 'REGEX_MISMATCH', "
                "'unexpected_value', {col}, "
                "'regex', '{pattern}'"
                ") END".format(
                    when_condition=when_condition,
                    col=col_upper,
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

        col_a_upper = col_a.upper()
        col_b_upper = col_b.upper()
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
        ).format(a=col_a_upper, b=col_b_upper, expectation_id=expectation_id, col_a=col_a, col_b=col_b)

        return [object_expr]

    def _build_column_pair_greater_validation(self, validation: Dict) -> List[str]:
        """Build SQL for column pair greater-than validation flags."""
        col_a = validation.get("column_a")
        col_b = validation.get("column_b")
        or_equal = validation.get("or_equal", False)

        col_a_upper = col_a.upper()
        col_b_upper = col_b.upper()
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
            "'or_equal', {or_equal}"
            "\n    ) END\n  "
        ).format(
            a=col_a_upper,
            b=col_b_upper,
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

        condition_upper = condition_col.upper()
        required_upper = required_col.upper()
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
            condition=condition_upper,
            required=required_upper,
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

        condition_upper = condition_col.upper()
        target_upper = target_col.upper()
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
            condition=condition_upper,
            target=target_upper,
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

        # If the validation already carries an expectation_id (from a prior
        # annotation pass), keep it so the generator and parser stay in sync
        # across call sites. This also prevents double-annotation when the
        # suite is decorated before reaching the SQL generator.
        existing_id = val_copy.get("expectation_id")
        if existing_id:
            annotated.append(val_copy)
            continue

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
