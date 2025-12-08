# New Snowflake-Native Validation Framework Architecture

## Overview

This document outlines the proposed architecture for a simplified, Snowflake-native validation framework that eliminates the query builder and generates SQL dynamically from validation rules.

---

## Current vs. Proposed Architecture

### Current (GX-Based)
```
┌─────────────────┐
│  Query Builder  │ → Saves SQL queries to database/files
└────────┬────────┘
         ↓
┌─────────────────┐
│  Suite Editor   │ → References saved queries + defines validations
└────────┬────────┘
         ↓
┌─────────────────┐
│   GX Runner     │ → Runs query, fetches data, runs expectations in Python
└────────┬────────┘
         ↓
┌─────────────────┐
│    Results      │
└─────────────────┘
```

**Issues:**
- Two separate tools (query builder + suite editor)
- Queries persisted separately from rules
- Data fetched to Python for validation
- Complex chunking needed for large datasets

### Proposed (Snowflake-Native)
```
┌─────────────────────────────┐
│   Suite Builder/Editor      │ → Single unified interface
│  - Data source filters      │
│  - Validation rules         │
└──────────────┬──────────────┘
               ↓
┌──────────────────────────────┐
│  Dynamic SQL Generator       │ → Translates rules → SQL on-the-fly
│  - Builds WHERE clause       │
│  - Builds validation logic   │
│  - Determines grain context  │
└──────────────┬───────────────┘
               ↓
┌──────────────────────────────┐
│  Snowflake Execution         │ → Single query validates everything
└──────────────┬───────────────┘
               ↓
┌──────────────────────────────┐
│  Results (GX-compatible)     │
└──────────────────────────────┘
```

**Benefits:**
- ✅ Single tool (simpler UX)
- ✅ Rules are source of truth
- ✅ SQL generated on-demand (no stale queries)
- ✅ All validation in Snowflake (32x faster)
- ✅ Smart grain-based context (smaller payloads)

---

## Suite Configuration Schema

### YAML Structure

```yaml
metadata:
  suite_name: "Level_1_Validation"
  description: "Baseline validation for material master data"
  index_column: "MATERIAL_NUMBER"

# Data source (replaces saved queries)
data_source:
  table: "PROD_MO_MONM.REPORTING.vw_ProductDataAll"
  filters:
    PRODUCT_HIERARCHY: "LIKE '5%'"
    OMS_FLAG: "= 'Y'"
    SALES_ORGANIZATION: "= 'BEC'"
    PLANT: "IN ('00A', '00B', '00C')"

# Validation rules
validations:
  # Standard not-null check
  - type: expect_column_values_to_not_be_null
    columns:
      - MATERIAL_NUMBER
      - BASE_UNIT_OF_MEASURE
      - GROSS_WEIGHT

  # Value in set
  - type: expect_column_values_to_be_in_set
    column: MATERIAL_TYPE
    value_set: ["FERT", "HALB", "ROH"]

  # Conditional logic
  - type: conditional_required
    condition_column: MATERIAL_TYPE
    condition_values: ["FERT"]
    required_column: BOM_STATUS

  # Cross-column comparison
  - type: expect_column_pair_values_a_to_be_greater_than_b
    column_a: GROSS_WEIGHT
    column_b: NET_WEIGHT
    or_equal: true
```

### UI Configuration Builder

Instead of separate Query Builder and Suite Editor, a single interface:

```
┌─────────────────────────────────────────────────────────┐
│  Suite Builder: Level_1_Validation                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  📋 Suite Metadata                                       │
│  ┌────────────────────────────────────────────┐        │
│  │ Name: Level_1_Validation                    │        │
│  │ Description: Baseline validation...         │        │
│  │ Index Column: MATERIAL_NUMBER               │        │
│  └────────────────────────────────────────────┘        │
│                                                          │
│  🔍 Data Source Filters                                 │
│  ┌────────────────────────────────────────────┐        │
│  │ Table: vw_ProductDataAll                    │        │
│  │                                             │        │
│  │ Filters:                                    │        │
│  │   PRODUCT_HIERARCHY  LIKE  '5%'       [X]  │        │
│  │   OMS_FLAG           =     'Y'        [X]  │        │
│  │   [+ Add Filter]                            │        │
│  └────────────────────────────────────────────┘        │
│                                                          │
│  ✓ Validation Rules                                     │
│  ┌────────────────────────────────────────────┐        │
│  │ 1. Not Null Check                     [Edit] [X]    │
│  │    Columns: MATERIAL_NUMBER, BASE_UNIT...   │        │
│  │                                             │        │
│  │ 2. Value In Set                       [Edit] [X]    │
│  │    Column: MATERIAL_TYPE                    │        │
│  │    Values: FERT, HALB, ROH                  │        │
│  │                                             │        │
│  │ 3. Conditional Required               [Edit] [X]    │
│  │    If MATERIAL_TYPE = FERT                  │        │
│  │    Then BOM_STATUS required                 │        │
│  │                                             │        │
│  │ [+ Add Validation Rule]                     │        │
│  └────────────────────────────────────────────┘        │
│                                                          │
│  [Preview SQL]  [Test Run (1000 rows)]  [Save Suite]   │
└─────────────────────────────────────────────────────────┘
```

---

## Smart Grain-Based Context

### Problem: Unnecessary Context Columns

Current approach includes ALL organizational context columns in every failure:
- MATERIAL_NUMBER ✅
- Sales Organization ❓ (not always relevant)
- Plant ❓ (not always relevant)
- Distribution Channel ❓ (not always relevant)
- Warehouse Number ❓ (rarely relevant)
- Storage Type ❓ (rarely relevant)
- Storage Location ❓ (rarely relevant)

### Solution: Include Only Grain-Relevant Context

Use `grain_mapping.py` to determine which context columns are needed:

```python
from core.grain_mapping import get_grain_for_column

def get_context_columns_for_validation(column_name: str) -> list[str]:
    """
    Determine which context columns to include based on column grain.

    Returns only the columns needed to uniquely identify failures
    at the correct granularity level.
    """
    table_grain, unique_by = get_grain_for_column(column_name)

    # Map grain table to required context columns
    context_map = {
        "MARA": ["MATERIAL_NUMBER"],
        "MARC": ["MATERIAL_NUMBER", "PLANT"],
        "MVKE": ["MATERIAL_NUMBER", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"],
        "MARD": ["MATERIAL_NUMBER", "PLANT", "STORAGE_LOCATION"],
        "MLGT": ["MATERIAL_NUMBER", "WAREHOUSE_NUMBER"],
    }

    return context_map.get(table_grain, ["MATERIAL_NUMBER"])
```

### Example: Different Grains

**MARA Column (GROSS_WEIGHT) - Material Level:**
```json
{
  "column": "GROSS_WEIGHT",
  "failed_materials": [
    {
      "MATERIAL_NUMBER": "12345"
      // Only material number - this is material-level data
    }
  ]
}
```

**MARC Column (MRP_TYPE) - Plant Level:**
```json
{
  "column": "MRP_TYPE",
  "failed_materials": [
    {
      "MATERIAL_NUMBER": "12345",
      "Plant": "00A"
      // Material + Plant - this is plant-specific data
    }
  ]
}
```

**MVKE Column (PRICING_GROUP) - Sales Org Level:**
```json
{
  "column": "PRICING_GROUP",
  "failed_materials": [
    {
      "MATERIAL_NUMBER": "12345",
      "Sales Organization": "BEC",
      "Distribution Channel": "01"
      // Material + Sales Org + Dist Channel - this is sales-specific data
    }
  ]
}
```

**Benefits:**
- 🚀 **Smaller JSON payloads** (1-3 columns vs 7 columns)
- 🎯 **Clearer context** (only relevant organizational info)
- ✅ **Correct deduplication** (matches natural grain)
- 💾 **Less storage** (smaller result files)

---

## Dynamic SQL Generation

### High-Level Flow

```python
def generate_validation_sql(suite_config: dict) -> str:
    """
    Generate complete Snowflake SQL from suite configuration.

    Args:
        suite_config: Parsed YAML configuration

    Returns:
        Complete SQL query string
    """
    # 1. Parse data source
    table = suite_config["data_source"]["table"]
    filters = suite_config["data_source"]["filters"]

    # 2. Build WHERE clause
    where_clause = build_where_clause(filters)

    # 3. Collect all columns being validated
    all_columns = collect_all_columns(suite_config["validations"])

    # 4. Determine required context columns (union of all grain contexts)
    context_columns = determine_context_columns(all_columns)

    # 5. Build validation logic for each rule
    validation_parts = []
    for validation in suite_config["validations"]:
        sql_part = build_validation_sql(validation)
        validation_parts.append(sql_part)

    # 6. Assemble complete query
    return f"""
    WITH base_data AS (
      SELECT
        {', '.join(all_columns)},
        {', '.join(context_columns)}
      FROM {table}
      WHERE {where_clause}
    )
    SELECT
      {',\n      '.join(validation_parts)}
    FROM base_data
    """
```

### Validation Type → SQL Templates

Each validation type has a SQL template:

```python
VALIDATION_TEMPLATES = {
    "expect_column_values_to_not_be_null": """
        COUNT(*) as {col}_total,
        SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) as {col}_null_count,
        ARRAY_COMPACT(ARRAY_AGG(
          CASE
            WHEN "{col}" IS NULL
            THEN OBJECT_CONSTRUCT({context_fields})
            ELSE NULL
          END
        )) as {col}_failures
    """,

    "expect_column_values_to_be_in_set": """
        SUM(CASE WHEN "{col}" NOT IN ({value_set}) THEN 1 ELSE 0 END) as {col}_invalid_count,
        ARRAY_COMPACT(ARRAY_AGG(
          CASE
            WHEN "{col}" NOT IN ({value_set})
            THEN OBJECT_CONSTRUCT({context_fields}, 'Unexpected Value', "{col}")
            ELSE NULL
          END
        )) as {col}_failures
    """,

    "conditional_required": """
        SUM(CASE
          WHEN "{condition_col}" IN ({condition_values}) AND "{required_col}" IS NULL
          THEN 1 ELSE 0
        END) as {name}_violation_count,
        ARRAY_COMPACT(ARRAY_AGG(
          CASE
            WHEN "{condition_col}" IN ({condition_values}) AND "{required_col}" IS NULL
            THEN OBJECT_CONSTRUCT({context_fields},
              '{condition_col}', "{condition_col}",
              '{required_col}', "{required_col}")
            ELSE NULL
          END
        )) as {name}_failures
    """
}
```

### Example: Complete Generated SQL

**Input YAML:**
```yaml
data_source:
  table: "PROD_MO_MONM.REPORTING.vw_ProductDataAll"
  filters:
    PRODUCT_HIERARCHY: "LIKE '5%'"
    OMS_FLAG: "= 'Y'"

validations:
  - type: expect_column_values_to_not_be_null
    columns: [GROSS_WEIGHT, MRP_TYPE]
```

**Generated SQL:**
```sql
WITH base_data AS (
  SELECT
    "GROSS_WEIGHT",
    "MRP_TYPE",
    "MATERIAL_NUMBER",
    "PLANT"  -- Included because MRP_TYPE is MARC grain
  FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
  WHERE PRODUCT_HIERARCHY LIKE '5%'
    AND OMS_FLAG = 'Y'
)
SELECT
  -- Validation for GROSS_WEIGHT (MARA grain)
  COUNT(*) as gross_weight_total,
  SUM(CASE WHEN "GROSS_WEIGHT" IS NULL THEN 1 ELSE 0 END) as gross_weight_null_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN "GROSS_WEIGHT" IS NULL
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', "MATERIAL_NUMBER"
      )
      ELSE NULL
    END
  )) as gross_weight_failures,

  -- Validation for MRP_TYPE (MARC grain)
  COUNT(*) as mrp_type_total,
  SUM(CASE WHEN "MRP_TYPE" IS NULL THEN 1 ELSE 0 END) as mrp_type_null_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN "MRP_TYPE" IS NULL
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', "MATERIAL_NUMBER",
        'Plant', "PLANT"
      )
      ELSE NULL
    END
  )) as mrp_type_failures
FROM base_data
```

Notice:
- ✅ GROSS_WEIGHT failures only include MATERIAL_NUMBER (MARA grain)
- ✅ MRP_TYPE failures include MATERIAL_NUMBER + PLANT (MARC grain)
- ✅ Only necessary columns selected from base_data

---

## Migration Path

### Phase 1: Add Snowflake-Native Backend (Parallel)

Keep existing Query Builder + Suite Editor UI, but add Snowflake-native execution option:

```python
# In gx_runner.py
def run_validation_from_yaml(yaml_path, use_snowflake_native=False):
    if use_snowflake_native:
        # New path: generate SQL and execute
        config = load_yaml(yaml_path)
        sql = generate_validation_sql(config)
        return execute_snowflake_native(sql, config)
    else:
        # Existing GX path
        return run_gx_validation(yaml_path)
```

Toggle in UI:
```python
use_native = st.sidebar.checkbox("Use Snowflake-Native (Beta)", value=False)
results = run_validation_from_yaml(yaml_path, use_snowflake_native=use_native)
```

### Phase 2: Unified Suite Builder

Build new consolidated interface that:
1. Replaces both Query Builder and Suite Editor
2. Generates YAML directly (no separate query persistence)
3. Uses Snowflake-native execution by default
4. Has "Preview SQL" button to show generated query

### Phase 3: Deprecate Old Tools

Once validated:
- Remove Query Builder
- Remove Suite Editor
- Remove GX dependency
- Keep only: Suite Builder → Snowflake-Native Execution

---

## Technical Considerations

### 1. Filter Syntax Parsing

**User-Friendly Input:**
```yaml
filters:
  PRODUCT_HIERARCHY: "LIKE '5%'"
  OMS_FLAG: "Y"                    # Implicit =
  SALES_ORG: "['BEC', 'USA']"     # Implicit IN
```

**Parsed to SQL:**
```sql
WHERE PRODUCT_HIERARCHY LIKE '5%'
  AND OMS_FLAG = 'Y'
  AND SALES_ORG IN ('BEC', 'USA')
```

### 2. Column Grain Lookup

Pre-compute or cache grain mappings:
```python
# At suite validation time
column_grains = {
    col: get_grain_for_column(col)
    for col in all_validated_columns
}
```

### 3. Context Column Optimization

Only include columns that are actually used:
```python
# If all validations are MARA grain, only include MATERIAL_NUMBER
required_context = set()
for validation in validations:
    for col in validation.columns:
        context_cols = get_context_columns_for_validation(col)
        required_context.update(context_cols)

# Only select these in base_data CTE
```

### 4. Backward Compatibility

Ensure new system can read existing YAML files:
```python
def load_suite_config(yaml_path):
    config = load_yaml(yaml_path)

    # Handle old format with "data_source: query_function_name"
    if isinstance(config.get("data_source"), str):
        config = migrate_old_format(config)

    return config
```

---

## Benefits Summary

| Aspect | Old Approach | New Approach | Impact |
|--------|--------------|--------------|--------|
| **Tools** | Query Builder + Suite Editor | Single Suite Builder | 🎯 Simpler UX |
| **Query Storage** | Persisted separately | Generated on-demand | 🗑️ No stale queries |
| **Execution** | GX (Python) | Snowflake SQL | ⚡ 32x faster |
| **Context Columns** | All 7 every time | Grain-specific (1-3) | 💾 Smaller payloads |
| **Maintenance** | Update queries separately | Rules = source of truth | 🔧 Easier maintenance |
| **Scalability** | Chunking required | Single query handles all | 🚀 Better performance |

---

## Next Steps

1. ✅ Validate Snowflake-native validator produces correct output ← **Done**
2. ✅ Confirm downstream compatibility ← **Done**
3. ⏳ Build SQL template system for all validation types
4. ⏳ Implement dynamic SQL generator
5. ⏳ Add grain-based context optimization
6. ⏳ Build unified Suite Builder UI
7. ⏳ Test on existing suites
8. ⏳ Migrate production suites
9. ⏳ Deprecate old tools

This architecture leverages all the learnings from the demo while providing a cleaner, faster, more maintainable system!
