# Unified Snowflake-Native Validation Framework - Complete Guide

## Overview

This document describes the new unified validation framework that replaces the separate Query Builder + Suite Editor workflow with a single, streamlined approach.

**Key Innovation:** Dynamic SQL generation from YAML rules with grain-based context optimization.

## Architecture

### Old Approach
1. Query Builder generates and persists SQL queries
2. Suite Editor creates validation suites
3. Multiple round-trips to Snowflake
4. Fixed context columns for all validations

### New Unified Approach
1. **Single YAML configuration** defines all validation rules
2. **Dynamic SQL generation** on-the-fly (no persisted queries)
3. **Single Snowflake query** executes all validations
4. **Grain-based context** - each column gets only its grain-specific context

## Performance Benefits

### Benchmark Results (750,000 rows)
- **Old GX Approach:** ~40 minutes (6 threads, 75k rows/chunk)
- **New Snowflake-Native:** 75 seconds
- **Speedup:** 32x faster ✨

### Optimization Details
- **83.3% reduction** in context column payload (6 → 1 column for MARA grain)
- **Single query** vs multiple round-trips
- **All compute in Snowflake** vs Python processing
- **Minimal data transfer** back to Python

## Core Components

### 1. ValidationSQLGenerator (`validations/sql_generator.py`)

Generates complete Snowflake SQL from YAML configuration.

**Supported Validation Types:**
- `expect_column_values_to_not_be_null`
- `expect_column_values_to_be_in_set`
- `expect_column_values_to_not_be_in_set`
- `expect_column_values_to_match_regex`
- `expect_column_pair_values_to_be_equal`
- `expect_column_pair_values_a_to_be_greater_than_b`
- `custom:conditional_required`
- `custom:conditional_value_in_set`

**Key Methods:**
```python
from validations.sql_generator import ValidationSQLGenerator

# Load YAML config
with open("validation_yaml/my_suite.yaml", "r") as f:
    suite_config = yaml.safe_load(f)

# Generate SQL
generator = ValidationSQLGenerator(suite_config)
sql = generator.generate_sql(limit=1000)  # Optional limit for testing
```

### 2. Unified Runner (`validations/snowflake_runner.py`)

Main entry point for running validations.

**Usage:**
```python
from validations.snowflake_runner import run_validation_from_yaml_snowflake

# Run validation
results = run_validation_from_yaml_snowflake(
    yaml_path="validation_yaml/abb_shop_abp_data_presence.yaml",
    limit=1000  # Optional: limit for testing
)

# Results are GX-compatible
print(f"Total validations: {len(results['results'])}")
for result in results['results']:
    print(f"{result['column']}: {'✅' if result['success'] else '❌'}")
```

**Output Format:**
```python
{
    "results": [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "column": "GROSS_WEIGHT",
            "success": True,
            "element_count": 1000,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "failed_materials": [],
            "table_grain": "MARA",
            "unique_by": ["MATERIAL_NUMBER"]
        }
    ],
    "validated_materials": []
}
```

### 3. Grain-Based Context (`core/grain_mapping.py`)

Maps columns to SAP table grains and determines minimal context needed.

**New Functions:**
```python
from core.grain_mapping import (
    get_context_columns_for_column,
    get_context_columns_for_columns
)

# Single column
context = get_context_columns_for_column("GROSS_WEIGHT")
# Returns: ["MATERIAL_NUMBER"]  # MARA grain

# Multiple columns
context = get_context_columns_for_columns(["GROSS_WEIGHT", "MRP_TYPE"])
# Returns: ["MATERIAL_NUMBER", "PLANT"]  # Union of MARA + MARC grains
```

**Grain Definitions:**
- **MARA:** Material master data
  - Unique by: `MATERIAL_NUMBER`
- **MARC:** Plant-specific data
  - Unique by: `MATERIAL_NUMBER`, `PLANT`
- **MVKE:** Sales-specific data
  - Unique by: `MATERIAL_NUMBER`, `SALES_ORGANIZATION`, `DISTRIBUTION_CHANNEL`
- **MARD:** Storage location data
  - Unique by: `MATERIAL_NUMBER`, `PLANT`, `STORAGE_LOCATION`
- **MLGT:** Warehouse data
  - Unique by: `MATERIAL_NUMBER`, `PLANT`, `STORAGE_LOCATION`, `WAREHOUSE_NUMBER`, `STORAGE_TYPE`

## YAML Configuration

### Example Suite

```yaml
metadata:
  suite_name: "my_validation_suite"
  description: "Validation suite description"
  data_context:
    table: "PROD_MO_MONM.REPORTING.vw_ProductDataAll"  # Optional, defaults to vw_ProductDataAll
    where_clause: "MATERIAL_TYPE = 'FERT'"  # Optional filters

validations:
  # Not null validation
  - type: "expect_column_values_to_not_be_null"
    columns:
      - "GROSS_WEIGHT"
      - "NET_WEIGHT"

  # Value in set validation
  - type: "expect_column_values_to_be_in_set"
    rules:
      MATERIAL_TYPE: ["FERT", "HALB", "ROH"]
      BASE_UNIT_OF_MEASURE: ["EA", "KG", "M"]

  # Regex validation
  - type: "expect_column_values_to_match_regex"
    columns:
      - "MATERIAL_NUMBER"
    regex: "^[A-Z0-9-]+$"

  # Column pair validation
  - type: "expect_column_pair_values_a_to_be_greater_than_b"
    column_a: "GROSS_WEIGHT"
    column_b: "NET_WEIGHT"

  # Conditional validation
  - type: "custom:conditional_required"
    condition_column: "MATERIAL_TYPE"
    condition_values: ["FERT"]
    required_column: "BOM_STATUS"
```

## Testing

### 1. SQL Generation Test (No Snowflake Required)

Tests SQL generation logic without database connection:

```bash
python scripts/test_sql_generation.py --yaml validation_yaml/abb_shop_abp_data_presence.yaml
```

**Output:**
- SQL statistics (lines, characters, validation blocks)
- SQL pattern checks (WITH, ARRAY_COMPACT, OBJECT_CONSTRUCT, etc.)
- Validated columns analysis by grain
- Context column optimization metrics
- Sample SQL output

### 2. Full End-to-End Test (Requires Snowflake)

Run in Docker environment with all dependencies:

```bash
docker-compose exec app python scripts/test_unified_runner.py --limit 1000
```

**This test validates:**
- ✅ Complete flow from YAML to results
- ✅ SQL generation and execution
- ✅ Result parsing into GX-compatible format
- ✅ Grain-based context correctness
- ✅ Failed materials structure
- ✅ All required output fields present

### 3. Performance Comparison Test

Compare old GX approach vs new Snowflake-native:

```bash
docker-compose exec app python scripts/compare_validation_performance.py \
    --yaml validation_yaml/abb_shop_abp_data_presence.yaml \
    --limit 1000
```

## Migration Guide

### For Existing Validation Suites

**No changes required!** The YAML format is the same. The new framework reads existing YAML files.

**To use the new framework:**

```python
# Old approach
from validations.runner import run_validation_from_yaml
results = run_validation_from_yaml("validation_yaml/my_suite.yaml")

# New approach (replace import)
from validations.snowflake_runner import run_validation_from_yaml_snowflake
results = run_validation_from_yaml_snowflake("validation_yaml/my_suite.yaml")
```

### For Downstream Systems

**Zero changes required!** The output format is identical to GX.

All required fields are present:
- ✅ `expectation_type`
- ✅ `column`
- ✅ `success`
- ✅ `element_count`
- ✅ `unexpected_count`
- ✅ `unexpected_percent`
- ✅ `failed_materials` (with grain-specific context)
- ✅ `table_grain`
- ✅ `unique_by`

**Downstream systems continue to work unchanged:**
- ✅ Report pages
- ✅ Datalark integration
- ✅ Any custom consumers of validation results

## Key Differences from Old Approach

| Aspect | Old Approach | New Unified Framework |
|--------|--------------|----------------------|
| **SQL Storage** | Persisted queries in files | Generated on-the-fly from YAML |
| **Execution** | Multiple round-trips | Single query |
| **Context Columns** | All 6 context columns always | Grain-specific (1-5 columns) |
| **Performance** | ~40 min for 750k rows | ~75 sec for 750k rows (32x faster) |
| **Maintenance** | Separate query + suite editors | Single YAML configuration |
| **Flexibility** | Fixed query templates | Dynamic SQL generation |
| **Payload Size** | Full context always | 70-83% smaller payloads |

## SQL Generation Details

### Generated SQL Structure

```sql
WITH base_data AS (
  SELECT
    -- Only columns being validated
    "COLUMN1",
    "COLUMN2",
    -- Plus minimal context columns (grain-based)
    "MATERIAL_NUMBER",
    "PLANT"  -- Only if validating MARC columns
  FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
  WHERE <optional filters>
  LIMIT <optional limit>
)
SELECT
  -- For each validation: metrics + failures
  COUNT(*) as column1_total,
  SUM(CASE WHEN "COLUMN1" IS NULL THEN 1 ELSE 0 END) as column1_null_count,
  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN "COLUMN1" IS NULL
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', "MATERIAL_NUMBER",
        'Unexpected Value', "COLUMN1"
      )
      ELSE NULL
    END
  )) as column1_failures,

  -- Repeat for each validation...
FROM base_data
```

### Grain-Based Context Example

**Validation suite with mixed grains:**
- GROSS_WEIGHT (MARA grain)
- MRP_TYPE (MARC grain)
- SALES_STATUS (MVKE grain)

**Base data SELECT includes:**
```sql
SELECT
  "GROSS_WEIGHT",
  "MRP_TYPE",
  "SALES_STATUS",
  "MATERIAL_NUMBER",           -- All grains need this
  "PLANT",                      -- MARC and higher grains need this
  "SALES_ORGANIZATION",         -- MVKE grain needs this
  "DISTRIBUTION_CHANNEL"        -- MVKE grain needs this
FROM ...
```

**Failures for GROSS_WEIGHT only include:**
```json
{
  "MATERIAL_NUMBER": "12345",
  "Unexpected Value": null
}
```

**Failures for MRP_TYPE include:**
```json
{
  "MATERIAL_NUMBER": "12345",
  "PLANT": "1000",
  "Unexpected Value": null
}
```

## Troubleshooting

### Import Error: `ModuleNotFoundError: No module named 'pandas'`

**Solution:** Run tests inside Docker container where dependencies are installed:
```bash
docker-compose exec app python scripts/test_unified_runner.py
```

### SQL Error: `invalid identifier`

**Check:** Are you using actual DB column names (e.g., `SALES_ORGANIZATION`) not display names (e.g., `"Sales Organization"`)?

The framework uses actual Snowflake column names everywhere.

### Column Name Case Mismatch

**Solution:** The framework normalizes Snowflake results to lowercase before parsing:
```python
df.columns = df.columns.str.lower()
```

Column names in YAML should match actual Snowflake column names (case-insensitive).

### Validation Type Not Supported

**Check:** Is the validation type one of the supported types listed above?

If you need a new validation type, add it to:
1. `ValidationSQLGenerator._build_validation_logic()`
2. `snowflake_runner._parse_sql_results()`

## Next Steps

1. **Test in your environment:**
   ```bash
   docker-compose exec app python scripts/test_unified_runner.py --limit 1000
   ```

2. **Run performance comparison:**
   ```bash
   docker-compose exec app python scripts/compare_validation_performance.py --limit 10000
   ```

3. **Migrate existing suites:** Simply update imports to use `run_validation_from_yaml_snowflake`

4. **Monitor downstream systems:** Verify reports and Datalark continue working (they should!)

## Benefits Summary

✅ **32x faster** performance (75s vs 40min for 750k rows)
✅ **Zero downstream changes** required (GX-compatible output)
✅ **83% smaller payloads** (grain-based context optimization)
✅ **Simpler architecture** (single YAML config, no persisted queries)
✅ **Read-only access** compatible (no temp tables needed)
✅ **All GX functionality** supported (+ custom validations)
✅ **Single query execution** (vs multiple round-trips)

## Files Reference

### Core Framework
- `validations/sql_generator.py` - SQL generation engine (571 lines)
- `validations/snowflake_runner.py` - Unified validation runner (410 lines)
- `core/grain_mapping.py` - Grain-based context logic
- `core/constants.py` - Application constants

### Testing
- `scripts/test_sql_generation.py` - SQL generation test (no DB required)
- `scripts/test_unified_runner.py` - Full end-to-end test (requires Snowflake)
- `scripts/compare_validation_performance.py` - Performance comparison tool

### Documentation
- `docs/unified_framework_guide.md` - This guide
- `docs/validation_sql_patterns.md` - SQL pattern details for each validation type
- `docs/downstream_compatibility.md` - Impact analysis for downstream systems
- `docs/snowflake_native_validation_demo.md` - Original demo documentation
