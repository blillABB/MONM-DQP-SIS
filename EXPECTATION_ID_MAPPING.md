# Expectation ID Mapping System

This document explains how expectation IDs uniquely identify expectation/column pairings and how we map back to expectation types and columns.

## Overview

The system uses **deterministic hash-based IDs** generated from YAML configuration. The YAML acts as the canonical mapping source.

---

## ID Generation Flow

### Step 1: Base Expectation ID (YAML → Base ID)

**Location:** `validations/sql_generator.py:739-760` (`_annotate_expectation_ids()`)

**Input:** YAML validation configuration
```yaml
validations:
  - type: expect_column_values_to_not_be_null  # idx=0
    columns:
      - ABP_ELECTRICALDATA1
      - ABP_EFFICIENCYLEVEL
      - ABP_INSULATIONCLASS
```

**Process:**
```python
raw_id = f"{suite_name}|{idx}|{validation.get('type', '')}"
# Example: "ABB SHOP DATA PRESENCE|0|expect_column_values_to_not_be_null"

expectation_id = hashlib.md5(raw_id.encode()).hexdigest()[:12]
# Example: "cda4e89d"

final_id = f"exp_{expectation_id}"
# Example: "exp_cda4e89d"
```

**Output:** Base expectation ID stored in validation dict
```python
{
  "type": "expect_column_values_to_not_be_null",
  "columns": ["ABP_ELECTRICALDATA1", "ABP_EFFICIENCYLEVEL", ...],
  "expectation_id": "exp_cda4e89d"  # ← Added by annotation
}
```

---

### Step 2: Scoped Expectation IDs (Base ID → Column-Specific IDs)

**Location:** `validations/sql_generator.py:763-769` (`build_scoped_expectation_id()`)

**Why Needed:** Single validation can apply to multiple columns - each needs unique ID

**Input:** Base expectation ID + column name
```python
base_id = "exp_cda4e89d"
column = "ABP_ELECTRICALDATA1"
```

**Process:**
```python
raw_scope = f"{base_id}|{column}"
# Example: "exp_cda4e89d|ABP_ELECTRICALDATA1"

scoped_hash = hashlib.md5(raw_scope.encode()).hexdigest()[:8]
# Example: "a1b2c3d4"

scoped_id = f"{base_id}_{scoped_hash}"
# Example: "exp_cda4e89d_a1b2c3d4"
```

**Output:** Multiple scoped IDs (one per column)
```
exp_cda4e89d_a1b2c3d4  → ABP_ELECTRICALDATA1
exp_cda4e89d_e5f6g7h8  → ABP_EFFICIENCYLEVEL
exp_cda4e89d_i9j0k1l2  → ABP_INSULATIONCLASS
```

---

## ID Usage in SQL Generation

**Location:** `validations/sql_generator.py:415-446` (example: `_build_not_null_validation()`)

### SQL Query Structure

The scoped IDs are embedded in the generated SQL:

```sql
SELECT
  MATERIAL_NUMBER,
  ABP_ELECTRICALDATA1,
  ABP_EFFICIENCYLEVEL,
  -- ... other columns ...
  ARRAY_COMPACT(ARRAY_CONSTRUCT(
    -- For ABP_ELECTRICALDATA1
    CASE WHEN ABP_ELECTRICALDATA1 IS NULL THEN OBJECT_CONSTRUCT(
      'expectation_id', 'exp_cda4e89d_a1b2c3d4',  ← Scoped ID
      'expectation_type', 'expect_column_values_to_not_be_null',
      'column', 'ABP_ELECTRICALDATA1',
      'failure_reason', 'NULL_VALUE',
      'unexpected_value', ABP_ELECTRICALDATA1
    ) END,

    -- For ABP_EFFICIENCYLEVEL
    CASE WHEN ABP_EFFICIENCYLEVEL IS NULL THEN OBJECT_CONSTRUCT(
      'expectation_id', 'exp_cda4e89d_e5f6g7h8',  ← Different scoped ID
      'expectation_type', 'expect_column_values_to_not_be_null',
      'column', 'ABP_EFFICIENCYLEVEL',
      'failure_reason', 'NULL_VALUE',
      'unexpected_value', ABP_EFFICIENCYLEVEL
    ) END
  )) AS validation_results
FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
```

### SQL Results

Each row contains an array of failure objects:

```json
{
  "MATERIAL_NUMBER": "12345",
  "ABP_ELECTRICALDATA1": null,
  "ABP_EFFICIENCYLEVEL": "Premium",
  "validation_results": [
    {
      "expectation_id": "exp_cda4e89d_a1b2c3d4",
      "expectation_type": "expect_column_values_to_not_be_null",
      "column": "ABP_ELECTRICALDATA1",
      "failure_reason": "NULL_VALUE",
      "unexpected_value": null
    }
  ]
}
```

---

## Reverse Mapping: ID → Type + Column

### Method 1: Parse Validation Results from SQL (Runtime)

**Location:** `validations/snowflake_runner.py:693-719` (`_collect_validation_failures()`)

The SQL results **include both ID and metadata**, so no reverse lookup needed:

```python
for _, row in df.iterrows():
    entries = row.get("validation_results")  # JSON array
    for entry in entries:
        exp_id = entry.get("expectation_id")        # "exp_cda4e89d_a1b2c3d4"
        exp_type = entry.get("expectation_type")    # "expect_column_values_to_not_be_null"
        column = entry.get("column")                # "ABP_ELECTRICALDATA1"
        # ↑ All metadata embedded in the result!
```

**Key Insight:** The SQL embeds the type and column alongside the ID, so we don't need to "look up" the mapping - it's in the data.

---

### Method 2: Build Catalog from YAML (UI/Analysis)

**Location:** `validations/derived_status_resolver.py:174-213` (`_build_expectation_catalog()`)

For UI displays and derived statuses, build a complete catalog from YAML:

```python
catalog = []

for validation in validations:
    val_type = validation.get("type")

    # Extract target columns
    if "columns" in validation:
        targets = validation["columns"]
    elif "column" in validation:
        targets = [validation["column"]]
    # ... other extraction logic

    # Generate scoped IDs for each target
    for target in targets:
        scoped_id = build_scoped_expectation_id(validation, target)

        catalog.append({
            "scoped_id": scoped_id,              # "exp_cda4e89d_a1b2c3d4"
            "type": val_type,                    # "expect_column_values_to_not_be_null"
            "targets": [target],                 # ["ABP_ELECTRICALDATA1"]
            "base_id": validation["expectation_id"]  # "exp_cda4e89d"
        })
```

**Output Catalog:**
```python
[
  {
    "scoped_id": "exp_cda4e89d_a1b2c3d4",
    "type": "expect_column_values_to_not_be_null",
    "targets": ["ABP_ELECTRICALDATA1"],
    "base_id": "exp_cda4e89d"
  },
  {
    "scoped_id": "exp_cda4e89d_e5f6g7h8",
    "type": "expect_column_values_to_not_be_null",
    "targets": ["ABP_EFFICIENCYLEVEL"],
    "base_id": "exp_cda4e89d"
  },
  # ... more entries
]
```

**Usage:** Given an ID, search catalog for matching `scoped_id` to get type and column:

```python
def lookup_expectation(exp_id: str, catalog: List[dict]) -> dict:
    for entry in catalog:
        if entry["scoped_id"] == exp_id:
            return {
                "type": entry["type"],
                "column": entry["targets"][0] if len(entry["targets"]) == 1 else "|".join(entry["targets"])
            }
    return None

# Example usage
result = lookup_expectation("exp_cda4e89d_a1b2c3d4", catalog)
# Returns: {"type": "expect_column_values_to_not_be_null", "column": "ABP_ELECTRICALDATA1"}
```

---

## Complete Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│ YAML Configuration (Source of Truth)                                │
├─────────────────────────────────────────────────────────────────────┤
│ validations:                                                         │
│   - type: expect_column_values_to_not_be_null                       │
│     columns: [ABP_ELECTRICALDATA1, ABP_EFFICIENCYLEVEL]             │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 1: Annotate Base IDs                                           │
│ (sql_generator.py:_annotate_expectation_ids)                        │
├─────────────────────────────────────────────────────────────────────┤
│ Hash: MD5("ABB SHOP|0|expect_column_values_to_not_be_null")         │
│ Result: "exp_cda4e89d"                                              │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Step 2: Generate Scoped IDs per Column                              │
│ (sql_generator.py:build_scoped_expectation_id)                      │
├─────────────────────────────────────────────────────────────────────┤
│ Column: ABP_ELECTRICALDATA1                                         │
│   Hash: MD5("exp_cda4e89d|ABP_ELECTRICALDATA1")                     │
│   ID: "exp_cda4e89d_a1b2c3d4"                                       │
│                                                                      │
│ Column: ABP_EFFICIENCYLEVEL                                         │
│   Hash: MD5("exp_cda4e89d|ABP_EFFICIENCYLEVEL")                     │
│   ID: "exp_cda4e89d_e5f6g7h8"                                       │
└────────────────────────┬────────────────────────────────────────────┘
                         │
        ┌────────────────┴────────────────┐
        │                                  │
        ▼                                  ▼
┌──────────────────────┐    ┌─────────────────────────────────────────┐
│ Embed in SQL Query   │    │ Build Catalog for Lookup                │
│ (SQL Generator)      │    │ (DerivedStatusResolver)                 │
├──────────────────────┤    ├─────────────────────────────────────────┤
│ CASE WHEN col IS     │    │ catalog = [                             │
│   NULL THEN          │    │   {                                     │
│   OBJECT_CONSTRUCT(  │    │     "scoped_id": "exp_..._a1b2c3d4",   │
│     'expectation_id',│    │     "type": "expect_column_...",        │
│     'exp_..._a1b2',  │    │     "targets": ["ABP_ELECTRICALDATA1"]  │
│     'type',          │    │   },                                    │
│     'expect_...',    │    │   ...                                   │
│     'column',        │    │ ]                                       │
│     'ABP_ELEC...'    │    │                                         │
│   )                  │    │ # Lookup by scoped_id                   │
│ END                  │    │ entry = catalog[scoped_id]              │
└──────┬───────────────┘    └─────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Snowflake Results                                                    │
├─────────────────────────────────────────────────────────────────────┤
│ validation_results: [                                                │
│   {                                                                  │
│     "expectation_id": "exp_cda4e89d_a1b2c3d4",                      │
│     "expectation_type": "expect_column_values_to_not_be_null",      │
│     "column": "ABP_ELECTRICALDATA1",  ← Type + Column included!     │
│     "failure_reason": "NULL_VALUE"                                   │
│   }                                                                  │
│ ]                                                                    │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Result Parsing (snowflake_runner.py)                                │
├─────────────────────────────────────────────────────────────────────┤
│ 1. Extract from SQL results (already has type + column)             │
│ 2. Count failures by expectation_id                                 │
│ 3. Match with YAML validations using regenerated IDs                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Files Reference

| File | Function | Purpose |
|------|----------|---------|
| `validations/sql_generator.py:739` | `_annotate_expectation_ids()` | Generate base IDs from YAML |
| `validations/sql_generator.py:763` | `build_scoped_expectation_id()` | Generate column-specific IDs |
| `validations/sql_generator.py:415` | `_build_not_null_validation()` | Embed IDs in SQL |
| `validations/snowflake_runner.py:693` | `_collect_validation_failures()` | Extract IDs from results |
| `validations/derived_status_resolver.py:174` | `_build_expectation_catalog()` | Build ID→metadata catalog |

---

## Summary

**Question:** How do we get from expectation ID back to expectation type and column?

**Answer:** Two ways:

1. **From SQL Results (Most Common):** The SQL embeds the type and column alongside the ID in every failure object, so no reverse lookup is needed.

2. **From YAML Catalog (UI/Analysis):** Rebuild the same IDs from YAML using the deterministic hash functions, creating a catalog that maps `scoped_id → {type, targets}`.

**The YAML is the canonical source** - all IDs are computed from it, and the mapping can always be reconstructed by re-running the same hash functions.
