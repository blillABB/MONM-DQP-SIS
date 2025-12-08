# Grain-Based Deduplication System

## Overview

The grain-based deduplication system solves the problem of duplicate failures when validating SAP MDM data that includes organizational context columns (Plant, Sales Organization, Distribution Channel, etc.).

### The Problem

When validating fields from different SAP tables, organizational context creates data explosion:

| Material | Plant | Sales Org | GROSS_WEIGHT | MRP_TYPE |
|----------|-------|-----------|--------------|----------|
| MAT001   | 00A   | BEC       | NULL         | P3       |
| MAT001   | 00B   | BEC       | NULL         | P3       |
| MAT001   | 00A   | 7000      | NULL         | P3       |

**GROSS_WEIGHT** (from MARA table) fails validation 3 times, but it's the **same material** with the same issue!

**Before grain deduplication**:
- ❌ 3 failures sent to DataLark for MAT001
- ❌ Inflated metrics (3 failures vs 1 unique material)
- ❌ Wasted BAPI calls

**After grain deduplication**:
- ✅ 1 failure sent to DataLark for MAT001
- ✅ Accurate metrics (1 unique material)
- ✅ Efficient BAPI usage

## How It Works

### 1. Column-to-Grain Mapping

The system automatically maps each column to its SAP table grain (unique key):

```python
from core.grain_mapping import get_grain_for_column

# MARA field - Material level only
table, grain = get_grain_for_column("GROSS_WEIGHT")
# Returns: ("MARA", ["MATERIAL_NUMBER"])

# MARC field - Material + Plant level
table, grain = get_grain_for_column("MRP_TYPE")
# Returns: ("MARC", ["MATERIAL_NUMBER", "PLANT"])

# MVKE field - Material + Sales Org + Dist Channel level
table, grain = get_grain_for_column("PRICING_GROUP")
# Returns: ("MVKE", ["MATERIAL_NUMBER", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"])
```

### 2. Automatic Grain Detection

When validations run, grain metadata is automatically added to results:

```json
{
  "expectation_type": "expect_column_values_to_not_be_null",
  "column": "GROSS_WEIGHT",
  "table_grain": "MARA",
  "unique_by": ["MATERIAL_NUMBER"],
  "failed_materials": [...]
}
```

### 3. Smart Deduplication Before DataLark Send

When sending failures to DataLark, the system:

1. Extracts the `unique_by` columns from the validation result
2. Deduplicates the failures based on those columns
3. Shows both counts to the user:
   - **Unique records** (what gets sent to DataLark)
   - **Total rows** (with org context)

Example UI output:
```
📊 MARA grain: 1 unique records (3 total rows with org context)
Deduplication key: Material Number
```

## SAP Table Grains

| Table | Description | Grain (Unique Key) |
|-------|-------------|--------------------|
| **MARA** | Material Master Basic Data | `MATERIAL_NUMBER` |
| **MAKT** | Material Description | `MATERIAL_NUMBER` |
| **MARC** | Material Master Plant Data | `MATERIAL_NUMBER, PLANT` |
| **MVKE** | Sales Organization Data | `MATERIAL_NUMBER, SALES_ORGANIZATION, DISTRIBUTION_CHANNEL` |
| **MBEW** | Valuation Data | `MATERIAL_NUMBER, PLANT` |
| **MARD** | Storage Location Data | `MATERIAL_NUMBER, PLANT, STORAGE_LOCATION` |
| **MLGT** | Warehouse Management Data | `MATERIAL_NUMBER, WAREHOUSE_NUMBER` |

## Column-to-Table Mapping

The mapping is extracted from the `STAGE."sp_CreateProductDataTable"` stored procedure and maintained in `core/grain_mapping.py`.

### Example Columns

**MARA columns** (material-level):
- GROSS_WEIGHT, NET_WEIGHT, MATERIAL_TYPE, INDUSTRY_SECTOR, BASE_UNIT_OF_MEASURE, GLOBAL_PRODUCT_ID, PACK_INDICATOR, SIZE_DIMENSIONS, etc.

**MARC columns** (material-plant level):
- MRP_TYPE, PROCUREMENT_TYPE, PURCHASING_GROUP, MRP_CONTROLLER, AVAILABILITY_CHECK, PLANNING_TIME_FENCE, HTS_CODE, COUNTRY_OF_ORIGIN, BATCH_MANAGEMENT, etc.

**MVKE columns** (material-sales org-dist channel level):
- PRICING_GROUP, SALES_STATUS, MATERIAL_GROUP_1-5, DELIVERING_PLANT, DISTRIBUTION_INDICATOR, CASH_DISCOUNT, OMS_FLAG, etc.

## Usage

### For Developers

The grain system works automatically - no changes needed to YAML validation files:

```yaml
validations:
  # Grain is auto-detected as MARA (material-level)
  - type: "expect_column_values_to_not_be_null"
    columns:
      - "GROSS_WEIGHT"
      - "NET_WEIGHT"

  # Grain is auto-detected as MARC (material-plant level)
  - type: "expect_column_values_to_not_be_null"
    columns:
      - "MRP_TYPE"
      - "PURCHASING_GROUP"
```

### For Business Users

When viewing validation results:

1. **Summary section** shows the grain: `Table Grain: MARA | Unique By: MATERIAL_NUMBER`
2. **DataLark section** shows deduplication summary before sending
3. **Metrics** reflect actual unique issues, not inflated row counts

## Benefits

### 1. Accurate Metrics
- Failure counts reflect true data quality issues
- No double-counting across org levels

### 2. Efficient BAPI Usage
- Only unique records sent to DataLark
- Reduced API call volume
- Faster processing

### 3. Preserved Context
- Organizational columns still available for drill-down
- Full traceability maintained
- Context used for analysis, not counting

### 4. Automatic & Transparent
- No manual configuration required
- Works for all existing validations
- Clear UI feedback

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ Validation Execution (BaseValidationSuite)                  │
│                                                              │
│  1. Column validated (e.g., GROSS_WEIGHT)                   │
│  2. Grain auto-detected: get_grain_for_column()             │
│     → Returns ("MARA", ["MATERIAL_NUMBER"])                 │
│  3. Grain stored in result metadata                         │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ UI Display (drill_down.py)                                  │
│                                                              │
│  1. Extract unique_by from result: ["MATERIAL_NUMBER"]      │
│  2. Deduplicate DataFrame: df.drop_duplicates(subset=...)   │
│  3. Show both counts:                                       │
│     - Unique: 1 material                                    │
│     - Total: 3 rows with org context                        │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ DataLark Send (drill_down.py)                               │
│                                                              │
│  1. Use deduplicated DataFrame                              │
│  2. Send only unique records                                │
│  3. Avoid duplicate BAPI calls                              │
└─────────────────────────────────────────────────────────────┘
```

## Fallback Behavior

If grain columns are not available in the DataFrame:

1. **Validate columns exist**: `validate_grain_columns_exist()`
2. **Show warning**: "⚠️ Grain columns not all available. Using fallback grain."
3. **Use fallback**: `get_fallback_grain()` returns most granular available grain
4. **Minimum fallback**: Always defaults to `["MATERIAL_NUMBER"]` if available

## Testing

Run the grain mapping tests:

```bash
# Quick test
python test_grain_quick.py

# Full test suite (requires pytest)
python -m pytest tests/test_grain_mapping.py -v
```

## Maintenance

### Adding New Columns

When new columns are added to `vw_ProductDataAll`:

1. **Identify source table** (MARA, MARC, MVKE, etc.)
2. **Add to `core/grain_mapping.py`**:
   ```python
   COLUMN_TO_TABLE = {
       # ... existing mappings
       "NEW_COLUMN": "MARA",  # or appropriate table
   }
   ```
3. **Verify grain definition exists** for that table in `GRAIN_DEFINITIONS`
4. **Run tests**: `python test_grain_quick.py`

### Updating Grains

If SAP table structures change (rare):

1. **Update `GRAIN_DEFINITIONS`** in `core/grain_mapping.py`
2. **Verify all dependent code** still works
3. **Run full test suite**

## See Also

- `core/grain_mapping.py` - Implementation
- `validations/base_validation.py` - Integration with validation framework
- `app/components/drill_down.py` - UI integration
- `docs/DATA_SOURCES.md` - Data source documentation
