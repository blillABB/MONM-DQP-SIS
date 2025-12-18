# Adapter Fixes Summary

**Date**: 2025-12-18

## Issues Fixed

### ✅ Issue #1: DataFrame Structure Mismatch
**Problem**: Drill-down expected columns like `"Expectation Type"`, `"Column"`, `"Material Number"`, `"Unexpected Value"` but we had columnar format.

**Solution**: Build `failed_materials` list for each result, which `results_to_dataframe()` converts to the expected long format.

**Code**:
```python
failed_materials.append({
    "material_number": material_number,
    "MATERIAL_NUMBER": str(material_number).upper(),
    "Unexpected Value": unexpected_value,
})

results.append({
    ...
    "failed_materials": failed_materials,  # ← Now populated!
})
```

---

### ✅ Issue #2: Missing `failed_materials` Field
**Problem**: Results didn't include the `failed_materials` list needed by `results_to_dataframe()`.

**Solution**: Extract failed materials by filtering the columnar DataFrame on PASS/FAIL values.

**Code**:
```python
failed_df = get_failed_materials(df, exp_id=exp_col, index_column=index_column)

for _, row in failed_df.iterrows():
    # Build failed_materials entry
    ...
```

---

### ✅ Issue #3: Missing `expected` Values
**Problem**: Drill-down couldn't show expected values (e.g., valid values for `be_in_set`).

**Solution**: Load YAML config and extract expected values based on validation type.

**Code**:
```python
# Load YAML
with open(yaml_path, 'r') as f:
    yaml_config = yaml.safe_load(f)

# Extract expected values
if expectation_type == 'expect_column_values_to_be_in_set':
    rules = validation_config.get('rules', {})
    expected = rules.get(column)

results.append({
    ...
    "expected": expected,  # ← Now included!
})
```

---

### ✅ Issue #4: Derived Status `failed_materials` Not Properly Populated
**Problem**: Derived status failures didn't show which specific expectations or columns failed for each material.

**Solution**: Analyze the columnar DataFrame to find which constituent expectations failed for each material.

**Code**:
```python
# Get constituent expectation IDs from YAML
constituent_exp_ids = derived_config.get("expectation_ids", [])

# Analyze which failed for this material
failed_expectations = []
failed_columns = set()

for exp_id in constituent_exp_ids:
    if exp_id in row.index and row[exp_id] == 'FAIL':
        failed_expectations.append(exp_id)
        exp_metadata = lookup_expectation_metadata(exp_id, yaml_path)
        failed_columns.add(exp_metadata.get("column"))

failed_materials.append({
    "MATERIAL_NUMBER": material_number,
    "failed_columns": list(failed_columns),        # ← Now populated!
    "failure_count": len(failed_expectations),     # ← Now correct!
    "failed_expectations": failed_expectations     # ← Now populated!
})
```

---

### ✅ Issue #5: Wide-to-Long Format Conversion
**Problem**: Columnar DataFrame is wide format (one row per material), but UI needs long format (one row per failure).

**Solution**: Let `results_to_dataframe()` handle the conversion using our `failed_materials` lists.

**How it Works**:
```python
# Wide format (columnar DataFrame):
material_number | exp_a3f | exp_c49
10001           | FAIL    | PASS
10002           | PASS    | FAIL

# Our failed_materials:
[
    {"material_number": 10001, "Unexpected Value": NULL},  # for exp_a3f
    {"material_number": 10002, "Unexpected Value": "X"},   # for exp_c49
]

# results_to_dataframe() converts to long format:
Expectation Type | Column | Material Number | Unexpected Value
not_be_null      | TYPE   | 10001           | NULL
be_in_set        | STATUS | 10002           | "X"
```

---

### ✅ Issue #6: No Access to Actual Unexpected Values
**Problem**: Columnar DataFrame only has PASS/FAIL, not the actual invalid values.

**Solution**: Extract the actual value from the source column being validated (using metadata lookup to know which column).

**Code**:
```python
# Metadata lookup tells us which column is being validated
metadata = lookup_expectation_metadata(exp_col, yaml_path)
column = metadata.get("column")  # e.g., "MATERIAL_TYPE"

# Extract actual value from that source column
source_column = column.lower()
unexpected_value = row.get(source_column, row.get(source_column.upper()))

# Result: unexpected_value = NULL (or whatever the actual value was)
```

---

## Key Insight

The user's suggestion was brilliant: **use the YAML metadata to unfold the columnar format back into the expected structure**.

Instead of:
- ❌ Complex wide-to-long transformations
- ❌ Joining with raw data sources
- ❌ Building new data structures from scratch

We:
- ✅ Filter the columnar DataFrame for failures (simple: `df[df[exp_col] == 'FAIL']`)
- ✅ Use metadata lookup to know which source column to extract values from
- ✅ Build the expected `failed_materials` structure directly

This approach:
- Leverages the columnar format (easy filtering)
- Uses existing metadata infrastructure (YAML is source of truth)
- Produces the exact format the UI expects
- Requires minimal code changes

## Testing

To verify the fixes work:

```bash
# Run the test adapter script
python test_streamlit_adapter.py

# Check that:
# 1. results have failed_materials populated
# 2. Unexpected Value shows actual values (not just exp IDs)
# 3. expected field is included where applicable
# 4. Derived status shows failed_columns and failed_expectations
```

## Future Improvements

Once the adapter is validated, consider:

1. **Remove the adapter** - Update UI components to work with columnar format directly
2. **Optimize metadata lookups** - Cache YAML parsing results
3. **Add context columns** - Include plant, storage location, etc. in failed_materials
4. **Support more expectation types** - Add expected value extraction for all validation types

## Files Modified

- `app/pages/Validation_Report.py` - Enhanced adapter function
  - Populates failed_materials with actual unexpected values
  - Extracts expected values from YAML
  - Analyzes derived status failures properly
