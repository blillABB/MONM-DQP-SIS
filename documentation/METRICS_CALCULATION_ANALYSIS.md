# Metrics Calculation Analysis - Bug Identified ❌

## The Problem

The current metrics calculation is **comparing apples to oranges**:
- **Total** uses `element_count` (total ROWS in dataset, including duplicates)
- **Failed** uses unique Material Numbers
- **Passed** = Total rows - Failed unique materials ❌ **This is wrong!**

## Example Data (from actual validation results)

```json
{
  "element_count": 1000,                 // Total ROWS in validation dataset
  "validated_materials_count": 113,      // Total UNIQUE materials
  "unexpected_count": 694                // Total ROWS that failed
}
```

This dataset has **~8.8 rows per material** (1000 rows ÷ 113 materials).

### Sample failed_materials array:
```json
[
  {"MATERIAL_NUMBER": "803432-9A", "Unexpected Value": null},
  {"MATERIAL_NUMBER": "803432-9A", "Unexpected Value": null},  // Duplicate!
  {"MATERIAL_NUMBER": "803432-9A", "Unexpected Value": null},  // Duplicate!
  {"MATERIAL_NUMBER": "233379", "Unexpected Value": null},
  {"MATERIAL_NUMBER": "233379", "Unexpected Value": null},     // Duplicate!
  ...
]
```

**Material "803432-9A" has 3 failing rows** but should only be counted **once** as a failed material.

## Current (Broken) Calculation

### Code:
```python
def calc_overall_kpis(df, validated_materials_count=0):
    total = 0
    if not df.empty and "Element Count" in df.columns:
        max_count = df["Element Count"].max()  # ❌ Gets element_count (1000 rows)
        total = int(max_count) if pd.notna(max_count) else 0

    # Falls back to validated_materials_count only if total is 0
    if total == 0 and validated_materials_count > 0:
        total = validated_materials_count  # ✅ Correct value, but not reached!

    failed = df["Material Number"].dropna().nunique()  # ✅ Unique failed materials
    passed = max(total - failed, 0)  # ❌ Subtracting unique materials from total rows!
    ...
```

### Example Output (with 50 unique failed materials):
- **Total Materials**: 1,000 ❌ (this is total ROWS, not materials!)
- **Materials Failing**: 50 ✅ (correct - unique materials)
- **Materials Passing**: 950 ❌ (completely wrong - mixing rows and unique counts)
- **Pass Rate**: 95.0% ❌ (should be much lower!)

## Correct Calculation

### What we should do:
```python
def calc_overall_kpis(df, validated_materials_count=0):
    # ALWAYS use validated_materials_count as the total
    total = validated_materials_count  # ✅ Total UNIQUE materials validated

    # Count unique failed materials
    failed = df["Material Number"].dropna().nunique()  # ✅ Unique failed materials

    # Calculate passed
    passed = max(total - failed, 0)  # ✅ Now comparing apples to apples!

    fail_rate = (failed / total * 100) if total > 0 else 0
    pass_rate = 100 - fail_rate
    return total, passed, failed, pass_rate, fail_rate
```

### Corrected Example Output (with actual data):
- **Total Materials**: 113 ✅ (unique materials validated)
- **Materials Failing**: 50 ✅ (unique materials with failures)
- **Materials Passing**: 63 ✅ (113 - 50)
- **Pass Rate**: 55.8% ✅ (accurate representation)

## Why This Happens

### Data Structure:
Many validation datasets have **multiple rows per material** because:
- Materials can have multiple plants
- Materials can have multiple storage locations
- Materials can have multiple distribution channels
- Level 1 validation pulls from `V_MDM_VWMASTER_CURR` which has ~8.8 rows per material

### GX Element Count:
Great Expectations `element_count` = **total rows validated**, NOT unique materials.

## Impact

### Broken Metrics Show:
- Inflated total counts (1000 instead of 113)
- Inflated pass counts (950 instead of 63)
- Inflated pass rates (95% instead of 55.8%)
- **Misleading dashboard** that makes data quality look much better than it is

### Example Real-World Impact:
If you have:
- 1,000 rows in validation dataset
- 113 unique materials
- 50 materials with failures (44.2% failure rate)

Current display:
```
Total Materials: 1,000
Materials Passing: 950
Pass Rate: 95% 🟢
```

Actual reality:
```
Total Materials: 113
Materials Passing: 63
Pass Rate: 55.8% 🔴
```

**This is a critical bug** - executives might think data quality is excellent when it's actually concerning!

## Root Cause

The logic has a backwards priority:
1. ❌ **First** tries to use `element_count` (total rows)
2. ✅ **Only falls back** to `validated_materials_count` if element_count is 0

Should be:
1. ✅ **First** tries to use `validated_materials_count` (unique materials)
2. ❌ **Only falls back** to element_count if validated_materials_count is unavailable (backward compatibility)

## Backward Compatibility Concern

Older cached results might not have `validated_materials` in the cache. The fix should:
1. **Prefer** `validated_materials_count` when available
2. **Warn** when falling back to element_count (indicating stale cache)
3. **Recommend** clearing cache to get accurate metrics

## The Fix

### Option 1: Simple Fix (Swap Priority)
```python
def calc_overall_kpis(df, validated_materials_count=0):
    # Prefer validated_materials_count (unique materials)
    total = validated_materials_count

    # Fall back to element_count only if validated_materials_count is unavailable
    if total == 0 and not df.empty and "Element Count" in df.columns:
        max_count = df["Element Count"].max()
        total = int(max_count) if pd.notna(max_count) else 0
        # Warn user that metrics may be inaccurate
        print("⚠️ Using element_count for total (cache may be stale). Clear cache for accurate metrics.")

    failed = df["Material Number"].dropna().nunique()
    passed = max(total - failed, 0)
    fail_rate = (failed / total * 100) if total > 0 else 0
    pass_rate = 100 - fail_rate
    return total, passed, failed, pass_rate, fail_rate
```

### Option 2: Strict Fix (Require validated_materials)
```python
def calc_overall_kpis(df, validated_materials_count=0):
    if validated_materials_count == 0:
        raise ValueError(
            "validated_materials_count is required for accurate metrics. "
            "Clear the cache and re-run validation."
        )

    total = validated_materials_count
    failed = df["Material Number"].dropna().nunique()
    passed = max(total - failed, 0)
    fail_rate = (failed / total * 100) if total > 0 else 0
    pass_rate = 100 - fail_rate
    return total, passed, failed, pass_rate, fail_rate
```

## Recommendation

**Use Option 1** (swap priority with warning) because:
- ✅ Fixes the bug immediately
- ✅ Maintains backward compatibility with old cache
- ✅ Warns users when metrics might be inaccurate
- ✅ Minimal code change
- ✅ Users can clear cache at their convenience

Then update cache schema documentation to note that `validated_materials` is required for accurate metrics.
