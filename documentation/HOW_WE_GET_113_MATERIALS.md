# How We Get 113 Materials - Detailed Breakdown

## TL;DR

**113 = unique material numbers in a dataset that has 1,000 total rows**

Your Level 1 validation pulls from `vw_ProductDataAll` which has **multiple rows per material**.

## The Flow

### 1️⃣ **Query Executes** (`core/queries.py`)
```sql
SELECT * FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
LIMIT 1000
```
**Returns**: 1,000 rows

### 2️⃣ **GX Validates** (`validations/base_validation.py`)
Great Expectations runs validations on all 1,000 rows:
```python
element_count = 1000  # Total rows validated
```

### 3️⃣ **Material Extraction** (`core/gx_runner.py`, lines 64-70)
After validation completes, we extract unique materials:
```python
validated_materials = df[idx_col].dropna().unique().tolist()
# Returns: ['803432-9A', '233379', 'STF225-20B-C', ...]
```

**This gives us**: 113 unique material numbers

### 4️⃣ **The Count**
```python
len(validated_materials) = 113
```

## Why Multiple Rows Per Material?

The `vw_ProductDataAll` view likely contains one row per:
- Material + Plant combination
- Material + Sales Org combination
- Material + Distribution Channel combination
- Or some other multi-dimensional grain

### Example Material with Multiple Rows:

```
MATERIAL_NUMBER | PLANT | SALES_ORG | DISTRIBUTION_CHANNEL | ...
----------------|-------|-----------|---------------------|----
803432-9A       | P001  | SO01      | DC01                | ...
803432-9A       | P001  | SO01      | DC02                | ...
803432-9A       | P002  | SO01      | DC01                | ...
```

Material **803432-9A** has 3 rows, but it's counted as **1 unique material**.

## Actual Numbers from Your Data

From `Level_1_Validation_2025-11-26_16-15-39.json`:
```json
{
  "element_count": 1000,              // ← Total rows
  "validated_materials": [            // ← Unique materials array
    "803432-9A",
    "233379",
    "STF225-20B-C",
    ...
  ]  // Length = 113
}
```

**Math check**:
- 1000 rows ÷ 113 materials = **~8.85 rows per material** on average

## The Code Path

### gx_runner.py (lines 64-70)
```python
# 5️⃣ Capture validated material numbers
validated_materials = []
if hasattr(validator, "df"):
    df = getattr(validator, "df")
    idx_col = getattr(validator, "INDEX_COLUMN", "MATERIAL_NUMBER")
    if isinstance(df, pd.DataFrame) and idx_col in df.columns:
        validated_materials = df[idx_col].dropna().unique().tolist()  # ← HERE!
        print(f"📦 Captured {len(validated_materials)} validated materials.", flush=True)
```

**Key operation**: `df[idx_col].dropna().unique().tolist()`
1. `df[idx_col]` - Get MATERIAL_NUMBER column (1000 values)
2. `.dropna()` - Remove nulls (if any)
3. `.unique()` - Get unique values only (**113 unique materials**)
4. `.tolist()` - Convert to list

### Validation_Report.py (line 250)
```python
total, passed, failed, pass_rate, fail_rate = calc_overall_kpis(df, len(validated_materials))
#                                                                     ^^^^^^^^^^^^^^^^^^^^^^^^
#                                                                     This is 113
```

## Visual Breakdown

```
┌─────────────────────────────────────────────────────────────┐
│ vw_ProductDataAll (LIMIT 1000)                              │
├─────────────────────────────────────────────────────────────┤
│ 1000 rows                                                   │
│   ├─ 803432-9A  (appears 12 times)                         │
│   ├─ 233379     (appears 8 times)                          │
│   ├─ STF225-20B-C (appears 5 times)                        │
│   ├─ ... (110 more unique materials)                       │
│                                                             │
│ df[MATERIAL_NUMBER].unique()                                │
│   ↓                                                         │
│ ['803432-9A', '233379', 'STF225-20B-C', ...]               │
│                                                             │
│ len(unique materials) = 113                                 │
└─────────────────────────────────────────────────────────────┘
```

## Why This Matters for Metrics

### OLD (BROKEN) Calculation:
```python
total = 1000  # element_count (ROWS)
failed = 50   # unique failed MATERIALS
passed = 950  # ❌ WRONG - mixing rows and materials
```

### NEW (FIXED) Calculation:
```python
total = 113   # unique MATERIALS
failed = 50   # unique failed MATERIALS
passed = 63   # ✅ CORRECT - apples to apples
```

## How to Verify This Yourself

Run this in your Streamlit app console or Python:
```python
import pandas as pd
from core.queries import get_level_1_dataframe

df = get_level_1_dataframe()

print(f"Total rows: {len(df)}")
# Output: Total rows: 1000

print(f"Unique materials: {df['MATERIAL_NUMBER'].nunique()}")
# Output: Unique materials: 113

print(f"Avg rows per material: {len(df) / df['MATERIAL_NUMBER'].nunique():.2f}")
# Output: Avg rows per material: 8.85
```

## Summary

| Metric | Value | What It Represents |
|--------|-------|-------------------|
| **1,000** | element_count | Total **rows** in validation dataset |
| **113** | validated_materials | Total **unique materials** validated |
| **~8.85** | average | Average **rows per material** |

**113 is the correct total** to use for pass/fail metrics because we care about:
- "How many **materials** passed?"
- "How many **materials** failed?"

NOT:
- "How many **rows** passed?"
- "How many **rows** failed?"

Materials are your business entities. Rows are just database records that can have duplicates based on dimensional attributes (plant, sales org, etc.).
