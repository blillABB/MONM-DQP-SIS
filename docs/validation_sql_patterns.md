# Validation Type → SQL Pattern Mapping

This document shows how each GX expectation type (standard + custom) translates to Snowflake SQL patterns.

**All patterns work with read-only access** - they use CTEs, CASE statements, and window functions without requiring temp tables or views.

---

## 1. ✅ expect_column_values_to_not_be_null

**GX YAML:**
```yaml
- type: expect_column_values_to_not_be_null
  columns: [MATERIAL_NUMBER, BASE_UNIT]
```

**SQL Pattern:**
```sql
-- Count nulls
SUM(CASE WHEN MATERIAL_NUMBER IS NULL THEN 1 ELSE 0 END) as material_number_null_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN MATERIAL_NUMBER IS NULL
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'Unexpected Value', NULL,
      'Column', 'MATERIAL_NUMBER'
    )
    ELSE NULL
  END
)) as material_number_failures
```

**Status:** ✅ Already implemented

---

## 2. ✅ expect_column_values_to_be_in_set

**GX YAML:**
```yaml
- type: expect_column_values_to_be_in_set
  rules:
    MATERIAL_TYPE: ["FERT", "HALB", "ROH"]
    STATUS: ["ACTIVE", "INACTIVE"]
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE WHEN MATERIAL_TYPE NOT IN ('FERT', 'HALB', 'ROH') THEN 1 ELSE 0 END) as material_type_invalid_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN MATERIAL_TYPE NOT IN ('FERT', 'HALB', 'ROH')
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'Unexpected Value', MATERIAL_TYPE,
      'Expected', 'One of: FERT, HALB, ROH'
    )
    ELSE NULL
  END
)) as material_type_failures
```

**Complexity:** Easy - just change `IS NULL` to `NOT IN (...)`

---

## 3. ✅ expect_column_values_to_not_be_in_set

**GX YAML:**
```yaml
- type: expect_column_values_to_not_be_in_set
  column: PROFIT_CENTER
  value_set: ["UNDEFINED", "UNKNOWN"]
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE WHEN PROFIT_CENTER IN ('UNDEFINED', 'UNKNOWN') THEN 1 ELSE 0 END) as profit_center_invalid_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN PROFIT_CENTER IN ('UNDEFINED', 'UNKNOWN')
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'Unexpected Value', PROFIT_CENTER,
      'Violation', 'Value must not be UNDEFINED or UNKNOWN'
    )
    ELSE NULL
  END
)) as profit_center_failures
```

**Complexity:** Easy - inverse of `to_be_in_set`

---

## 4. ✅ expect_column_values_to_match_regex

**GX YAML:**
```yaml
- type: expect_column_values_to_match_regex
  columns: [PACK_INDICATOR, PURCHASING_VALUE_GROUP]
  regex: "^\\s*$"  # blank values only
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE WHEN NOT RLIKE(PACK_INDICATOR, '^\\s*$') THEN 1 ELSE 0 END) as pack_indicator_regex_fail_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN NOT RLIKE(PACK_INDICATOR, '^\\s*$')
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'Unexpected Value', PACK_INDICATOR,
      'Expected Pattern', '^\\s*$'
    )
    ELSE NULL
  END
)) as pack_indicator_failures
```

**Snowflake Functions:**
- `RLIKE(column, pattern)` - Regex matching
- `REGEXP_LIKE(column, pattern)` - Alternative syntax
- `REGEXP_SUBSTR()` - Extract matching parts

**Complexity:** Easy - Snowflake has full regex support

---

## 5. ✅ expect_column_pair_values_to_be_equal

**GX YAML:**
```yaml
- type: expect_column_pair_values_to_be_equal
  column_a: ITEM_CATEGORY_GROUP
  column_b: SALES_ITEM_CATEGORY_GROUP
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE
  WHEN ITEM_CATEGORY_GROUP != SALES_ITEM_CATEGORY_GROUP
    OR (ITEM_CATEGORY_GROUP IS NULL AND SALES_ITEM_CATEGORY_GROUP IS NOT NULL)
    OR (ITEM_CATEGORY_GROUP IS NOT NULL AND SALES_ITEM_CATEGORY_GROUP IS NULL)
  THEN 1
  ELSE 0
END) as column_pair_mismatch_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN ITEM_CATEGORY_GROUP != SALES_ITEM_CATEGORY_GROUP
      OR (ITEM_CATEGORY_GROUP IS NULL AND SALES_ITEM_CATEGORY_GROUP IS NOT NULL)
      OR (ITEM_CATEGORY_GROUP IS NOT NULL AND SALES_ITEM_CATEGORY_GROUP IS NULL)
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'ITEM_CATEGORY_GROUP', ITEM_CATEGORY_GROUP,
      'SALES_ITEM_CATEGORY_GROUP', SALES_ITEM_CATEGORY_GROUP,
      'Violation', 'Columns must be equal'
    )
    ELSE NULL
  END
)) as column_pair_failures
```

**Complexity:** Easy - direct column comparison with null handling

---

## 6. ✅ expect_column_pair_values_a_to_be_greater_than_b

**GX YAML:**
```yaml
- type: expect_column_pair_values_a_to_be_greater_than_b
  column_a: GROSS_WEIGHT
  column_b: NET_WEIGHT
  or_equal: true
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE
  WHEN GROSS_WEIGHT < NET_WEIGHT  -- or <= if or_equal is false
    OR GROSS_WEIGHT IS NULL
    OR NET_WEIGHT IS NULL
  THEN 1
  ELSE 0
END) as weight_comparison_fail_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN GROSS_WEIGHT < NET_WEIGHT OR GROSS_WEIGHT IS NULL OR NET_WEIGHT IS NULL
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'GROSS_WEIGHT', GROSS_WEIGHT,
      'NET_WEIGHT', NET_WEIGHT,
      'Violation', 'GROSS_WEIGHT must be >= NET_WEIGHT'
    )
    ELSE NULL
  END
)) as weight_comparison_failures
```

**Complexity:** Easy - numeric comparison operators

---

## 7. ✅ conditional_required (Custom)

**Description:** If COL_A in [values], then COL_B must not be null

**GX YAML:**
```yaml
- type: custom:conditional_required
  condition_column: MATERIAL_TYPE
  condition_values: ["FERT", "HALB"]
  required_column: BOM_STATUS
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE
  WHEN MATERIAL_TYPE IN ('FERT', 'HALB') AND BOM_STATUS IS NULL
  THEN 1
  ELSE 0
END) as conditional_required_fail_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN MATERIAL_TYPE IN ('FERT', 'HALB') AND BOM_STATUS IS NULL
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'MATERIAL_TYPE', MATERIAL_TYPE,
      'BOM_STATUS', BOM_STATUS,
      'Violation', 'When MATERIAL_TYPE is FERT or HALB, BOM_STATUS is required'
    )
    ELSE NULL
  END
)) as conditional_required_failures
```

**Your Example:** "if COL_A = 'A', COL_B = 'B', COL_C = 'C' THEN COL_D = 'D'"
```sql
SUM(CASE
  WHEN COL_A = 'A' AND COL_B = 'B' AND COL_C = 'C' AND COL_D != 'D'
  THEN 1
  ELSE 0
END) as custom_rule_violation_count,

ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN COL_A = 'A' AND COL_B = 'B' AND COL_C = 'C' AND COL_D != 'D'
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'COL_A', COL_A,
      'COL_B', COL_B,
      'COL_C', COL_C,
      'COL_D', COL_D,
      'Expected COL_D', 'D',
      'Violation', 'When A/B/C are set, D must equal D'
    )
    ELSE NULL
  END
)) as custom_rule_failures
```

**Complexity:** Easy - nested AND conditions in CASE statement

---

## 8. ✅ conditional_value_in_set (Custom)

**Description:** If COL_A in [values], then COL_B must be in [values]

**GX YAML:**
```yaml
- type: custom:conditional_value_in_set
  condition_column: MATERIAL_TYPE
  condition_values: ["FERT"]
  target_column: MATERIAL_GROUP_4
  allowed_values: ["END", "RAE", "SAE"]
```

**SQL Pattern:**
```sql
-- Count violations
SUM(CASE
  WHEN MATERIAL_TYPE = 'FERT'
    AND MATERIAL_GROUP_4 NOT IN ('END', 'RAE', 'SAE')
  THEN 1
  ELSE 0
END) as conditional_value_fail_count,

-- Collect failures
ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN MATERIAL_TYPE = 'FERT' AND MATERIAL_GROUP_4 NOT IN ('END', 'RAE', 'SAE')
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'MATERIAL_TYPE', MATERIAL_TYPE,
      'MATERIAL_GROUP_4', MATERIAL_GROUP_4,
      'Expected', 'One of: END, RAE, SAE',
      'Violation', 'When MATERIAL_TYPE is FERT, MG4 must be END/RAE/SAE'
    )
    ELSE NULL
  END
)) as conditional_value_failures
```

**Complexity:** Easy - combines condition check + value set check

---

## 9. ✅ lookup_in_reference_column (Custom)

**Description:** Foreign key validation - check if values exist in another table/column

**GX YAML:**
```yaml
- type: custom:lookup_in_reference_column
  column: DELIVERING_PLANT
  reference_column: PLANT
```

**SQL Pattern (Same Table):**
```sql
WITH base_data AS (
  SELECT * FROM vw_ProductDataAll WHERE ...
),
valid_plants AS (
  SELECT DISTINCT PLANT FROM base_data
)
SELECT
  SUM(CASE
    WHEN base_data.DELIVERING_PLANT NOT IN (SELECT PLANT FROM valid_plants)
    THEN 1
    ELSE 0
  END) as lookup_fail_count,

  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN base_data.DELIVERING_PLANT NOT IN (SELECT PLANT FROM valid_plants)
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', base_data.MATERIAL_NUMBER,
        'DELIVERING_PLANT', base_data.DELIVERING_PLANT,
        'Violation', 'DELIVERING_PLANT not found in valid PLANT values'
      )
      ELSE NULL
    END
  )) as lookup_failures
FROM base_data
```

**SQL Pattern (Cross-Table with JOIN):**
```sql
WITH base_data AS (
  SELECT * FROM vw_ProductDataAll WHERE ...
),
reference_table AS (
  SELECT DISTINCT PLANT FROM vw_PlantMaster WHERE STATUS = 'ACTIVE'
)
SELECT
  SUM(CASE
    WHEN reference_table.PLANT IS NULL  -- LEFT JOIN returns NULL if not found
    THEN 1
    ELSE 0
  END) as lookup_fail_count,

  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN reference_table.PLANT IS NULL
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', base_data.MATERIAL_NUMBER,
        'PLANT', base_data.PLANT,
        'Violation', 'PLANT not found in PlantMaster'
      )
      ELSE NULL
    END
  )) as lookup_failures
FROM base_data
LEFT JOIN reference_table ON base_data.PLANT = reference_table.PLANT
```

**Complexity:** Moderate - requires CTEs or subqueries, but totally doable

**Note:** With read-only access, we can:
- ✅ Use CTEs to define reference sets
- ✅ JOIN to other tables/views in the schema
- ✅ Use `IN (subquery)` for lookups
- ✅ Use `EXISTS` / `NOT EXISTS` for existence checks

---

## 10. 🎯 Future-Proof: Any Complex SQL Logic

The beauty of this approach is that **any business rule that can be expressed in SQL can be validated**.

### Example: Multi-Column Complex Rules

**Business Rule:** "If MATERIAL_TYPE is FERT and SALES_STATUS is 10, then PRICING_GROUP must be AM and VALUATION_CLASS must start with 79"

```sql
SUM(CASE
  WHEN MATERIAL_TYPE = 'FERT'
    AND SALES_STATUS = '10'
    AND (PRICING_GROUP != 'AM' OR NOT RLIKE(VALUATION_CLASS, '^79'))
  THEN 1
  ELSE 0
END) as complex_rule_violations,

ARRAY_COMPACT(ARRAY_AGG(
  CASE
    WHEN MATERIAL_TYPE = 'FERT'
      AND SALES_STATUS = '10'
      AND (PRICING_GROUP != 'AM' OR NOT RLIKE(VALUATION_CLASS, '^79'))
    THEN OBJECT_CONSTRUCT(
      'MATERIAL_NUMBER', MATERIAL_NUMBER,
      'MATERIAL_TYPE', MATERIAL_TYPE,
      'SALES_STATUS', SALES_STATUS,
      'PRICING_GROUP', PRICING_GROUP,
      'VALUATION_CLASS', VALUATION_CLASS,
      'Violation', 'Complex rule: FERT+10 requires AM pricing and 79xx valuation'
    )
    ELSE NULL
  END
)) as complex_rule_failures
```

### Example: Window Functions (Duplicates)

**Check for duplicate MATERIAL_NUMBER within PLANT:**

```sql
WITH duplicates AS (
  SELECT
    *,
    COUNT(*) OVER (PARTITION BY MATERIAL_NUMBER, PLANT) as dup_count
  FROM base_data
)
SELECT
  SUM(CASE WHEN dup_count > 1 THEN 1 ELSE 0 END) as duplicate_count,

  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN dup_count > 1
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', MATERIAL_NUMBER,
        'PLANT', PLANT,
        'Duplicate Count', dup_count,
        'Violation', 'Duplicate MATERIAL_NUMBER within PLANT'
      )
      ELSE NULL
    END
  )) as duplicate_failures
FROM duplicates
```

### Example: Aggregate Validations

**Sum of QUANTITY across all rows for a material must equal TOTAL_QUANTITY:**

```sql
WITH aggregates AS (
  SELECT
    MATERIAL_NUMBER,
    SUM(QUANTITY) as calculated_total,
    MAX(TOTAL_QUANTITY) as expected_total
  FROM base_data
  GROUP BY MATERIAL_NUMBER
)
SELECT
  SUM(CASE
    WHEN calculated_total != expected_total
    THEN 1
    ELSE 0
  END) as aggregate_mismatch_count,

  ARRAY_COMPACT(ARRAY_AGG(
    CASE
      WHEN calculated_total != expected_total
      THEN OBJECT_CONSTRUCT(
        'MATERIAL_NUMBER', MATERIAL_NUMBER,
        'Calculated Total', calculated_total,
        'Expected Total', expected_total,
        'Violation', 'Sum of QUANTITY does not match TOTAL_QUANTITY'
      )
      ELSE NULL
    END
  )) as aggregate_failures
FROM aggregates
```

---

## Summary: Can We Do It All?

| Validation Type | Snowflake SQL | Read-Only OK? |
|-----------------|---------------|---------------|
| Not null checks | ✅ `CASE WHEN col IS NULL` | ✅ Yes |
| Value in set | ✅ `CASE WHEN col NOT IN (...)` | ✅ Yes |
| Regex matching | ✅ `RLIKE(col, pattern)` | ✅ Yes |
| Column comparisons | ✅ `CASE WHEN col_a != col_b` | ✅ Yes |
| Conditional logic | ✅ Nested `CASE` with `AND`/`OR` | ✅ Yes |
| Cross-table lookups | ✅ `JOIN` or `IN (subquery)` | ✅ Yes |
| Window functions | ✅ `COUNT(*) OVER (PARTITION BY)` | ✅ Yes |
| Aggregates | ✅ `SUM()`, `AVG()`, `COUNT()` in CTEs | ✅ Yes |
| Complex business rules | ✅ Any SQL boolean expression | ✅ Yes |

**Answer: YES** - We can replicate all GX functionality + custom expectations using Snowflake SQL with read-only access.

---

## Extensibility Architecture

To handle future validation needs, we can design a flexible YAML schema:

```yaml
metadata:
  suite_name: flexible_validation_example
  data_source: abb_shop_data

validations:
  # Standard built-in types (pre-defined SQL templates)
  - type: expect_column_values_to_not_be_null
    columns: [COL_A, COL_B]

  # Custom conditional logic (flexible SQL expression)
  - type: custom_sql_condition
    name: "A/B/C implies D"
    description: "When A/B/C are set, D must be D"
    condition: "COL_A = 'A' AND COL_B = 'B' AND COL_C = 'C' AND COL_D != 'D'"
    failure_columns: [COL_A, COL_B, COL_C, COL_D]
    message: "When A/B/C are set, D must equal 'D'"

  # Raw SQL (maximum flexibility for unforeseen needs)
  - type: raw_sql
    name: "Complex business rule"
    sql: |
      SELECT
        MATERIAL_NUMBER,
        'Complex rule violated' as violation
      FROM base_data
      WHERE (condition1 AND condition2) OR (condition3 AND NOT condition4)
```

This gives you:
1. **Speed**: Standard types use optimized SQL templates
2. **Flexibility**: Custom SQL for edge cases
3. **Maintainability**: YAML config (not Python code)
4. **Extensibility**: Add new patterns without changing core code

---

## Performance Comparison

Based on your results:

| Approach | Time (750k rows) | Notes |
|----------|------------------|-------|
| GX Chunked | ~40 minutes | 6 threads, 75k rows/chunk, 20 validations |
| Snowflake-Native | **75 seconds** | Single query, all validations |
| **Speedup** | **~32x faster** | 🚀 |

The Snowflake-native approach is dramatically faster because:
- Single execution plan (not N queries)
- All compute in Snowflake's distributed engine
- No Python bottleneck
- Minimal data transfer (only failures)

---

## Recommendation

✅ **Migrate to Snowflake-native validation**

The approach is:
- **Faster** (32x on large datasets)
- **Flexible** (handles all current + future validation types)
- **Simpler** (less moving parts, no chunking complexity)
- **Compatible** (same output format for reports/Datalark)

Next steps:
1. Extend validator to support all validation types above
2. Create YAML translator (GX format → Snowflake SQL)
3. Test on remaining validation suites
4. Migrate production suites one by one
