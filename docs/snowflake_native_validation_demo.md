# Snowflake-Native Validation Demo

## Overview

This demo explores replacing Great Expectations (GX) with Snowflake-native SQL validations to improve performance on large datasets. The approach pushes all validation logic down to Snowflake using SQL aggregations and JSON functions, eliminating Python-side processing overhead.

## Problem Statement

Current GX-based validations may suffer from:
- Multiple round-trips between Python and Snowflake (one per expectation)
- Data transfer overhead (fetching data to Python for validation)
- GX framework overhead
- Limited parallelization across expectations

For large datasets (millions of rows), these overheads can significantly impact performance.

## Solution Approach

**Snowflake-Native Validation:**
- Single SQL query validates all columns at once
- All validation logic executes in Snowflake's distributed compute
- Uses `CASE` statements for validation logic
- Uses `ARRAY_AGG` and `OBJECT_CONSTRUCT` to build failure details
- Returns only summary metrics and failed materials (minimal data transfer)
- Produces identical output format to GX for compatibility with existing reports and Datalark

## Architecture

### Query Structure

```sql
WITH base_data AS (
  SELECT columns, context_columns
  FROM table
  WHERE filter_conditions
  LIMIT optional_limit
),
validation_results AS (
  SELECT
    -- For each column being validated:
    COUNT(*) as column_total,
    SUM(CASE WHEN column IS NULL THEN 1 ELSE 0 END) as column_null_count,
    ARRAY_AGG(
      CASE
        WHEN column IS NULL
        THEN OBJECT_CONSTRUCT(
          'MATERIAL_NUMBER', material_number,
          'Unexpected Value', column,
          'Sales Organization', sales_org,
          'Plant', plant,
          ...
        )
        ELSE NULL
      END
    ) FILTER (WHERE column IS NULL) as column_failures
  FROM base_data
)
SELECT * FROM validation_results
```

### Output Format

The Snowflake-native approach produces **identical output** to GX:

```json
{
  "results": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "column": "ABP_ELECTRICALDATA1",
      "success": false,
      "element_count": 1000,
      "unexpected_count": 50,
      "unexpected_percent": 5.0,
      "failed_materials": [
        {
          "MATERIAL_NUMBER": "123456",
          "Unexpected Value": null,
          "Sales Organization": "1000",
          "Plant": "0001"
        }
      ],
      "table_grain": "MARA",
      "unique_by": ["MATERIAL_NUMBER"]
    }
  ],
  "validated_materials": []
}
```

This ensures:
- ✅ Existing reports continue to work
- ✅ Datalark integration remains compatible
- ✅ Streamlit UI requires no changes
- ✅ File export format is identical

## Files

### Core Implementation

| File | Description |
|------|-------------|
| `validations/snowflake_native_validator.py` | Main validator class implementing SQL-based validation |
| `scripts/compare_validation_performance.py` | Performance comparison script (GX vs Snowflake-native) |

### Key Classes

#### `SnowflakeNativeValidator`

Main validation class that:
1. Generates SQL with validation logic for all columns
2. Executes single query in Snowflake
3. Parses results into GX-compatible format
4. Saves results to JSON files

**Methods:**
- `validate_not_null(columns, where_clause, limit)` - Validate columns are not null
- `save_results_to_file(results)` - Save results matching GX output format

## Usage

### Quick Test (Limited Data)

Test on 10,000 rows:

```bash
python scripts/compare_validation_performance.py --limit 10000
```

### Full Dataset Comparison

Run on complete dataset:

```bash
python scripts/compare_validation_performance.py
```

### Custom Suite

```bash
python scripts/compare_validation_performance.py \
  --yaml validation_yaml/your_suite.yaml \
  --limit 50000
```

### Programmatic Usage

```python
from validations.snowflake_native_validator import SnowflakeNativeValidator

# Initialize validator
validator = SnowflakeNativeValidator(
    suite_name="my_validation_suite",
    table_name='PROD_MO_MONM.REPORTING."vw_ProductDataAll"',
    index_column="MATERIAL_NUMBER"
)

# Run validation
results = validator.validate_not_null(
    columns=["COLUMN1", "COLUMN2", "COLUMN3"],
    where_clause="PRODUCT_HIERARCHY LIKE '5%'",
    limit=None  # Full dataset
)

# Save results
validator.save_results_to_file(results)
```

## Expected Performance Improvements

### Theoretical Advantages

1. **Reduced Round-Trips**
   - GX: N queries (one per column/expectation)
   - Snowflake-native: 1 query (all validations)
   - Expected improvement: ~Nx faster for query overhead

2. **Reduced Data Transfer**
   - GX: May fetch data to Python for validation
   - Snowflake-native: Only returns summary metrics
   - Expected improvement: 10-100x less data transferred

3. **Better Parallelization**
   - Snowflake can parallelize all validations within one query execution plan
   - GX runs expectations sequentially

4. **No Framework Overhead**
   - Direct SQL execution vs GX abstraction layer

### Real-World Factors

Performance gains depend on:
- Dataset size (larger = more benefit)
- Number of validations (more validations = more benefit)
- GX's actual query generation (may already be optimized)
- Network latency
- Warehouse size and availability

**Run the comparison script to measure actual performance on your data!**

## Comparison Output

The comparison script produces:

```
================================================================================
📊 PERFORMANCE COMPARISON
================================================================================

⏱️  Execution Time:
   GX:                    45.23 seconds
   Snowflake-native:      12.67 seconds
   Difference:            32.56 seconds
   Speedup:                3.57x

📋 Result Counts:
   GX validations:           20
   SF validations:           20
   Match:             ✅ Yes

🔍 Validation Results Comparison:
   ✅ ABP_ELECTRICALDATA1                       PASS   (GX:     0, SF:     0)
   ✅ ABP_EFFICIENCYLEVEL                       PASS   (GX:     0, SF:     0)
   ❌ ABP_INSULATIONCLASS                       FAIL   (GX:   150, SF:   150)
   ...

✅ All validation results match perfectly!

================================================================================
🏁 SUMMARY
================================================================================
✅ Snowflake-native is 3.57x FASTER (32.56s saved)
✅ Results are identical - Snowflake-native is a valid replacement!

💡 Recommendation:
   🚀 Strong candidate for migration to Snowflake-native!
   The performance improvement is significant and results match.
```

## Limitations & Considerations

### Current Limitations

1. **Read-Only Schema**
   - Cannot create tables/views in `PROD_MO_MONM.REPORTING`
   - All processing happens in-query
   - Results returned to Python for formatting

2. **Not Null Validations Only**
   - Demo implements `expect_column_values_to_not_be_null`
   - Other expectation types require additional SQL patterns

3. **Validated Materials List**
   - Currently empty in output (not critical for reports/Datalark)
   - Could be populated with additional query if needed

### Extensibility

The approach can be extended to support:

- **Value set validations**: `CASE WHEN column NOT IN (...)`
- **Numeric range validations**: `CASE WHEN column < min OR column > max`
- **Regex validations**: `CASE WHEN NOT RLIKE(column, pattern)`
- **Cross-column validations**: `CASE WHEN column1 IS NULL AND column2 IS NOT NULL`
- **Custom business rules**: Any SQL boolean expression

Example multi-type validation:

```sql
-- Not null check
SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as status_null_count,

-- Value set check
SUM(CASE WHEN status NOT IN ('active', 'inactive') THEN 1 ELSE 0 END) as status_invalid_count,

-- Range check
SUM(CASE WHEN price < 0 OR price > 100000 THEN 1 ELSE 0 END) as price_range_count
```

## Migration Path

If performance gains are significant:

### Phase 1: Parallel Running (Current Demo)
- Run both GX and Snowflake-native in parallel
- Compare results for accuracy
- Build confidence in Snowflake approach

### Phase 2: Selective Migration
- Migrate high-volume suites first (biggest performance wins)
- Keep GX for complex validations that are harder to express in SQL

### Phase 3: Full Migration (Optional)
- Build Snowflake-native validator for all expectation types
- Deprecate GX dependency
- Maintain same output format for compatibility

### Phase 4: Native Snowflake Integration
- Consider writing results directly to Snowflake tables (if write access obtained)
- Eliminate file-based exports
- Query validation history directly from Snowflake

## Testing Recommendations

1. **Start Small**: Test with `--limit 1000` to verify correctness
2. **Scale Up**: Test with `--limit 10000`, `--limit 100000`
3. **Full Run**: Compare on complete dataset
4. **Verify Output**: Ensure both approaches produce identical failed_materials
5. **Multiple Suites**: Test on different validation suites
6. **Peak Hours**: Test during different warehouse load periods

## Next Steps

1. ✅ Run comparison on `abb_shop_abp_data_presence` suite
2. ✅ Verify output format compatibility
3. ✅ Measure performance improvements
4. ⏳ Extend to other expectation types if results are promising
5. ⏳ Test on largest validation suites for maximum impact
6. ⏳ Consider migration strategy based on results

## Questions to Answer

Through this demo, we aim to answer:

1. **How much faster is Snowflake-native?**
   - Run comparison script to measure
   - Test on different dataset sizes

2. **Do results match exactly?**
   - Compare validation pass/fail status
   - Compare unexpected counts
   - Compare failed materials lists

3. **Is output format compatible?**
   - Test with existing reporting UI
   - Test with Datalark integration
   - Verify file exports are identical

4. **What's the migration effort?**
   - How many expectation types do we use?
   - How complex are they to translate to SQL?
   - Can we automate the translation?

5. **What are the trade-offs?**
   - Code maintainability
   - SQL complexity
   - Debugging experience
   - Developer familiarity

## Contact & Feedback

After running the comparison, consider:
- What was the speedup factor?
- Did results match perfectly?
- Were there any unexpected issues?
- Is this approach worth pursuing further?

Share findings to inform the decision on whether to migrate away from GX.
