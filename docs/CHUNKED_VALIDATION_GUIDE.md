# Chunked Validation Guide

## Overview

This guide explains how to use the **streaming chunked validation** feature for large datasets. Instead of loading entire datasets into memory, data is fetched and validated in manageable chunks, enabling:

- ✅ Validation of datasets with **millions of rows**
- ✅ **Concurrent processing** of multiple chunks
- ✅ **Adaptive performance tuning** that learns optimal settings
- ✅ **Memory efficient** - never loads full dataset at once
- ✅ **100% backward compatible** - existing validations work unchanged

---

## When to Use Chunking

### Use Chunking When:
- ✅ Your dataset has **100k+ rows**
- ✅ Validation takes **more than 2-3 minutes**
- ✅ You encounter **memory issues** with large datasets
- ✅ You want **faster validation** through concurrent processing

### Don't Use Chunking When:
- ❌ Your dataset has **less than 50k rows** (overhead not worth it)
- ❌ You need **cross-row validations** that depend on seeing all data at once
- ❌ You're doing exploratory validation and want immediate feedback

---

## Quick Start

### 1. Enable Chunking in Your YAML

Add a `chunking` section to your validation YAML metadata:

```yaml
metadata:
  suite_name: "MyValidation"
  index_column: "MATERIAL_NUMBER"
  data_source: "get_my_dataframe"

  # Add this section to enable chunking
  chunking:
    enabled: true                # Turn on streaming validation
    target_time_seconds: 300     # Target: complete in 5 minutes
    initial_chunk_size: 75000    # Start with 75k rows per chunk
    max_workers: 4               # Process 4 chunks concurrently

validations:
  # Your validation rules (no changes needed!)
  - type: "expect_column_values_to_not_be_null"
    columns: ["MATERIAL_NUMBER", "DESCRIPTION"]
```

### 2. Run Validation Normally

No code changes needed! Run your validation the same way:

```python
from core.gx_runner import run_validation_from_yaml

results = run_validation_from_yaml("validation_yaml/MyValidation.yaml")
```

The system automatically detects chunking is enabled and uses the streaming runner.

---

## Configuration Parameters

### `chunking.enabled` (boolean)
- **Default:** `false`
- **Description:** Enable/disable chunked validation
- **Example:** `enabled: true`

### `chunking.target_time_seconds` (integer)
- **Default:** `300` (5 minutes)
- **Description:** Target total validation time in seconds
- **How it works:** Performance tuner uses this to calculate optimal chunk size
- **Example:** `target_time_seconds: 180` (3 minutes)

### `chunking.initial_chunk_size` (integer)
- **Default:** `75000`
- **Description:** Number of rows per chunk for the first run
- **Range:** Recommended 10,000 - 200,000
- **How it works:** After first run, system auto-adjusts based on performance
- **Example:** `initial_chunk_size: 100000`

### `chunking.max_workers` (integer)
- **Default:** `4`
- **Description:** Maximum number of chunks to process concurrently
- **Range:** Recommended 2-8 (depends on available CPU cores)
- **Note:** More workers = faster, but diminishing returns after 6-8
- **Example:** `max_workers: 6`

---

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│ 1. YAML Config Loaded                                       │
│    - Detects chunking: enabled = true                       │
│    - Returns StreamingValidationRunner instead of           │
│      BaseValidationSuite                                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Performance Tuner Checks History                         │
│    - Loads past performance data                            │
│    - Calculates optimal chunk size                          │
│    - May adjust from initial_chunk_size                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. Streaming Validation Begins                              │
│    - Fetch chunk 1 (LIMIT 75000 OFFSET 0)                  │
│    - Fetch chunk 2 (LIMIT 75000 OFFSET 75000)              │
│    - Fetch chunk 3 (LIMIT 75000 OFFSET 150000)             │
│    - ... (up to max_workers chunks fetched concurrently)   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. Concurrent Validation (ThreadPoolExecutor)               │
│    - Worker 1: Validate chunk 1 with GX                    │
│    - Worker 2: Validate chunk 2 with GX                    │
│    - Worker 3: Validate chunk 3 with GX                    │
│    - Worker 4: Validate chunk 4 with GX                    │
│    - As chunks complete, new chunks are fetched            │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. Result Merging                                           │
│    - Group results by (expectation_type, column)           │
│    - Sum element_count, unexpected_count                   │
│    - Recalculate unexpected_percent                        │
│    - Combine failed_materials lists                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 6. Performance Recording                                    │
│    - Save metrics: chunk_size, time, throughput            │
│    - Store in: validation_results/performance/             │
│    - Used for next run's optimization                      │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow Example

For a dataset with **300,000 rows** and `chunk_size=75000`, `max_workers=4`:

1. **Fetch Phase:**
   - Chunk 1: `SELECT * FROM table LIMIT 75000 OFFSET 0`
   - Chunk 2: `SELECT * FROM table LIMIT 75000 OFFSET 75000`
   - Chunk 3: `SELECT * FROM table LIMIT 75000 OFFSET 150000`
   - Chunk 4: `SELECT * FROM table LIMIT 75000 OFFSET 225000`

2. **Validate Phase** (all 4 chunks validate concurrently):
   - Each chunk creates a temporary GX validator
   - All expectations run on each chunk independently
   - Results captured with chunk-level metrics

3. **Merge Phase:**
   - Results aggregated by expectation type + column
   - Example: "expect_column_values_to_not_be_null on MATERIAL_NUMBER"
     - Chunk 1: 5 failures out of 75k rows
     - Chunk 2: 3 failures out of 75k rows
     - Chunk 3: 0 failures out of 75k rows
     - Chunk 4: 2 failures out of 75k rows
     - **Merged:** 10 failures out of 300k rows (0.0033%)

---

## Adaptive Performance Tuning

The system **learns** from each validation run and automatically optimizes chunk size.

### How Auto-Tuning Works

1. **First Run:**
   - Uses `initial_chunk_size` from YAML (e.g., 75,000)
   - Records: chunk_size, total_time, throughput (rows/sec)
   - Saves to: `validation_results/performance/{suite_name}_performance.json`

2. **Subsequent Runs:**
   - Loads performance history
   - Calculates average throughput from last 10 runs
   - Estimates optimal chunk_size to hit `target_time_seconds`
   - Uses formula:
     ```python
     estimated_chunks = (target_time / avg_time_per_chunk) * max_workers
     optimal_chunk_size = median_total_rows / estimated_chunks
     ```
   - Constrains between 10,000 and 200,000 rows

3. **Continuous Learning:**
   - Each run adds to performance history
   - System adapts to changes in:
     - Data size (more/fewer rows)
     - Data complexity (more expensive validations)
     - System resources (faster/slower hardware)

### Example Performance Evolution

| Run | Chunk Size | Workers | Total Rows | Time | Throughput | Notes |
|-----|------------|---------|------------|------|------------|-------|
| 1   | 75,000     | 4       | 1,000,000  | 8m   | 2,083/sec  | First run (initial setting) |
| 2   | 85,000     | 4       | 1,000,000  | 7m   | 2,381/sec  | System increased chunk size |
| 3   | 90,000     | 4       | 1,000,000  | 6.5m | 2,564/sec  | Further optimization |
| 4   | 95,000     | 4       | 1,000,000  | 6m   | 2,778/sec  | Converging on optimal |
| 5   | 95,000     | 4       | 1,000,000  | 5.8m | 2,874/sec  | Stable performance |

---

## Performance Monitoring

### View Performance History

Performance metrics are stored in JSON files:

```bash
cat validation_results/performance/Aurora_Motors_Validation_performance.json
```

Example output:

```json
[
  {
    "timestamp": "2024-01-15T10:30:00",
    "chunk_size": 75000,
    "max_workers": 4,
    "total_rows": 1000000,
    "total_time": 480.5,
    "num_chunks": 14,
    "throughput": 2081.4,
    "time_per_row": 0.00048
  },
  {
    "timestamp": "2024-01-15T11:45:00",
    "chunk_size": 85000,
    "max_workers": 4,
    "total_rows": 1000000,
    "total_time": 420.2,
    "num_chunks": 12,
    "throughput": 2380.1,
    "time_per_row": 0.00042
  }
]
```

### Console Output

During validation, you'll see detailed progress:

```
▶ Chunking enabled for Aurora_Motors_Validation
📊 Performance tuner suggests chunk size: 85,000 (was 75,000)
▶ Starting streaming validation: Aurora_Motors_Validation
   Chunk size: 85,000 rows
   Max workers: 4
   Total rows: 1,000,000 (~12 chunks)

  [Chunk 1/12] Validating 85,000 rows...
  [Chunk 2/12] Validating 85,000 rows...
  [Chunk 3/12] Validating 85,000 rows...
  [Chunk 4/12] Validating 85,000 rows...
  [Chunk 1/12] ✅ Completed in 34.2s
  [Chunk 5/12] Validating 85,000 rows...
  [Chunk 2/12] ✅ Completed in 35.1s
  ...

▶ Merging results from 12 chunks...
✅ Merged into 42 unique expectations

✅ Streaming validation complete:
   Total time: 420.2s (7.0 minutes)
   Chunks processed: 12
   Validated materials: 1,000,000
   Throughput: 2,380 materials/second

📊 Performance recorded: 2380 rows/sec (85,000 chunk size, 4 workers)
```

---

## Troubleshooting

### Problem: "Data source doesn't support count_only"

**Cause:** Your data source function doesn't support the new pagination parameters.

**Solution:** Ensure your query function in `core/queries.py` has been updated:

```python
@register_query("my_data_source")
def my_data_source(
    limit: int = None,
    offset: int = None,
    count_only: bool = False
) -> pd.DataFrame | int:
    sql = "SELECT * FROM my_table"

    if limit is not None and not count_only:
        sql += f"\nLIMIT {limit}"
    if offset is not None and not count_only:
        sql += f"\nOFFSET {offset}"

    return run_query(sql, count_only=count_only)
```

### Problem: Validation is slower than expected

**Possible causes:**

1. **Chunk size too small:** If chunk_size < 50k, overhead dominates
   - **Solution:** Increase `initial_chunk_size` to 75k-100k

2. **Too few workers:** If max_workers = 1-2, no parallelism benefit
   - **Solution:** Increase `max_workers` to 4-6

3. **Network latency:** Snowflake connection slow
   - **Solution:** Check warehouse size, consider larger chunks to reduce round trips

4. **Complex validations:** Expensive expectations (regex, custom logic)
   - **Solution:** This is expected - chunking helps but can't eliminate computational cost

### Problem: Out of memory errors

**Cause:** Chunk size too large for available memory.

**Solution:** Decrease `initial_chunk_size`:
```yaml
chunking:
  initial_chunk_size: 50000  # Reduced from 75000
```

### Problem: Results don't match non-chunked validation

**Cause:** This should NOT happen - results should be identical.

**Action:** This is a bug - please report with:
1. YAML configuration
2. Expected vs actual results
3. Console output logs

---

## Advanced Topics

### Custom Expectations with Chunking

Custom expectations work seamlessly with chunking. Each chunk is validated independently, and results are merged just like standard GX expectations.

**Example:** Conditional validation

```yaml
validations:
  - type: "custom:conditional_required"
    column: "BOM_STATUS"
    conditions:
      - column: "MATERIAL_TYPE"
        value: "FERT"
```

This runs on each chunk and results are merged automatically.

### Limitations

1. **Cross-row dependencies:** Validations that need to see ALL rows at once won't work with chunking
   - Example: "Find duplicate pairs across entire dataset"
   - Workaround: Use Snowflake-native validation (Option 3 from planning)

2. **Ordering requirements:** If validation order matters (row N depends on row N-1), chunking breaks this
   - This is rare in data quality validation
   - Workaround: Disable chunking for these specific suites

3. **Stateful validations:** If validation accumulates state across rows, chunking may give unexpected results
   - Example: "Running total should never decrease"
   - Workaround: Implement as Snowflake window function

### Performance Tuning Tips

1. **Start conservative:**
   - Begin with `initial_chunk_size: 50000` and `max_workers: 2`
   - Let the system tune up rather than starting too aggressive

2. **Match workers to CPU cores:**
   - If you have 4 CPU cores, use `max_workers: 4`
   - Going beyond available cores gives diminishing returns

3. **Monitor first few runs:**
   - Check console output for chunk timing
   - If chunks complete in < 10 seconds, increase chunk size
   - If chunks take > 60 seconds, decrease chunk size

4. **Balance chunk size and worker count:**
   - More workers + smaller chunks = more parallelism but more overhead
   - Fewer workers + larger chunks = less overhead but less parallelism
   - Sweet spot is usually: 50k-100k chunk size, 4-6 workers

---

## Examples

### Example 1: Large Dataset (1M+ rows)

```yaml
metadata:
  suite_name: "ProductCatalog_Full"
  data_source: "get_full_catalog"
  chunking:
    enabled: true
    target_time_seconds: 300      # 5 minutes
    initial_chunk_size: 100000    # 100k rows
    max_workers: 6                # 6 concurrent chunks

validations:
  - type: "expect_column_values_to_not_be_null"
    columns: ["PRODUCT_ID", "DESCRIPTION"]
```

**Expected performance:** 1M rows in ~5 minutes (3,300 rows/sec)

### Example 2: Medium Dataset (100k-500k rows)

```yaml
metadata:
  suite_name: "MaterialMaster_Regional"
  data_source: "get_regional_materials"
  chunking:
    enabled: true
    target_time_seconds: 120      # 2 minutes
    initial_chunk_size: 50000     # 50k rows
    max_workers: 4                # 4 concurrent chunks

validations:
  - type: "expect_column_values_to_be_in_set"
    rules:
      REGION: ["NA", "EU", "APAC"]
```

**Expected performance:** 500k rows in ~2 minutes (4,166 rows/sec)

### Example 3: Disable Chunking (Small Dataset)

```yaml
metadata:
  suite_name: "QuickTest"
  data_source: "get_test_sample"
  # No chunking section = standard validation

validations:
  - type: "expect_column_values_to_not_be_null"
    columns: ["ID"]
```

**Use case:** Small test suites, exploratory validation, < 10k rows

---

## FAQ

**Q: Does chunking change validation results?**
A: No. Results are identical to non-chunked validation. The system validates each chunk independently and merges results correctly.

**Q: Can I use chunking with existing YAML files?**
A: Yes! Just add the `chunking` section to metadata. All existing validations work unchanged.

**Q: What happens if I change chunk_size mid-project?**
A: The performance tuner adapts. It will use your new setting as a starting point and continue learning.

**Q: Can I disable auto-tuning?**
A: Not currently, but it's non-intrusive. You can always manually set `initial_chunk_size` before each run.

**Q: Does chunking work with custom expectations?**
A: Yes! Custom expectations are validated per chunk and results are merged automatically.

**Q: How do I know if chunking is helping?**
A: Compare console output:
- Without chunking: "Running {suite} on {N} rows..."
- With chunking: "Starting streaming validation... {N} chunks"
- Check total time before/after enabling chunking

---

## Next Steps

1. **Try the example:** Run `Aurora_Motors_Validation_Chunked_Example.yaml`
2. **Monitor performance:** Check `validation_results/performance/`
3. **Enable on large suites:** Add chunking to validations that take > 3 minutes
4. **Let it learn:** Run a few times to let auto-tuning optimize

For questions or issues, see the main project README or open an issue.
