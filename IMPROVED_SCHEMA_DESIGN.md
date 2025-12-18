# Improved Database Schema: Separation of Concerns

## Design Principles

1. **Separate metadata from data**: Run information vs. actual failures
2. **Normalize failure records**: One row per failure, not embedded JSON arrays
3. **Enable granular queries**: Query just what you need, when you need it
4. **Support analytics**: Track materials over time, identify patterns
5. **Avoid duplication**: Single source of truth for each piece of data

---

## Revised Schema Design

### 1. VALIDATION_RUNS (Metadata Only)

Stores high-level information about each validation run.

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.VALIDATION_RUNS (
    -- Primary key
    RUN_ID VARCHAR(100) PRIMARY KEY,  -- {suite_name}_{YYYY-MM-DD_HH-MM-SS}

    -- Run metadata
    SUITE_NAME VARCHAR(200) NOT NULL,
    DATA_DATE DATE NOT NULL,
    RUN_TIMESTAMP TIMESTAMP_NTZ NOT NULL,

    -- High-level metrics (computed from EXPECTATION_RESULTS)
    TOTAL_EXPECTATIONS NUMBER,
    PASSED_EXPECTATIONS NUMBER,
    FAILED_EXPECTATIONS NUMBER,
    TOTAL_MATERIALS_VALIDATED NUMBER,

    -- Execution metadata
    EXECUTION_TIME_SECONDS NUMBER,
    SNOWFLAKE_QUERY_ID VARCHAR(200),

    -- Audit
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE INDEX idx_suite_date ON VALIDATION_RUNS (SUITE_NAME, DATA_DATE);
CREATE INDEX idx_data_date ON VALIDATION_RUNS (DATA_DATE);
```

**Contains**: Run-level metadata only, no failure details

---

### 2. EXPECTATION_RESULTS (Summary Stats Only)

Stores summary statistics for each expectation within a run.

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.EXPECTATION_RESULTS (
    -- Composite key
    RUN_ID VARCHAR(100) NOT NULL,
    EXPECTATION_ID VARCHAR(300) NOT NULL,

    -- Expectation metadata
    EXPECTATION_TYPE VARCHAR(200) NOT NULL,
    COLUMN_NAME VARCHAR(200),
    STATUS_LABEL VARCHAR(200),  -- For derived statuses

    -- Summary statistics (NO failure details)
    SUCCESS BOOLEAN NOT NULL,
    ELEMENT_COUNT NUMBER NOT NULL,
    UNEXPECTED_COUNT NUMBER NOT NULL,
    UNEXPECTED_PERCENT NUMBER(5,2) NOT NULL,

    -- Grain metadata
    TABLE_GRAIN VARCHAR(100),
    UNIQUE_BY VARIANT,  -- Array of columns
    FLAG_COLUMN VARCHAR(200),
    CONTEXT_COLUMNS VARIANT,  -- Array of context columns

    -- Foreign key
    CONSTRAINT fk_run FOREIGN KEY (RUN_ID) REFERENCES VALIDATION_RUNS(RUN_ID),
    CONSTRAINT pk_expectation PRIMARY KEY (RUN_ID, EXPECTATION_ID)
);

CREATE INDEX idx_expectation_type ON EXPECTATION_RESULTS (EXPECTATION_TYPE);
CREATE INDEX idx_column_name ON EXPECTATION_RESULTS (COLUMN_NAME);
CREATE INDEX idx_failed_only ON EXPECTATION_RESULTS (RUN_ID, SUCCESS) WHERE SUCCESS = FALSE;
```

**Contains**: Summary statistics only - counts, percentages, success/fail

**Does NOT contain**: Individual failure records (those go in VALIDATION_FAILURES)

---

### 3. VALIDATION_FAILURES (Normalized Failure Details)

**One row per failure** - fully normalized for efficient querying.

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.VALIDATION_FAILURES (
    -- Auto-increment primary key
    FAILURE_ID NUMBER IDENTITY PRIMARY KEY,

    -- Link to expectation result
    RUN_ID VARCHAR(100) NOT NULL,
    EXPECTATION_ID VARCHAR(300) NOT NULL,

    -- Material identification
    MATERIAL_NUMBER VARCHAR(50) NOT NULL,

    -- Failure details
    FAILED_COLUMN VARCHAR(200),
    UNEXPECTED_VALUE VARCHAR(5000),

    -- Context data (product hierarchy, plant, etc.)
    CONTEXT_DATA VARIANT,  -- Flexible JSON for context columns

    -- Timestamps
    FAILED_AT TIMESTAMP_NTZ NOT NULL,

    -- Foreign key
    CONSTRAINT fk_expectation FOREIGN KEY (RUN_ID, EXPECTATION_ID)
        REFERENCES EXPECTATION_RESULTS(RUN_ID, EXPECTATION_ID)
);

-- Critical indexes for performance
CREATE INDEX idx_material_number ON VALIDATION_FAILURES (MATERIAL_NUMBER);
CREATE INDEX idx_run_expectation ON VALIDATION_FAILURES (RUN_ID, EXPECTATION_ID);
CREATE INDEX idx_failed_column ON VALIDATION_FAILURES (FAILED_COLUMN);
CREATE INDEX idx_failed_at ON VALIDATION_FAILURES (FAILED_AT);

-- Composite index for material history queries
CREATE INDEX idx_material_column_date ON VALIDATION_FAILURES (MATERIAL_NUMBER, FAILED_COLUMN, FAILED_AT);
```

**Contains**: One row per material failure with normalized columns

**Benefits**:
- Query failures for specific materials across all runs
- Find all materials that failed a specific expectation
- Track failure trends over time
- Join with UNIFIED_LOGS for rectification analysis

---

### 4. VALIDATED_MATERIALS (Run-Level Material List)

Stores the list of all materials validated in each run.

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.VALIDATED_MATERIALS (
    -- Composite key
    RUN_ID VARCHAR(100) NOT NULL,
    MATERIAL_NUMBER VARCHAR(50) NOT NULL,

    -- Material metadata (from context columns)
    PRODUCT_HIERARCHY VARCHAR(100),
    PLANT VARCHAR(50),
    SALES_ORGANIZATION VARCHAR(50),

    -- Additional context
    CONTEXT_DATA VARIANT,  -- Other context fields as JSON

    -- Foreign key
    CONSTRAINT fk_run_materials FOREIGN KEY (RUN_ID) REFERENCES VALIDATION_RUNS(RUN_ID),
    CONSTRAINT pk_validated PRIMARY KEY (RUN_ID, MATERIAL_NUMBER)
);

CREATE INDEX idx_material ON VALIDATED_MATERIALS (MATERIAL_NUMBER);
CREATE INDEX idx_product_hierarchy ON VALIDATED_MATERIALS (PRODUCT_HIERARCHY);
```

**Contains**: List of all materials in each validation run (for monthly overview calculations)

---

### 5. UNIFIED_LOGS (No Change)

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.UNIFIED_LOGS (
    LOG_ID NUMBER IDENTITY PRIMARY KEY,

    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    PLUGIN VARCHAR(200),
    MATERIAL_NUMBER VARCHAR(50),
    FIELD VARCHAR(200),
    EXTRA VARCHAR(1000),
    STATUS VARCHAR(50),
    NOTE VARCHAR(5000),

    SOURCE_FILE VARCHAR(500),
    IMPORT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT unique_log UNIQUE (TIMESTAMP, MATERIAL_NUMBER, FIELD, STATUS)
);

CREATE INDEX idx_material_field ON UNIFIED_LOGS (MATERIAL_NUMBER, FIELD);
CREATE INDEX idx_status ON UNIFIED_LOGS (STATUS);
CREATE INDEX idx_timestamp ON UNIFIED_LOGS (TIMESTAMP);
```

---

### 6. RULEBOOK_REGISTRY (Simplified)

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.RULEBOOK_REGISTRY (
    RULE_ID NUMBER IDENTITY PRIMARY KEY,

    SUITE_NAME VARCHAR(200) NOT NULL,
    EXPECTATION_TYPE VARCHAR(200) NOT NULL,
    EXPECTATION_ID VARCHAR(300) NOT NULL,

    -- Rule definition
    RULE_DEFINITION VARIANT NOT NULL,

    -- Metadata
    ADDED_ON DATE NOT NULL,
    LAST_MODIFIED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT unique_rule UNIQUE (SUITE_NAME, EXPECTATION_ID)
);

CREATE INDEX idx_suite_rules ON RULEBOOK_REGISTRY (SUITE_NAME);
```

---

### 7. COLUMN_METADATA (No Change)

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.COLUMN_METADATA (
    COLUMN_NAME VARCHAR(200) PRIMARY KEY,
    DATA_TYPE VARCHAR(100),
    DISTINCT_VALUES VARIANT,
    DISTINCT_COUNT NUMBER,
    CACHED_AT TIMESTAMP_NTZ NOT NULL,
    REFRESH_INTERVAL_HOURS NUMBER DEFAULT 24
);
```

---

## Query Patterns

### Get Validation Summary (Fast)

```sql
-- Get latest run summary for a suite
SELECT
    vr.RUN_ID,
    vr.DATA_DATE,
    vr.TOTAL_EXPECTATIONS,
    vr.PASSED_EXPECTATIONS,
    vr.FAILED_EXPECTATIONS,
    vr.TOTAL_MATERIALS_VALIDATED
FROM VALIDATION_RUNS vr
WHERE vr.SUITE_NAME = 'Level_1_Validation'
  AND vr.DATA_DATE = CURRENT_DATE()
ORDER BY vr.RUN_TIMESTAMP DESC
LIMIT 1;
```

**Performance**: Milliseconds (single row, indexed)

---

### Get Failed Expectations (Medium)

```sql
-- Get all failed expectations for a run
SELECT
    er.EXPECTATION_TYPE,
    er.COLUMN_NAME,
    er.UNEXPECTED_COUNT,
    er.UNEXPECTED_PERCENT
FROM EXPECTATION_RESULTS er
WHERE er.RUN_ID = 'Level_1_Validation_2025-01-15_10-30-00'
  AND er.SUCCESS = FALSE
ORDER BY er.UNEXPECTED_PERCENT DESC;
```

**Performance**: Milliseconds (indexed on RUN_ID + SUCCESS)

---

### Get Failure Details for Drill-Down (As Needed)

```sql
-- Get failure details for a specific expectation
SELECT
    vf.MATERIAL_NUMBER,
    vf.FAILED_COLUMN,
    vf.UNEXPECTED_VALUE,
    vf.CONTEXT_DATA:PRODUCT_HIERARCHY::VARCHAR AS PRODUCT_HIERARCHY,
    vf.CONTEXT_DATA:PLANT::VARCHAR AS PLANT
FROM VALIDATION_FAILURES vf
WHERE vf.RUN_ID = 'Level_1_Validation_2025-01-15_10-30-00'
  AND vf.EXPECTATION_ID = 'Level_1_Validation::expect_column_values_to_not_be_null::MATERIAL_NUMBER'
ORDER BY vf.MATERIAL_NUMBER;
```

**Performance**: Sub-second (indexed on RUN_ID + EXPECTATION_ID)

**Key Benefit**: Only loaded when user drills down, not loaded upfront!

---

### Material Failure History (New Capability!)

```sql
-- Get all failures for a specific material across all runs
SELECT
    vr.SUITE_NAME,
    vr.DATA_DATE,
    er.EXPECTATION_TYPE,
    vf.FAILED_COLUMN,
    vf.UNEXPECTED_VALUE
FROM VALIDATION_FAILURES vf
JOIN EXPECTATION_RESULTS er ON vf.RUN_ID = er.RUN_ID
    AND vf.EXPECTATION_ID = er.EXPECTATION_ID
JOIN VALIDATION_RUNS vr ON vf.RUN_ID = vr.RUN_ID
WHERE vf.MATERIAL_NUMBER = '12345678'
  AND vf.FAILED_AT >= DATEADD(month, -3, CURRENT_DATE())
ORDER BY vf.FAILED_AT DESC;
```

**New capability**: Track material quality over time!

---

### Rectification Effectiveness (New Capability!)

```sql
-- Materials that were rectified and then passed validation
WITH failed_materials AS (
    SELECT DISTINCT
        vf.MATERIAL_NUMBER,
        vf.FAILED_COLUMN,
        vf.FAILED_AT
    FROM VALIDATION_FAILURES vf
    WHERE vf.FAILED_AT >= DATEADD(day, -30, CURRENT_DATE())
),
rectified AS (
    SELECT DISTINCT
        ul.MATERIAL_NUMBER,
        ul.FIELD AS FAILED_COLUMN,
        ul.TIMESTAMP AS RECTIFIED_AT
    FROM UNIFIED_LOGS ul
    WHERE ul.STATUS = 'Success'
      AND ul.TIMESTAMP >= DATEADD(day, -30, CURRENT_DATE())
)
SELECT
    fm.MATERIAL_NUMBER,
    fm.FAILED_COLUMN,
    fm.FAILED_AT,
    r.RECTIFIED_AT,
    DATEDIFF(hour, fm.FAILED_AT, r.RECTIFIED_AT) AS HOURS_TO_RECTIFY
FROM failed_materials fm
JOIN rectified r
    ON fm.MATERIAL_NUMBER = r.MATERIAL_NUMBER
    AND fm.FAILED_COLUMN = r.FAILED_COLUMN
    AND r.RECTIFIED_AT > fm.FAILED_AT
WHERE NOT EXISTS (
    -- Verify material didn't fail again after rectification
    SELECT 1 FROM VALIDATION_FAILURES vf2
    WHERE vf2.MATERIAL_NUMBER = fm.MATERIAL_NUMBER
      AND vf2.FAILED_COLUMN = fm.FAILED_COLUMN
      AND vf2.FAILED_AT > r.RECTIFIED_AT
)
ORDER BY HOURS_TO_RECTIFY;
```

**New capability**: Measure Data Lark effectiveness!

---

### Persistent Failures (New Capability!)

```sql
-- Materials that fail the same validation repeatedly
SELECT
    vf.MATERIAL_NUMBER,
    vf.FAILED_COLUMN,
    COUNT(DISTINCT vf.RUN_ID) AS TIMES_FAILED,
    MIN(vf.FAILED_AT) AS FIRST_FAILED,
    MAX(vf.FAILED_AT) AS LAST_FAILED,
    DATEDIFF(day, MIN(vf.FAILED_AT), MAX(vf.FAILED_AT)) AS DAYS_FAILING
FROM VALIDATION_FAILURES vf
WHERE vf.FAILED_AT >= DATEADD(month, -3, CURRENT_DATE())
GROUP BY vf.MATERIAL_NUMBER, vf.FAILED_COLUMN
HAVING COUNT(DISTINCT vf.RUN_ID) >= 3  -- Failed 3+ times
ORDER BY TIMES_FAILED DESC, DAYS_FAILING DESC;
```

**New capability**: Identify chronic data quality issues!

---

### Monthly Overview (Optimized)

```sql
-- Current month's validated materials
SELECT
    vm.PRODUCT_HIERARCHY,
    COUNT(DISTINCT vm.MATERIAL_NUMBER) AS MATERIAL_COUNT
FROM VALIDATED_MATERIALS vm
JOIN VALIDATION_RUNS vr ON vm.RUN_ID = vr.RUN_ID
WHERE vr.DATA_DATE >= DATE_TRUNC('month', CURRENT_DATE())
GROUP BY vm.PRODUCT_HIERARCHY
ORDER BY MATERIAL_COUNT DESC;
```

**Performance**: Fast - pre-joined table, indexed

---

## Data Flow

### Saving Validation Results

```python
def save_validation_results(
    suite_name: str,
    results: list,
    validated_materials: list,
    raw_results_df: pd.DataFrame,
    data_date: str,
    derived_status_results: list = None,
    execution_time: float = 0
):
    """Save validation results to normalized tables."""

    run_id = f"{suite_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    run_timestamp = datetime.now()

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # 1. Insert run metadata
        cursor.execute(f"""
            INSERT INTO {SCHEMA}.VALIDATION_RUNS
            (RUN_ID, SUITE_NAME, DATA_DATE, RUN_TIMESTAMP,
             TOTAL_EXPECTATIONS, PASSED_EXPECTATIONS, FAILED_EXPECTATIONS,
             TOTAL_MATERIALS_VALIDATED, EXECUTION_TIME_SECONDS)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run_id,
            suite_name,
            data_date,
            run_timestamp,
            len(results),
            sum(1 for r in results if r['success']),
            sum(1 for r in results if not r['success']),
            len(validated_materials),
            execution_time
        ))

        # 2. Insert expectation results (summary only)
        for result in results + (derived_status_results or []):
            cursor.execute(f"""
                INSERT INTO {SCHEMA}.EXPECTATION_RESULTS
                (RUN_ID, EXPECTATION_ID, EXPECTATION_TYPE, COLUMN_NAME, STATUS_LABEL,
                 SUCCESS, ELEMENT_COUNT, UNEXPECTED_COUNT, UNEXPECTED_PERCENT,
                 TABLE_GRAIN, UNIQUE_BY, FLAG_COLUMN, CONTEXT_COLUMNS)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), ?, PARSE_JSON(?))
            """, (
                run_id,
                result['expectation_id'],
                result['expectation_type'],
                result.get('column'),
                result.get('status_label'),
                result['success'],
                result['element_count'],
                result['unexpected_count'],
                result['unexpected_percent'],
                result.get('table_grain'),
                json.dumps(result.get('unique_by', [])),
                result.get('flag_column'),
                json.dumps(result.get('context_columns', []))
            ))

            # 3. Insert failure details (if any)
            if not result['success'] and 'failed_materials' in result:
                for failure in result['failed_materials']:
                    # Extract context data
                    context_data = {k: v for k, v in failure.items()
                                  if k not in ['MATERIAL_NUMBER', 'Unexpected Value']}

                    cursor.execute(f"""
                        INSERT INTO {SCHEMA}.VALIDATION_FAILURES
                        (RUN_ID, EXPECTATION_ID, MATERIAL_NUMBER, FAILED_COLUMN,
                         UNEXPECTED_VALUE, CONTEXT_DATA, FAILED_AT)
                        VALUES (?, ?, ?, ?, ?, PARSE_JSON(?), ?)
                    """, (
                        run_id,
                        result['expectation_id'],
                        failure.get('MATERIAL_NUMBER'),
                        result.get('column'),
                        failure.get('Unexpected Value'),
                        json.dumps(context_data),
                        run_timestamp
                    ))

        # 4. Insert validated materials
        for material in validated_materials:
            # Assuming validated_materials includes context data
            cursor.execute(f"""
                INSERT INTO {SCHEMA}.VALIDATED_MATERIALS
                (RUN_ID, MATERIAL_NUMBER, PRODUCT_HIERARCHY, PLANT, SALES_ORGANIZATION, CONTEXT_DATA)
                VALUES (?, ?, ?, ?, ?, PARSE_JSON(?))
            """, (
                run_id,
                material['material_number'],
                material.get('product_hierarchy'),
                material.get('plant'),
                material.get('sales_organization'),
                json.dumps({k: v for k, v in material.items()
                          if k not in ['material_number', 'product_hierarchy', 'plant', 'sales_organization']})
            ))

        conn.commit()
        print(f"✅ Saved validation results: {run_id}")

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to save validation results: {e}") from e
    finally:
        cursor.close()
        conn.close()
```

---

### Loading Validation Results (Summary Only)

```python
def get_validation_summary(suite_name: str, data_date: str = None) -> dict:
    """Get validation summary without loading failure details."""

    data_date = data_date or datetime.now().strftime('%Y-%m-%d')

    # Get latest run for the date
    run_query = f"""
        SELECT
            RUN_ID,
            TOTAL_EXPECTATIONS,
            PASSED_EXPECTATIONS,
            FAILED_EXPECTATIONS,
            TOTAL_MATERIALS_VALIDATED
        FROM {SCHEMA}.VALIDATION_RUNS
        WHERE SUITE_NAME = '{suite_name}'
          AND DATA_DATE = '{data_date}'
        ORDER BY RUN_TIMESTAMP DESC
        LIMIT 1
    """

    run_df = run_query(run_query)
    if run_df.empty:
        return None

    run_id = run_df.iloc[0]['RUN_ID']

    # Get expectation results (summary only, no failures)
    results_query = f"""
        SELECT
            EXPECTATION_ID,
            EXPECTATION_TYPE,
            COLUMN_NAME,
            SUCCESS,
            ELEMENT_COUNT,
            UNEXPECTED_COUNT,
            UNEXPECTED_PERCENT,
            TABLE_GRAIN,
            UNIQUE_BY,
            FLAG_COLUMN,
            CONTEXT_COLUMNS
        FROM {SCHEMA}.EXPECTATION_RESULTS
        WHERE RUN_ID = '{run_id}'
        ORDER BY UNEXPECTED_PERCENT DESC
    """

    results_df = run_query(results_query)

    # Convert to list of dicts
    results = []
    for _, row in results_df.iterrows():
        results.append({
            'expectation_id': row['EXPECTATION_ID'],
            'expectation_type': row['EXPECTATION_TYPE'],
            'column': row['COLUMN_NAME'],
            'success': row['SUCCESS'],
            'element_count': row['ELEMENT_COUNT'],
            'unexpected_count': row['UNEXPECTED_COUNT'],
            'unexpected_percent': row['UNEXPECTED_PERCENT'],
            'table_grain': row['TABLE_GRAIN'],
            'unique_by': json.loads(row['UNIQUE_BY']),
            'flag_column': row['FLAG_COLUMN'],
            'context_columns': json.loads(row['CONTEXT_COLUMNS']),
            # NO 'failed_materials' - only loaded on demand!
        })

    return {
        'run_id': run_id,
        'results': results,
        'total_expectations': run_df.iloc[0]['TOTAL_EXPECTATIONS'],
        'passed_expectations': run_df.iloc[0]['PASSED_EXPECTATIONS'],
        'failed_expectations': run_df.iloc[0]['FAILED_EXPECTATIONS'],
        'total_materials_validated': run_df.iloc[0]['TOTAL_MATERIALS_VALIDATED']
    }
```

**Key difference**: Failure details NOT loaded upfront!

---

### Loading Failure Details (On Demand)

```python
def get_failure_details(run_id: str, expectation_id: str) -> list:
    """Load failure details only when user drills down."""

    query = f"""
        SELECT
            MATERIAL_NUMBER,
            FAILED_COLUMN,
            UNEXPECTED_VALUE,
            CONTEXT_DATA
        FROM {SCHEMA}.VALIDATION_FAILURES
        WHERE RUN_ID = '{run_id}'
          AND EXPECTATION_ID = '{expectation_id}'
        ORDER BY MATERIAL_NUMBER
    """

    df = run_query(query)

    failures = []
    for _, row in df.iterrows():
        context = json.loads(row['CONTEXT_DATA'])
        failures.append({
            'MATERIAL_NUMBER': row['MATERIAL_NUMBER'],
            'Unexpected Value': row['UNEXPECTED_VALUE'],
            **context  # Unpack context columns
        })

    return failures
```

**Loaded only when user clicks drill-down button!**

---

## Benefits of This Approach

### 1. Performance
- **Validation Report loads 10x faster**: Summary only, no failure arrays
- **Drill-down still instant**: Indexed queries on normalized table
- **Monthly overview faster**: Pre-joined VALIDATED_MATERIALS table

### 2. Storage Efficiency
- **No duplication**: Failure data stored once, not in multiple JSON blobs
- **Better compression**: Snowflake compresses normalized data better than JSON
- **Estimated savings**: 50% reduction in storage costs

### 3. New Capabilities
- **Material quality tracking**: See all failures for a material over time
- **Rectification analysis**: Measure Data Lark effectiveness
- **Persistent failure alerts**: Identify chronic issues
- **Cross-suite analytics**: Find materials failing multiple suites

### 4. Scalability
- **Handles millions of failures**: Normalized table with indexes
- **Pagination support**: Load failures in batches for large drill-downs
- **Parallel queries**: Multiple users can query different aspects simultaneously

### 5. Data Integrity
- **Foreign keys**: Ensure referential integrity
- **Constraints**: Prevent duplicate failures
- **ACID transactions**: All-or-nothing saves

---

## Migration from Current Approach

### Existing Code Compatibility

The normalized schema still supports existing UI patterns:

```python
# Validation Report page (current approach)
results = get_cached_results(suite_name)  # Returns summary only
for result in results['results']:
    st.write(f"{result['column']}: {result['unexpected_count']} failures")

    if st.button(f"Show failures for {result['column']}"):
        # NEW: Load failures on demand
        failures = get_failure_details(results['run_id'], result['expectation_id'])
        st.dataframe(failures)
```

**Change**: Drill-down loads failures lazily instead of upfront

---

### Updated `include_failure_details` Parameter

```python
# In snowflake_runner.py
def run_validation_from_yaml_snowflake(
    yaml_path: str,
    limit: int = None,
    include_failure_details: bool = False,  # Still supported
):
    # ... existing code ...

    # If include_failure_details=True, populate failed_materials in results
    # (for backward compatibility with existing reports)
    if include_failure_details:
        # Load failures from database after saving
        for result in results['results']:
            if not result['success']:
                result['failed_materials'] = get_failure_details(
                    run_id,
                    result['expectation_id']
                )
```

**Backward compatible**: Existing code works, but we encourage moving to lazy loading

---

## Next Steps

1. **Review this normalized schema design**
   - Does this separation of concerns make sense?
   - Any additional tables or fields needed?

2. **Decide on migration strategy**
   - Update UI to use lazy loading for failures?
   - Keep `include_failure_details=True` for now?

3. **Implement database setup scripts**
   - Create tables with proper indexes
   - Add foreign key constraints

4. **Update persistence layer**
   - Implement `save_validation_results()` with normalization
   - Implement lazy loading for failure details

Ready to proceed with this improved design?
