# Roadmap: File Persistence → Database-Driven Architecture

## Current State (File-Based)

```
┌─────────────┐
│ YAML Files  │ ← Validation definitions
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│ SQL Generator       │ ← Generates query with JSON array column
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Snowflake Execution │ ← Returns: validation_results = [{"expectation_id": "...", ...}]
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Python Parsing      │ ← Parse JSON, compute derived statuses, build DataFrames
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Streamlit UI        │ ← Display results (ephemeral, no persistence)
└─────────────────────┘
```

**Problems:**
- ❌ No persistence - results lost after session ends
- ❌ Must re-run queries to see historical data
- ❌ Can't track changes over time
- ❌ Complex JSON parsing in Python
- ❌ Derived statuses computed client-side
- ❌ Long, non-parseable expectation IDs

---

## Target State (Database-Driven)

```
┌─────────────┐
│ YAML Files  │ ← Validation definitions (single source of truth)
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│ SQL Generator       │ ← Generates query with COLUMNAR format
│ (Updated)           │   Each expectation = separate column
└──────┬──────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│ Snowflake Execution                                          │
│ Returns: mat_num | org_level | exp_a3f | exp_b2e | derived  │
│          MAT1    | LEVEL1    | PASS    | FAIL    | FAIL     │
└──────┬──────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────┐
│ Results Table       │ ← INSERT INTO validation_results (persisted!)
│ (Snowflake)         │   Partitioned by: suite_name, run_timestamp
└──────┬──────────────┘
       │
       ▼
┌─────────────────────┐
│ Streamlit UI        │ ← Query historical data, filter by exp IDs
│ (Simplified)        │   No JSON parsing, direct SQL filtering
└─────────────────────┘
       │
       ▼
┌─────────────────────┐
│ Analytics/BI        │ ← Tableau, PowerBI can query directly
└─────────────────────┘
```

**Benefits:**
- ✅ Results persisted in database
- ✅ Historical tracking and trending
- ✅ Simple columnar format (exp_a3f = 'PASS/FAIL')
- ✅ Short, parseable expectation IDs
- ✅ Derived statuses computed in SQL
- ✅ Direct SQL filtering and aggregation
- ✅ BI tool integration

---

## Key Architectural Changes

### 1. Expectation ID Format

**Before:**
```
exp_expect_column_values_to_not_be_null_cols_cda4e89d_c5f707d4
```

**After:**
```
exp_a3f4b2  (6-8 hex chars, deterministic hash)
```

**Parsing:**
```python
# Load YAML, regenerate IDs to find match
metadata = lookup_from_yaml(exp_id='exp_a3f4b2', yaml_path='suite.yaml')
# Returns: {'type': 'expect_column_values_to_not_be_null', 'column': 'ORG_LEVEL'}
```

### 2. SQL Results Format

**Before (JSON Array):**
```sql
SELECT
  MATERIAL_NUMBER,
  ORG_LEVEL,
  ARRAY_CONSTRUCT(
    CASE WHEN org_level IS NULL THEN OBJECT_CONSTRUCT(
      'expectation_id', 'exp_long_id_here',
      'expectation_type', 'expect_column_values_to_not_be_null',
      'column', 'ORG_LEVEL',
      'failure_reason', 'NULL_VALUE'
    ) END
  ) AS validation_results
FROM source
```

**After (Columnar):**
```sql
SELECT
  MATERIAL_NUMBER,
  ORG_LEVEL,
  STATUS,
  CASE WHEN ORG_LEVEL IS NULL THEN 'FAIL' ELSE 'PASS' END AS exp_a3f,
  CASE WHEN STATUS NOT IN ('A','I') THEN 'FAIL' ELSE 'PASS' END AS exp_b2e,

  -- Derived status (computed in SQL!)
  CASE
    WHEN exp_a3f = 'FAIL' OR exp_b2e = 'FAIL'
    THEN 'FAIL'
    ELSE 'PASS'
  END AS derived_abp_incomplete

FROM source
```

### 3. Derived Status Computation

**Before (Python):**
```python
# Parse JSON, iterate rows, match IDs, deduplicate materials
for row in df.iterrows():
    entries = json.loads(row['validation_results'])
    for entry in entries:
        if entry['expectation_id'] in derived_exp_ids:
            # ... complex logic
```

**After (SQL):**
```sql
-- Simple OR condition in SELECT statement
CASE
  WHEN exp_a3f = 'FAIL' OR exp_b2e = 'FAIL' OR exp_c7d = 'FAIL'
  THEN 'FAIL'
  ELSE 'PASS'
END AS derived_status_name
```

### 4. Database Schema

**New Table: `validation_results`**
```sql
CREATE TABLE validation_results (
  run_id VARCHAR,              -- UUID for this run
  run_timestamp TIMESTAMP,     -- When validation ran
  suite_name VARCHAR,          -- "ABB SHOP DATA PRESENCE"
  material_number VARCHAR,     -- Primary business key

  -- Context columns (from grain mapping)
  org_level VARCHAR,
  product_hierarchy VARCHAR,

  -- Expectation columns (dynamic per suite)
  exp_a3f VARCHAR,  -- PASS/FAIL
  exp_b2e VARCHAR,  -- PASS/FAIL
  exp_c7d VARCHAR,  -- PASS/FAIL
  -- ... (N expectation columns)

  -- Derived status columns
  derived_abp_incomplete VARCHAR,  -- PASS/FAIL
  derived_z01_incomplete VARCHAR,  -- PASS/FAIL

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY (suite_name, DATE(run_timestamp));
```

**New Table: `expectation_metadata`** (optional, for quick lookups)
```sql
CREATE TABLE expectation_metadata (
  expectation_id VARCHAR PRIMARY KEY,  -- exp_a3f
  suite_name VARCHAR,
  expectation_type VARCHAR,
  column_name VARCHAR,
  yaml_path VARCHAR,
  created_at TIMESTAMP
);
```

---

## Migration Phases

### Phase 1: Update ID Generation (Foundation)
**Goal:** Shorter, stable expectation IDs

**Changes:**
- Update `validations/sql_generator.py:_annotate_expectation_ids()` to use 6-char hashes
- Update `build_scoped_expectation_id()` to use 4-char scopes
- Add `lookup_expectation_from_yaml()` helper function
- Update YAML files with new IDs (run migration script)

**Files Modified:**
- `validations/sql_generator.py`
- `core/yaml_helpers.py` (new)
- All YAML files in `validation_yaml/`

**Deliverables:**
- [ ] Shorter ID generation function
- [ ] YAML lookup/parsing function
- [ ] Migration script to update existing YAMLs
- [ ] Unit tests for ID stability

**Risks:**
- Breaking change - existing IDs in YAML will change
- Need to regenerate all YAMLs

---

### Phase 2: Columnar SQL Generation
**Goal:** Generate one column per expectation instead of JSON array

**Changes:**
- Update `ValidationSQLGenerator.generate_sql()` to output columnar format
- Each validation → separate `CASE` statement with `AS exp_{id}` alias
- Remove `validation_results` array construction
- Add derived status columns to SQL

**Example:**
```python
def _build_columnar_select(self) -> str:
    columns = []

    # Context columns
    columns.extend(self.context_columns)

    # Expectation columns
    for validation in self.validations:
        exp_id = validation['expectation_id']
        case_expr = self._build_case_expression(validation)
        columns.append(f"{case_expr} AS {exp_id}")

    # Derived status columns
    for derived in self.derived_statuses:
        derived_expr = self._build_derived_expression(derived)
        columns.append(f"{derived_expr} AS {derived['status'].lower().replace(' ', '_')}")

    return ",\n  ".join(columns)
```

**Files Modified:**
- `validations/sql_generator.py` (major refactor)
- `validations/snowflake_runner.py` (simplified parsing)

**Deliverables:**
- [ ] Columnar SQL generator
- [ ] Derived status SQL expression builder
- [ ] Updated result parser (no JSON parsing)
- [ ] Comparison tests (old vs new format)

**Risks:**
- Large refactor of SQL generation logic
- Need to ensure SQL correctness

---

### Phase 3: Database Persistence Layer
**Goal:** Write results to Snowflake table instead of returning ephemeral DataFrames

**Changes:**
- Create `validation_results` table schema
- Add `persist_validation_results()` function
- Add `run_id` and `run_timestamp` to results
- Implement table partitioning strategy

**New Module:** `core/persistence.py`
```python
def persist_validation_results(
    df: pd.DataFrame,
    suite_name: str,
    run_id: str = None
) -> str:
    """
    Write validation results to Snowflake table.

    Returns:
        run_id - Unique identifier for this validation run
    """
    run_id = run_id or str(uuid.uuid4())
    run_timestamp = datetime.now()

    # Add metadata columns
    df['run_id'] = run_id
    df['run_timestamp'] = run_timestamp
    df['suite_name'] = suite_name

    # Write to Snowflake
    write_pandas(
        conn=get_snowflake_connection(),
        df=df,
        table_name='validation_results',
        database='PROD_MO_MONM',
        schema='REPORTING',
        auto_create_table=False  # Pre-defined schema
    )

    return run_id
```

**Files Created:**
- `core/persistence.py` (new)
- `core/table_schemas.sql` (DDL definitions)

**Files Modified:**
- `validations/snowflake_runner.py` (add persistence call)
- `app/pages/Validation_Report.py` (query from table instead of running live)

**Deliverables:**
- [ ] Database table DDL
- [ ] Persistence module
- [ ] Historical query functions
- [ ] Table partitioning implementation
- [ ] Data retention policy

**Risks:**
- Table schema needs to be dynamic (different suites have different expectations)
- May need VARIANT column for flexible schema

---

### Phase 4: UI Updates for Historical Data
**Goal:** Query historical validation runs from database

**Changes:**
- Add run history selector in UI
- Query `validation_results` table instead of running SQL
- Add trending/comparison features
- Filter by exp_id columns directly

**New UI Features:**
```python
# Streamlit UI
selected_run = st.selectbox(
    "Select validation run",
    options=get_validation_runs(suite_name='ABB SHOP DATA PRESENCE')
)

# Query historical results
results_df = query_validation_results(
    run_id=selected_run['run_id']
)

# Filter by specific expectation
failed_exp_a3f = results_df[results_df['exp_a3f'] == 'FAIL']
```

**Files Modified:**
- `app/pages/Validation_Report.py` (major updates)
- `app/components/drill_down.py` (query by exp_id)
- `core/queries.py` (add historical query functions)

**Deliverables:**
- [ ] Run history UI component
- [ ] Historical query functions
- [ ] Trend charts (pass/fail over time)
- [ ] Run comparison view
- [ ] Export historical data

**Risks:**
- UI complexity increases
- Need to handle large result sets efficiently

---

### Phase 5: Metadata Management
**Goal:** Optional - populate `expectation_metadata` table for quick lookups

**Changes:**
- Parse YAML files to extract expectation metadata
- Insert into `expectation_metadata` table
- Add API for looking up exp_id → {type, column}
- Auto-sync when YAML changes

**Implementation:**
```python
def sync_expectation_metadata(yaml_path: str):
    """Parse YAML and sync metadata to database."""
    suite_config = yaml.safe_load(open(yaml_path))
    suite_name = suite_config['metadata']['suite_name']

    metadata_records = []
    for validation in suite_config['validations']:
        for column in extract_targets(validation):
            exp_id = build_scoped_expectation_id(validation, column)
            metadata_records.append({
                'expectation_id': exp_id,
                'suite_name': suite_name,
                'expectation_type': validation['type'],
                'column_name': column,
                'yaml_path': str(yaml_path)
            })

    # Upsert to database
    upsert_expectation_metadata(metadata_records)
```

**Files Created:**
- `scripts/sync_metadata.py` (CLI tool)

**Files Modified:**
- `core/persistence.py` (add metadata functions)

**Deliverables:**
- [ ] Metadata sync script
- [ ] Metadata query API
- [ ] Automated sync on YAML changes
- [ ] Metadata table DDL

**Risks:**
- Adds complexity
- May not be necessary if YAML parsing is fast enough

---

## Technical Decisions

### Decision 1: Dynamic Schema vs Fixed Schema

**Option A: Dynamic Schema (VARIANT column)**
```sql
CREATE TABLE validation_results (
  run_id VARCHAR,
  suite_name VARCHAR,
  material_number VARCHAR,
  expectation_results VARIANT,  -- {"exp_a3f": "PASS", "exp_b2e": "FAIL", ...}
  derived_results VARIANT       -- {"derived_abp": "FAIL", ...}
);
```

**Pros:** Flexible, no schema changes needed
**Cons:** Less type-safe, harder to query specific expectations

**Option B: Fixed Wide Schema**
```sql
CREATE TABLE validation_results (
  run_id VARCHAR,
  suite_name VARCHAR,
  material_number VARCHAR,
  exp_001 VARCHAR,
  exp_002 VARCHAR,
  -- ... (max N columns)
  exp_999 VARCHAR
);
```

**Pros:** Type-safe, easy SQL filtering
**Cons:** Wastes space, requires max column limit

**Option C: Hybrid (Recommended)**
```sql
CREATE TABLE validation_results (
  run_id VARCHAR,
  suite_name VARCHAR,
  material_number VARCHAR,

  -- Common context columns
  org_level VARCHAR,
  product_hierarchy VARCHAR,

  -- All expectations as VARIANT (for flexibility)
  expectation_results VARIANT,  -- {"exp_a3f": "PASS", ...}

  -- Common derived statuses as typed columns
  derived_abp_incomplete VARCHAR,
  derived_z01_incomplete VARCHAR
);

-- Query: Still easy to filter
WHERE expectation_results:exp_a3f = 'FAIL'
```

**Decision:** Start with Option C (Hybrid)

---

### Decision 2: Real-time vs Batch Persistence

**Option A: Real-time (Write immediately after validation)**
```python
results = run_validation_from_yaml_snowflake(yaml_path)
persist_validation_results(results['full_results_df'], suite_name)
```

**Option B: Batch (Scheduled runs that write to DB)**
```python
# Airflow/cron job
for yaml_file in validation_yamls:
    results = run_validation_from_yaml_snowflake(yaml_file)
    persist_validation_results(results['full_results_df'], suite_name)
```

**Decision:** Support both - real-time for UI, batch for scheduled monitoring

---

### Decision 3: Expectation ID Stability

**Question:** What happens when YAML changes (add/remove validations)?

**Solution:** Use content-based hashing (current approach)
- Adding validation → new exp_id generated
- Removing validation → exp_id disappears from future runs
- Changing validation order → IDs stay stable (hash includes type+column, not index)

**Update hash to exclude index:**
```python
# Old (unstable)
raw_id = f"{suite_name}|{idx}|{validation['type']}|{column}"

# New (stable)
raw_id = f"{suite_name}|{validation['type']}|{column}"
```

This ensures same expectation always gets same ID, regardless of position in YAML.

---

## Success Metrics

- [ ] Query performance: Historical queries < 2 seconds
- [ ] Storage efficiency: < 10 GB per 1M validation runs
- [ ] UI responsiveness: Load historical run < 1 second
- [ ] Data integrity: 100% match between live and persisted results
- [ ] Backwards compatibility: Old JSON format still supported for 6 months

---

## Rollout Plan

### Week 1-2: Phase 1 (ID Updates)
- Implement shorter IDs
- Build YAML lookup functions
- Migrate existing YAMLs
- Testing and validation

### Week 3-4: Phase 2 (Columnar SQL)
- Refactor SQL generator
- Update result parser
- Parallel run old vs new format
- Validate correctness

### Week 5-6: Phase 3 (Database Persistence)
- Create table schemas
- Implement persistence layer
- Test write performance
- Set up partitioning

### Week 7-8: Phase 4 (UI Updates)
- Add historical query UI
- Build trend charts
- Run comparison features
- User acceptance testing

### Week 9-10: Phase 5 (Metadata + Polish)
- Metadata sync (if needed)
- Performance optimization
- Documentation
- Training

---

## Open Questions

1. **Data Retention:** How long to keep validation results?
   - Proposed: 90 days detail, 1 year aggregated

2. **Concurrent Runs:** Can same suite run multiple times simultaneously?
   - Proposed: Use `run_id` to distinguish

3. **Schema Evolution:** How to handle YAML changes over time?
   - Proposed: Version YAMLs, store yaml_version in results table

4. **Access Control:** Who can query historical data?
   - Proposed: Same permissions as current Snowflake access

5. **Cost:** Snowflake storage costs for historical data?
   - Proposed: Monitor and optimize partitioning/clustering

---

## Next Steps

1. **Review this roadmap** with stakeholders
2. **Prioritize phases** based on business value
3. **Spike Phase 1** to validate approach
4. **Create detailed tickets** for Phase 1 tasks
5. **Begin implementation**
