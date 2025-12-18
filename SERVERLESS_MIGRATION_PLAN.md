# Serverless Migration Plan: Eliminating File Persistence

## Executive Summary

This plan eliminates all file system dependencies to enable deployment in Serverless in Scribe (SiS) while maintaining full functionality of validation reports and monthly overviews.

**Current State:** Heavy reliance on local files for validation results, unified logs, caches, and historical archives.

**Target State:** All persistent data stored in Snowflake tables, with Streamlit session-state caching for performance.

---

## Architecture Overview

### Current File Dependencies (8 categories)

1. **Validation Results** → `/validation_results/<suite>/<suite>_YYYY-MM-DD.json`
2. **Daily Caches** → `/validation_results/cache/<suite>_cache.json`
3. **Unified Logs** → `/Logs/Unified_Logs/Unified_Logs_*.csv`
4. **Monthly Overview Cache** → `/validation_results/cache/monthly_overview_cache.json`
5. **Column Metadata Cache** → `/validation_results/column_metadata_cache.json`
6. **Failures CSV Cache** → `/validation_results/cache/<suite>_failures_YYYY-MM-DD.csv`
7. **Rulebook Registry** → `/rulebook_registry.json`
8. **Monthly Archives** → `.tar.gz` files + summary JSONs

### Proposed Database Solution

All persistent data moves to **Snowflake** in a new schema: `PROD_MO_MONM.DQP_METADATA`

---

## Database Schema Design

### 1. VALIDATION_RESULTS Table

Stores all validation run results (replaces JSON files + daily cache).

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.VALIDATION_RESULTS (
    -- Primary key
    RUN_ID VARCHAR(100) PRIMARY KEY,  -- Format: {suite_name}_{YYYY-MM-DD_HH-MM-SS}

    -- Metadata
    SUITE_NAME VARCHAR(200) NOT NULL,
    DATA_DATE DATE NOT NULL,
    RUN_TIMESTAMP TIMESTAMP_NTZ NOT NULL,

    -- Results (stored as JSON)
    RESULTS VARIANT,                      -- Array of validation result objects
    DERIVED_STATUS_RESULTS VARIANT,       -- Array of derived status result objects
    VALIDATED_MATERIALS VARIANT,          -- Array of material numbers

    -- Indexing
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT unique_suite_run UNIQUE (SUITE_NAME, RUN_TIMESTAMP)
);

-- Indexes for efficient querying
CREATE INDEX idx_suite_date ON PROD_MO_MONM.DQP_METADATA.VALIDATION_RESULTS (SUITE_NAME, DATA_DATE);
CREATE INDEX idx_data_date ON PROD_MO_MONM.DQP_METADATA.VALIDATION_RESULTS (DATA_DATE);
```

**Benefits:**
- Single table replaces both persistent results and daily cache
- Automatic deduplication via unique constraint
- Fast queries by suite + date
- VARIANT columns efficiently store JSON without schema changes

---

### 2. UNIFIED_LOGS Table

Stores Data Lark rectification logs (replaces CSV files).

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.UNIFIED_LOGS (
    -- Primary key
    LOG_ID NUMBER IDENTITY PRIMARY KEY,

    -- Data Lark log fields
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    PLUGIN VARCHAR(200),
    MATERIAL_NUMBER VARCHAR(50),
    FIELD VARCHAR(200),
    EXTRA VARCHAR(1000),
    STATUS VARCHAR(50),           -- 'Success' or 'Failed'
    NOTE VARCHAR(5000),

    -- Source tracking
    SOURCE_FILE VARCHAR(500),     -- Original CSV filename
    IMPORT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Indexing
    CONSTRAINT unique_log_entry UNIQUE (TIMESTAMP, MATERIAL_NUMBER, FIELD, STATUS)
);

-- Indexes for efficient querying
CREATE INDEX idx_material_field ON PROD_MO_MONM.DQP_METADATA.UNIFIED_LOGS (MATERIAL_NUMBER, FIELD);
CREATE INDEX idx_status ON PROD_MO_MONM.DQP_METADATA.UNIFIED_LOGS (STATUS);
CREATE INDEX idx_timestamp ON PROD_MO_MONM.DQP_METADATA.UNIFIED_LOGS (TIMESTAMP);
```

**Benefits:**
- Replaces dynamic CSV file reading
- Indexed queries for rectification status checks
- Data Lark can write directly to this table (if supported) or via API ingestion

---

### 3. RULEBOOK_REGISTRY Table

Stores validation rules catalog (replaces JSON file).

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.RULEBOOK_REGISTRY (
    -- Primary key
    RULE_ID NUMBER IDENTITY PRIMARY KEY,

    -- Rule identification
    SUITE_NAME VARCHAR(200) NOT NULL,
    EXPECTATION_TYPE VARCHAR(200) NOT NULL,

    -- Rule definition (stored as JSON for flexibility)
    RULE_DEFINITION VARIANT NOT NULL,  -- Contains: column, added_on, value_set, etc.

    -- Metadata
    ADDED_ON DATE NOT NULL,
    LAST_MODIFIED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    -- Deduplication
    RULE_HASH VARCHAR(64) NOT NULL,  -- SHA-256 of rule definition for deduplication

    CONSTRAINT unique_rule UNIQUE (SUITE_NAME, EXPECTATION_TYPE, RULE_HASH)
);

-- Index for suite-level queries
CREATE INDEX idx_suite_rules ON PROD_MO_MONM.DQP_METADATA.RULEBOOK_REGISTRY (SUITE_NAME);
```

**Benefits:**
- Git-tracked file becomes database-backed
- Deduplication via hash
- Queryable rule history

---

### 4. COLUMN_METADATA Table

Stores vw_ProductDataAll schema metadata (replaces JSON cache).

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.COLUMN_METADATA (
    -- Primary key
    COLUMN_NAME VARCHAR(200) PRIMARY KEY,

    -- Metadata
    DATA_TYPE VARCHAR(100),
    DISTINCT_VALUES VARIANT,      -- Array of distinct values (if < 100)
    DISTINCT_COUNT NUMBER,         -- Count of distinct values

    -- Cache management
    CACHED_AT TIMESTAMP_NTZ NOT NULL,
    REFRESH_INTERVAL_HOURS NUMBER DEFAULT 24,

    -- Version tracking
    SCHEMA_VERSION VARCHAR(50) DEFAULT '1.0'
);
```

**Benefits:**
- Automatically refreshes on schedule (vs manual file updates)
- Can be shared across deployments
- Supports schema evolution tracking

---

### 5. MONTHLY_OVERVIEW_CACHE Table

Stores pre-computed monthly aggregations (replaces JSON cache).

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.MONTHLY_OVERVIEW_CACHE (
    -- Primary key
    MONTH VARCHAR(7) PRIMARY KEY,  -- Format: YYYY-MM

    -- Aggregated metrics (stored as JSON for flexibility)
    CURRENT_TOTAL NUMBER,
    PREVIOUS_TOTAL NUMBER,
    DELTA NUMBER,
    CURRENT_MATERIALS VARIANT,           -- Array of material numbers
    PRODUCT_HIERARCHY_BREAKDOWN VARIANT, -- Array of {Product Hierarchy, Count}
    LOGS_STATS VARIANT,                  -- Nested object with log aggregations

    -- Cache metadata
    CACHED_AT TIMESTAMP_NTZ NOT NULL,
    DATA_DATE DATE NOT NULL,

    -- Auto-expiration
    EXPIRES_AT TIMESTAMP_NTZ
);

-- Index for date range queries
CREATE INDEX idx_cached_at ON PROD_MO_MONM.DQP_METADATA.MONTHLY_OVERVIEW_CACHE (CACHED_AT);
```

**Benefits:**
- Automatic cache expiration via EXPIRES_AT
- Historical monthly data preserved
- Fast monthly report loading

---

### 6. VALIDATION_FAILURES_CACHE Table

Stores raw Snowflake validation results for drill-down (replaces CSV cache).

```sql
CREATE TABLE PROD_MO_MONM.DQP_METADATA.VALIDATION_FAILURES_CACHE (
    -- Composite key
    SUITE_NAME VARCHAR(200) NOT NULL,
    DATA_DATE DATE NOT NULL,

    -- Raw results DataFrame stored as Parquet or JSON
    FAILURES_DATA VARIANT NOT NULL,  -- Entire DataFrame serialized to JSON

    -- Metadata
    ROW_COUNT NUMBER,
    CACHED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_failures PRIMARY KEY (SUITE_NAME, DATA_DATE)
);

-- Auto-cleanup: Delete entries older than 7 days
-- Implement via scheduled task or TTL
```

**Benefits:**
- Eliminates CSV file management
- Automatic cleanup of stale caches
- Supports large DataFrames via VARIANT compression

---

## Migration Strategy

### Phase 1: Database Setup (1 day)

1. **Create Snowflake schema and tables**
   ```bash
   python scripts/setup_serverless_schema.py
   ```
   - Creates `DQP_METADATA` schema
   - Creates all 6 tables with indexes
   - Sets up proper permissions

2. **Migrate historical data**
   ```bash
   python scripts/migrate_to_database.py --dry-run
   python scripts/migrate_to_database.py --execute
   ```
   - Loads existing JSON files → VALIDATION_RESULTS table
   - Loads CSV logs → UNIFIED_LOGS table
   - Loads rulebook → RULEBOOK_REGISTRY table
   - Validates data integrity

### Phase 2: Code Refactoring (2-3 days)

1. **Create database abstraction layer**
   - New module: `core/persistence.py`
   - Functions: `save_validation_results()`, `load_validation_results()`, etc.
   - Drop-in replacement for `cache_manager.py` functions

2. **Update cache_manager.py**
   - Replace file I/O with database calls
   - Keep same function signatures for backward compatibility
   - Add Streamlit session-state caching for performance

3. **Update unified_logs.py**
   - Replace CSV reading with Snowflake queries
   - Add pagination for large result sets
   - Maintain same API for consumers

4. **Update rulebook_manager.py**
   - Replace JSON file with database operations
   - Add hash-based deduplication

### Phase 3: Testing & Validation (1-2 days)

1. **Integration tests**
   - Verify all reports load correctly
   - Check monthly overview calculations
   - Test drill-down functionality

2. **Performance benchmarks**
   - Compare database query times vs file reads
   - Optimize slow queries with additional indexes

3. **Data integrity checks**
   - Compare file-based results with database results
   - Ensure no data loss during migration

### Phase 4: Deployment (1 day)

1. **Deploy to SiS**
   - Update `.streamlit/secrets.toml` with Snowflake credentials
   - Remove file system dependencies from deployment config
   - Deploy application

2. **Cutover**
   - Disable file-based archival scripts
   - Enable database-backed operations
   - Monitor for issues

---

## Code Changes Overview

### New Module: core/persistence.py

```python
"""
Database persistence layer for serverless deployment.

Replaces file-based cache_manager.py with Snowflake-backed storage.
"""

import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
from core.queries import run_query, get_connection

SCHEMA = "PROD_MO_MONM.DQP_METADATA"

def save_validation_results(
    suite_name: str,
    results: list,
    validated_materials: list,
    raw_results_df: pd.DataFrame,
    data_date: Optional[str] = None,
    derived_status_results: list = None,
) -> None:
    """Save validation results to database (replaces save_daily_suite_artifacts)."""

    run_id = f"{suite_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
    data_date = data_date or datetime.now().strftime('%Y-%m-%d')

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
            INSERT INTO {SCHEMA}.VALIDATION_RESULTS
            (RUN_ID, SUITE_NAME, DATA_DATE, RUN_TIMESTAMP, RESULTS,
             DERIVED_STATUS_RESULTS, VALIDATED_MATERIALS)
            VALUES (?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?), PARSE_JSON(?))
        """, (
            run_id,
            suite_name,
            data_date,
            datetime.now(),
            json.dumps(results or []),
            json.dumps(derived_status_results or []),
            json.dumps(validated_materials or [])
        ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def get_cached_results(suite_name: str) -> Optional[dict]:
    """Get cached validation results for today (replaces file-based cache)."""

    today = datetime.now().strftime('%Y-%m-%d')

    query = f"""
        SELECT RESULTS, DERIVED_STATUS_RESULTS, VALIDATED_MATERIALS
        FROM {SCHEMA}.VALIDATION_RESULTS
        WHERE SUITE_NAME = '{suite_name}'
          AND DATA_DATE = '{today}'
        ORDER BY RUN_TIMESTAMP DESC
        LIMIT 1
    """

    df = run_query(query)

    if df.empty:
        return None

    row = df.iloc[0]
    return {
        "results": json.loads(row["RESULTS"]),
        "derived_status_results": json.loads(row["DERIVED_STATUS_RESULTS"]),
        "validated_materials": json.loads(row["VALIDATED_MATERIALS"])
    }

def load_validation_history() -> list:
    """Load all historical validation results (replaces file globbing)."""

    query = f"""
        SELECT
            RUN_ID,
            SUITE_NAME,
            DATA_DATE,
            RUN_TIMESTAMP,
            RESULTS,
            DERIVED_STATUS_RESULTS,
            VALIDATED_MATERIALS
        FROM {SCHEMA}.VALIDATION_RESULTS
        ORDER BY RUN_TIMESTAMP DESC
    """

    df = run_query(query)

    history = []
    for _, row in df.iterrows():
        history.append({
            "suite_name": row["SUITE_NAME"],
            "data_date": row["DATA_DATE"],
            "results": json.loads(row["RESULTS"]),
            "derived_status_results": json.loads(row["DERIVED_STATUS_RESULTS"]),
            "validated_materials": json.loads(row["VALIDATED_MATERIALS"])
        })

    return history

# Similar functions for:
# - save_unified_log_entry()
# - get_rectified_materials()
# - save_rulebook_entry()
# - get_cached_column_metadata()
# - save_monthly_overview()
# etc.
```

### Updated: core/cache_manager.py

```python
"""
Backward-compatible cache manager that delegates to database persistence.

Maintains existing API for minimal code changes.
"""

from core.persistence import (
    save_validation_results as _db_save_validation_results,
    get_cached_results as _db_get_cached_results,
    # ... other imports
)

def save_daily_suite_artifacts(*args, **kwargs):
    """Delegate to database persistence layer."""
    return _db_save_validation_results(*args, **kwargs)

def get_cached_results(*args, **kwargs):
    """Delegate to database with session-state caching."""
    import streamlit as st

    cache_key = f"cached_results_{args[0]}"

    # Check session state first (in-memory cache)
    if cache_key in st.session_state:
        return st.session_state[cache_key]

    # Fetch from database
    result = _db_get_cached_results(*args, **kwargs)

    # Cache in session state
    if result:
        st.session_state[cache_key] = result

    return result

# Similar delegations for other functions...
```

---

## Caching Strategy for Performance

### Three-Tier Caching

1. **Streamlit Session State** (fastest, ephemeral)
   - Stores data for current user session
   - Automatically cleared on page refresh
   - Used for: validation results, column metadata, monthly overview

2. **Snowflake Tables** (persistent)
   - Stores all historical data
   - Survives deployments and restarts
   - Used for: validation results, unified logs, rulebook

3. **Snowflake Result Cache** (automatic)
   - Snowflake automatically caches query results for 24 hours
   - Speeds up repeated identical queries

### Session-State Implementation

```python
import streamlit as st
from datetime import datetime

def cached_query(cache_key: str, query_fn, ttl_minutes: int = 60):
    """
    Execute query with session-state caching and TTL.

    Args:
        cache_key: Unique identifier for this cache entry
        query_fn: Function that returns data to cache
        ttl_minutes: Cache expiration time in minutes
    """
    cache_data = st.session_state.get(cache_key)

    # Check if cache exists and is fresh
    if cache_data:
        cached_at = cache_data.get("cached_at")
        if cached_at:
            age_minutes = (datetime.now() - cached_at).total_seconds() / 60
            if age_minutes < ttl_minutes:
                return cache_data["data"]

    # Cache miss or stale - fetch fresh data
    data = query_fn()

    st.session_state[cache_key] = {
        "data": data,
        "cached_at": datetime.now()
    }

    return data

# Usage example:
def load_monthly_overview():
    return cached_query(
        "monthly_overview",
        lambda: _fetch_monthly_overview_from_db(),
        ttl_minutes=360  # 6 hours
    )
```

---

## Data Lark Integration Options

### Option 1: Direct Database Write (Recommended)

Data Lark writes directly to `UNIFIED_LOGS` table via Snowflake connection.

**Pros:**
- Real-time data availability
- No file transfer required
- Single source of truth

**Cons:**
- Requires Data Lark to support Snowflake writes

### Option 2: API Ingestion

Data Lark posts logs to a Streamlit API endpoint, which writes to database.

```python
# New file: api/ingest_logs.py
import streamlit as st
from core.persistence import save_unified_log_entry

def ingest_log_endpoint():
    """API endpoint for Data Lark to post logs."""
    if st.experimental_get_query_params().get("action") == ["ingest_log"]:
        log_data = st.session_state.get("log_payload")
        if log_data:
            save_unified_log_entry(log_data)
            return {"status": "success"}
```

### Option 3: CSV Import Script (Fallback)

If Data Lark must continue writing CSVs, run a scheduled import script:

```bash
# Cron job: Run every 5 minutes
python scripts/import_unified_logs.py
```

---

## Archive Strategy

### Historical Data Retention

**Validation Results:**
- Keep all runs in database indefinitely (Snowflake handles compression)
- No need for monthly tar.gz archives

**Unified Logs:**
- Keep last 12 months in database
- Archive older data to Snowflake's internal stage or S3

```sql
-- Archive old logs to internal stage
CREATE OR REPLACE STAGE DQP_METADATA.ARCHIVES;

COPY INTO @DQP_METADATA.ARCHIVES/unified_logs_2024.parquet
FROM (
    SELECT * FROM DQP_METADATA.UNIFIED_LOGS
    WHERE TIMESTAMP < DATEADD(month, -12, CURRENT_DATE())
);

-- Delete archived data
DELETE FROM DQP_METADATA.UNIFIED_LOGS
WHERE TIMESTAMP < DATEADD(month, -12, CURRENT_DATE());
```

---

## Deployment Checklist

### Pre-Deployment

- [ ] Create Snowflake schema and tables
- [ ] Migrate historical data to database
- [ ] Update all file I/O code to use database
- [ ] Add session-state caching
- [ ] Run integration tests
- [ ] Benchmark performance
- [ ] Update documentation

### Deployment

- [ ] Deploy to SiS staging environment
- [ ] Verify Snowflake connectivity
- [ ] Test all reports load correctly
- [ ] Monitor query performance
- [ ] Deploy to production

### Post-Deployment

- [ ] Archive old CSV/JSON files
- [ ] Remove file-based archival scripts
- [ ] Update Data Lark integration
- [ ] Monitor Snowflake costs
- [ ] Document operational procedures

---

## Performance Optimization

### Database Indexes

All tables include indexes on frequently queried columns:
- Suite name + date (validation results)
- Material number (unified logs, validation results)
- Timestamp (unified logs, monthly cache)

### Query Optimization Tips

1. **Use LIMIT for exploratory queries**
   ```sql
   SELECT * FROM VALIDATION_RESULTS LIMIT 100
   ```

2. **Filter by date range**
   ```sql
   WHERE DATA_DATE >= DATEADD(day, -30, CURRENT_DATE())
   ```

3. **Use VARIANT efficiently**
   ```sql
   SELECT RESULTS:results[0]:column AS first_result_column
   ```

4. **Leverage Snowflake result cache**
   - Identical queries within 24 hours return instantly from cache

---

## Cost Considerations

### Snowflake Storage Costs

- **Compressed storage:** ~$23/TB/month
- **Estimated data size:**
  - Validation results: ~10 MB/day = 300 MB/month
  - Unified logs: ~5 MB/day = 150 MB/month
  - **Total:** ~450 MB/month = $0.01/month

### Snowflake Compute Costs

- **Query costs:** Based on warehouse usage
- **Estimated usage:**
  - Daily validations: ~5 seconds compute time
  - Monthly overview: ~10 seconds compute time
  - **Total:** ~2 minutes/day = $0.05/day (X-Small warehouse)

**Total estimated cost increase:** ~$1.50/month

---

## Rollback Plan

If issues arise during deployment:

1. **Revert code to previous version**
   ```bash
   git checkout previous-stable-version
   git push -f origin main
   ```

2. **Restore file-based operations**
   - Database tables remain intact
   - File-based code still functional (kept as backup)

3. **Export database data back to files**
   ```bash
   python scripts/export_database_to_files.py
   ```

---

## Future Enhancements

1. **Real-time dashboards**
   - Stream validation results to database as they complete
   - Live-updating monthly overview

2. **Multi-user collaboration**
   - Shared validation history across users
   - Audit trail for rule changes

3. **Advanced analytics**
   - Trend analysis across months
   - Failure pattern detection
   - Predictive validation

4. **API endpoints**
   - REST API for external systems to query validation status
   - Webhook notifications for failed validations

---

## Questions for Discussion

1. **Data Lark Integration:** Which option do you prefer for unified log ingestion?
   - Direct database writes
   - API endpoint
   - CSV import script

2. **Archive Strategy:** How long should we retain historical validation results?
   - Keep all indefinitely (recommended, ~$0.01/month)
   - Archive after 12 months
   - Archive after 6 months

3. **Migration Timeline:** When would you like to execute this migration?
   - Immediately
   - After testing in staging
   - Phased rollout (one component at a time)

4. **Performance Requirements:** What are acceptable load times for reports?
   - Monthly overview: < 2 seconds?
   - Drill-down report: < 3 seconds?

5. **Snowflake Costs:** Is ~$1.50/month additional cost acceptable?

---

## Next Steps

Once you approve this plan, I will:

1. Create database setup scripts (`setup_serverless_schema.py`)
2. Create migration scripts (`migrate_to_database.py`)
3. Implement `core/persistence.py` module
4. Update existing code to use new persistence layer
5. Add comprehensive tests
6. Update documentation

Estimated implementation time: **4-5 days** (including testing)

Ready to proceed? Let me know your preferences for the questions above!
