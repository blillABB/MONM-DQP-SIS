# Migration Plan: Streamlit in Snowflake (SiS)

## Executive Summary

The new unified Snowflake-native validation framework **can run entirely in Streamlit in Snowflake**, eliminating the need for Docker and Great Expectations. This migration plan outlines what to keep, what to remove, and how to migrate.

## Why This Works in SiS

✅ **No GX dependency** - framework uses only: yaml, pandas, json, pathlib
✅ **No Docker needed** - pure Python + SQL
✅ **Native Snowflake connector** - Snowpark available in SiS
✅ **Minimal Python processing** - 95% of work done in SQL
✅ **Small codebase** - core framework is ~5 files

## Migration Strategy

### Phase 1: Code Cleanup (This Branch)
Remove unnecessary code from the current branch to create a clean baseline.

### Phase 2: SiS Adaptation
Adapt remaining code for Streamlit in Snowflake environment.

### Phase 3: UI Modernization
Update Streamlit UI to use new unified framework.

---

## Phase 1: Code Cleanup - What to Remove

### 🗑️ Can Be Removed Entirely

#### Great Expectations Related
```
custom_expectations/
├── __init__.py              ❌ Remove (GX-specific)
├── base.py                  ❌ Remove (GX-specific)
├── conditional_rules.py     ❌ Remove (GX-specific)
└── lookup_validation.py     ❌ Remove (GX-specific)

validations/
├── __init__.py              ⚠️  Update (remove GX imports)
└── base_validation.py       ❌ Remove (GX wrapper)

core/
├── gx_runner.py             ❌ Remove (GX-specific)
├── chunked_validation.py    ❌ Remove (GX chunking logic)
└── yaml_to_python.py        ❌ Remove (GX conversion)

tests/
└── test_custom_expectations.py  ❌ Remove (tests GX expectations)
```

**Reason:** New framework validates entirely in SQL, no GX needed.

#### Old Query Builder System
```
app/pages/
└── Query_Builder.py         ❌ Remove (replaced by dynamic SQL generation)

queryBuilder.py              ❌ Remove (old query builder logic)

core/
└── column_cache.py          ⚠️  Maybe keep? (useful for UI column selection)
```

**Reason:** SQL generated dynamically from YAML, no need for Query Builder UI.

#### Docker Infrastructure
```
Dockerfile                   ⚠️  Keep for local dev? (user decision)
docker-compose.yaml          ⚠️  Keep for local dev? (user decision)
docker_launcher.py           ❌ Remove if not using Docker
```

**User Decision:** Keep if you want local Docker dev, remove if going SiS-only.

#### Performance Tuning (Now Obsolete)
```
core/
├── performance_tuner.py     ❌ Remove (GX-specific tuning)
└── cache_manager.py         ⚠️  Review (may still be useful for results caching)
```

**Reason:** Performance solved by Snowflake-native approach, tuning no longer needed.

#### Misc/Utility Scripts
```
fix_rulebook.py              ❌ Remove (one-off script)
test_grain_quick.py          ❌ Remove (one-off test)
plotly_overview_example.py  ❌ Remove (example file)
```

---

## Phase 1: Code Cleanup - What to Keep

### ✅ Core Framework (New Unified Approach)

```
validations/
├── sql_generator.py         ✅ KEEP - Core SQL generation engine
├── snowflake_runner.py      ✅ KEEP - Main validation runner
└── snowflake_native_validator.py  ⚠️  Can remove (superseded by snowflake_runner.py)

core/
├── grain_mapping.py         ✅ KEEP - Grain-based context logic
├── constants.py             ✅ KEEP - Application constants
├── config.py                ✅ KEEP - Snowflake connection config (adapt for SiS)
├── queries.py               ✅ KEEP - May still have utility queries
└── utils.py                 ✅ KEEP - General utilities

scripts/
├── test_unified_runner.py   ✅ KEEP - End-to-end testing
├── test_sql_generation.py   ✅ KEEP - SQL generation testing
├── test_sql_generator.py    ✅ KEEP - Original SQL gen test
└── compare_validation_performance.py  ⚠️  Keep for benchmarking, remove after migration complete
```

### ✅ Streamlit UI (Needs Updates)

```
app/
├── Home.py                  ✅ KEEP - Update to use new framework
├── suite_discovery.py       ✅ KEEP - Suite management still useful
├── pages/
│   ├── YAML_Editor.py       ✅ KEEP - Still needed for editing suites
│   └── Validation_Report.py ✅ KEEP - Update to use new framework
└── components/
    ├── drill_down.py        ✅ KEEP - UI component still useful
    └── ui_helpers.py        ✅ KEEP - UI utilities

app_launcher.py              ✅ KEEP - Streamlit launcher
```

### ✅ Data & Configuration

```
validation_yaml/             ✅ KEEP - All YAML suites (unchanged)
docs/                        ✅ KEEP - Documentation
data_lark/                   ✅ KEEP - Datalark integration (unchanged)

core/
└── rulebook_manager.py      ✅ KEEP - Rulebook management still useful
```

### ✅ Testing Infrastructure

```
tests/
├── conftest.py              ✅ KEEP - Test configuration
├── test_grain_mapping.py    ✅ KEEP - Tests grain logic
├── test_column_validation.py ✅ KEEP - Update for new framework
└── test_yaml_schema.py      ✅ KEEP - YAML validation still needed
```

---

## Phase 2: SiS Adaptation - Required Changes

### 1. Update `core/config.py` for Snowpark

**Current:**
```python
import snowflake.connector

SNOWFLAKE_CONFIG = {
    "account": "ABB-ABB_MO",
    "user": "...",
    "authenticator": "externalbrowser",
    # ...
}
```

**SiS Version:**
```python
# In Streamlit in Snowflake, use Snowpark session
from snowflake.snowpark.context import get_active_session

def get_snowflake_session():
    """Get Snowpark session (works in SiS)."""
    return get_active_session()
```

### 2. Update `core/queries.py` for Snowpark

**Current:**
```python
def run_query(sql: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    df = pd.read_sql(sql, conn)
    return df
```

**SiS Version:**
```python
def run_query(sql: str) -> pd.DataFrame:
    session = get_snowflake_session()
    df = session.sql(sql).to_pandas()
    return df
```

### 3. Update `requirements.txt` for SiS

**Remove:**
```
great_expectations>=0.18.0   ❌ Not needed
cryptography>=41.0.0         ❌ Not needed (for key-pair auth)
matplotlib>=3.7.0            ⚠️  Optional (plotting)
```

**Keep:**
```
streamlit>=1.28.0            ✅ Required
pandas>=2.0.0                ✅ Required
numpy>=1.24.0                ✅ Required
plotly>=5.18.0               ✅ For charts
requests>=2.28.0             ✅ For Datalark
pyyaml>=6.0.0                ✅ For YAML parsing
```

**Note:** In SiS, `snowflake-connector-python` is built-in, don't need to specify it.

### 4. Update Streamlit Pages

#### `app/pages/Validation_Report.py`

**Current (uses GX):**
```python
from validations.runner import run_validation_from_yaml
results = run_validation_from_yaml(yaml_path)
```

**New (uses Snowflake-native):**
```python
from validations.snowflake_runner import run_validation_from_yaml_snowflake
results = run_validation_from_yaml_snowflake(yaml_path)
```

#### `app/Home.py`

Update to mention new Snowflake-native approach, remove GX references.

### 5. File Structure for SiS

**Minimal SiS deployment needs:**
```
app/
├── Home.py
├── pages/
│   ├── YAML_Editor.py
│   └── Validation_Report.py
└── components/
    ├── drill_down.py
    └── ui_helpers.py

validations/
├── sql_generator.py
└── snowflake_runner.py

core/
├── grain_mapping.py
├── constants.py
├── config.py (adapted for Snowpark)
└── queries.py (adapted for Snowpark)

validation_yaml/
└── *.yaml (all suite files)

data_lark/
├── __init__.py
└── client.py

requirements.txt (updated)
```

**Total: ~15 core files + YAML configs**

---

## Phase 3: UI Modernization

### New Features to Add

1. **Validation Runner Page**
   - Select YAML suite
   - Set row limit (for testing)
   - Run validation
   - Show real-time progress
   - Display results with drill-down

2. **SQL Preview Page**
   - Load YAML suite
   - Generate and show SQL
   - Show optimization stats (grain-based context)
   - Copy SQL for manual execution

3. **Performance Dashboard**
   - Compare old vs new approach
   - Show execution times
   - Visualize payload size reduction

### Updated Page Structure

```
app/
├── Home.py                          ← Update: Remove GX references
├── pages/
│   ├── 1_Validation_Runner.py       ← NEW: Main validation interface
│   ├── 2_YAML_Editor.py             ← Keep: Suite editing
│   ├── 3_Validation_Report.py       ← Update: Use new framework
│   └── 4_SQL_Preview.py             ← NEW: Show generated SQL
└── components/
    ├── drill_down.py                ← Keep: Failure drill-down
    └── ui_helpers.py                ← Keep: UI utilities
```

---

## Migration Checklist

### Phase 1: Cleanup (This Branch)
- [ ] Remove `custom_expectations/` directory
- [ ] Remove `validations/base_validation.py`
- [ ] Remove `core/gx_runner.py`
- [ ] Remove `core/chunked_validation.py`
- [ ] Remove `core/yaml_to_python.py`
- [ ] Remove `core/performance_tuner.py`
- [ ] Remove `app/pages/Query_Builder.py`
- [ ] Remove `queryBuilder.py`
- [ ] Remove `tests/test_custom_expectations.py`
- [ ] Remove utility scripts: `fix_rulebook.py`, `test_grain_quick.py`, etc.
- [ ] Update `validations/__init__.py` (remove GX imports)
- [ ] Update `requirements.txt` (remove GX)
- [ ] Remove `validations/snowflake_native_validator.py` (superseded by snowflake_runner.py)
- [ ] **Decision:** Keep or remove Docker files?

### Phase 2: SiS Adaptation
- [ ] Create `core/config_sis.py` for Snowpark connection
- [ ] Update `core/queries.py` to use Snowpark
- [ ] Create `requirements_sis.txt` (minimal dependencies)
- [ ] Test SQL generation in SiS environment
- [ ] Test validation execution in SiS environment
- [ ] Update all Streamlit pages to use new framework

### Phase 3: UI Updates
- [ ] Create `app/pages/1_Validation_Runner.py`
- [ ] Create `app/pages/4_SQL_Preview.py`
- [ ] Update `app/Home.py`
- [ ] Update `app/pages/3_Validation_Report.py`
- [ ] Remove Query Builder references from UI
- [ ] Add performance metrics to UI

### Phase 4: Testing & Documentation
- [ ] Test all YAML suites with new framework
- [ ] Verify Datalark integration still works
- [ ] Update all documentation
- [ ] Create SiS deployment guide
- [ ] Archive old approach documentation

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| SiS environment limitations | Low | Medium | Test core framework in SiS early |
| Breaking downstream systems | Low | High | Output format unchanged (GX-compatible) |
| Missing GX functionality | Low | Medium | All validation types already supported in SQL |
| Performance in SiS | Low | Low | SQL execution same as external, Python minimal |
| User adoption | Medium | Medium | Maintain backward compatibility during transition |

---

## Timeline Estimate

**Phase 1 (Cleanup):** 2-4 hours
- Remove old code
- Update imports
- Test remaining code still works

**Phase 2 (SiS Adaptation):** 4-6 hours
- Update config for Snowpark
- Adapt query execution
- Test in SiS environment

**Phase 3 (UI Modernization):** 8-12 hours
- Create new pages
- Update existing pages
- Polish UI/UX

**Phase 4 (Testing & Docs):** 4-6 hours
- Comprehensive testing
- Update documentation
- Create deployment guide

**Total: 18-28 hours** (2.5-3.5 days)

---

## Rollback Plan

If migration encounters issues:

1. **Keep this branch separate** - don't merge to main until fully tested
2. **Old GX approach still on main** - can revert if needed
3. **Gradual migration** - run both approaches in parallel during transition
4. **A/B testing** - compare results between old and new

---

## Benefits Summary

### What You Gain
✅ **No Docker** - Run directly in Snowflake
✅ **No GX dependency** - Simpler stack
✅ **32x faster** - Proven performance improvement
✅ **Simpler codebase** - ~70% less code
✅ **Lower cost** - Less compute, faster execution
✅ **Easier maintenance** - Fewer dependencies
✅ **Native Snowflake** - Leverage platform fully

### What You Keep
✅ **Same YAML format** - No suite changes needed
✅ **Same output format** - Datalark integration unchanged
✅ **Same UI** - Streamlit pages work the same
✅ **Same functionality** - All validation types supported
✅ **Better performance** - Significantly faster

---

## Next Steps

**Immediate:**
1. Review this plan
2. Decide on Docker files (keep for local dev or remove?)
3. Approve Phase 1 cleanup

**After approval:**
1. Execute Phase 1 cleanup on this branch
2. Commit and push cleaned branch
3. Begin Phase 2 SiS adaptation

**Questions to Resolve:**
1. Keep Docker infrastructure for local development?
2. Keep `compare_validation_performance.py` for benchmarking?
3. Keep `core/column_cache.py` for UI column selection?
4. Timeline constraints - any deadline?
5. SiS environment access - is it available for testing?
