# Codebase Cleanup Summary

## Current State vs. Proposed Clean State

### Files to Remove (Safe to Delete)

#### Great Expectations Dependencies (~1,200 lines)
```
❌ custom_expectations/__init__.py              (12 lines)
❌ custom_expectations/base.py                  (156 lines)
❌ custom_expectations/conditional_rules.py     (216 lines)
❌ custom_expectations/lookup_validation.py     (~200 lines)
❌ validations/base_validation.py               (~400 lines)
❌ core/gx_runner.py                            (~300 lines)
❌ core/chunked_validation.py                   (~200 lines)
❌ core/yaml_to_python.py                       (~150 lines)
❌ tests/test_custom_expectations.py            (~100 lines)
```

#### Old Query Builder System (~800 lines)
```
❌ app/pages/Query_Builder.py                   (155 lines)
❌ queryBuilder.py                              (~500 lines)
❌ core/performance_tuner.py                    (~150 lines)
```

#### Utility Scripts (~300 lines)
```
❌ fix_rulebook.py                              (~100 lines)
❌ test_grain_quick.py                          (~50 lines)
❌ plotly_overview_example.py                   (~150 lines)
❌ docker_launcher.py                           (~50 lines - if going SiS-only)
```

#### Maybe Remove (User Decision)
```
⚠️  validations/snowflake_native_validator.py   (~350 lines - superseded by snowflake_runner.py)
⚠️  scripts/compare_validation_performance.py   (~200 lines - keep for benchmarking?)
⚠️  core/cache_manager.py                       (~150 lines - still useful?)
⚠️  core/column_cache.py                        (~200 lines - still useful for UI?)
```

#### Docker Files (User Decision)
```
⚠️  Dockerfile
⚠️  docker-compose.yaml
```

**Total Removable: ~2,300-3,500 lines of code**

---

### Files to Keep (Core Framework)

#### New Unified Framework (~1,200 lines)
```
✅ validations/sql_generator.py                 (571 lines) - SQL generation engine
✅ validations/snowflake_runner.py              (410 lines) - Validation runner
✅ core/grain_mapping.py                        (~200 lines) - Grain-based context
✅ core/constants.py                            (18 lines) - Application constants
```

#### Core Infrastructure (~600 lines)
```
✅ core/config.py                               (~100 lines) - Snowflake connection
✅ core/queries.py                              (~150 lines) - Query utilities
✅ core/utils.py                                (~200 lines) - General utilities
✅ core/unified_logs.py                         (~150 lines) - Logging
```

#### Streamlit UI (~1,500 lines)
```
✅ app/Home.py                                  (~200 lines) - Home page
✅ app/suite_discovery.py                       (~150 lines) - Suite management
✅ app/pages/YAML_Editor.py                     (~400 lines) - Suite editor
✅ app/pages/Validation_Report.py               (~500 lines) - Results viewer
✅ app/components/drill_down.py                 (~150 lines) - Drill-down UI
✅ app/components/ui_helpers.py                 (~100 lines) - UI utilities
✅ app_launcher.py                              (~50 lines) - Streamlit launcher
```

#### Data Integration (~200 lines)
```
✅ data_lark/client.py                          (~150 lines) - Datalark integration
✅ core/rulebook_manager.py                     (~200 lines) - Rulebook management
```

#### Testing (~400 lines)
```
✅ tests/conftest.py                            (~50 lines) - Test config
✅ tests/test_grain_mapping.py                  (~150 lines) - Grain tests
✅ tests/test_column_validation.py              (~100 lines) - Validation tests
✅ tests/test_yaml_schema.py                    (~100 lines) - YAML tests
✅ scripts/test_unified_runner.py               (~150 lines) - E2E tests
✅ scripts/test_sql_generation.py               (~150 lines) - SQL gen tests
```

#### Configuration & Data
```
✅ validation_yaml/                             (All YAML suite files)
✅ requirements.txt                             (Updated - remove GX)
✅ docs/                                        (All documentation)
```

**Total Keeping: ~4,000 lines**

---

## Impact Summary

### Before Cleanup
- **Total Python files:** ~45 files
- **Total lines of code:** ~6,500-7,000 lines
- **Dependencies:** 11 packages (including heavy GX)
- **Architecture:** GX wrapper + Query Builder + Suite Editor

### After Cleanup
- **Total Python files:** ~25 files (-44%)
- **Total lines of code:** ~4,000 lines (-40-50%)
- **Dependencies:** 6 packages (no GX)
- **Architecture:** Unified YAML → SQL → Results

---

## Dependency Changes

### requirements.txt - Before
```txt
streamlit>=1.28.0
pandas>=2.0.0
numpy>=1.24.0
great_expectations>=0.18.0      ❌ REMOVE (60MB+ package)
snowflake-connector-python>=3.0.0
matplotlib>=3.7.0               ⚠️  OPTIONAL
plotly>=5.18.0
requests>=2.28.0
cryptography>=41.0.0            ❌ REMOVE (for key-pair auth only)
pyyaml>=6.0.0
```

### requirements.txt - After (Standard Deployment)
```txt
streamlit>=1.28.0
pandas>=2.0.0
numpy>=1.24.0
snowflake-connector-python>=3.0.0
plotly>=5.18.0
requests>=2.28.0
pyyaml>=6.0.0
```

### requirements.txt - After (Streamlit in Snowflake)
```txt
streamlit>=1.28.0
pandas>=2.0.0
numpy>=1.24.0
# snowflake-connector-python - built-in to SiS
plotly>=5.18.0
requests>=2.28.0
pyyaml>=6.0.0
```

**Size Reduction:**
- Great Expectations alone: ~60MB+ installed
- Cryptography: ~10MB
- Total dependency size reduction: ~70MB

---

## Architecture Comparison

### Old Architecture (With GX)

```
┌─────────────────────────────────────────────┐
│           Streamlit UI                      │
├─────────────────────────────────────────────┤
│  Query Builder  │  Suite Editor  │ Reports  │
└────────┬────────┴────────┬───────┴─────┬────┘
         │                 │             │
         v                 v             v
┌─────────────┐   ┌──────────────┐   ┌──────────┐
│ queryBuilder│   │ YAML Configs │   │ GX Runner│
└──────┬──────┘   └──────┬───────┘   └────┬─────┘
       │                 │                 │
       v                 v                 v
┌────────────────────────────────────────────┐
│        core/queries.py (persisted SQL)     │
└────────────────┬───────────────────────────┘
                 │
                 v
┌────────────────────────────────────────────┐
│         Great Expectations Engine          │
│  • Chunked validation                      │
│  • Multiple round-trips                    │
│  • Python-based processing                 │
└────────────────┬───────────────────────────┘
                 │
                 v
┌────────────────────────────────────────────┐
│             Snowflake Database             │
└────────────────────────────────────────────┘
```

### New Architecture (Snowflake-Native)

```
┌─────────────────────────────────────────────┐
│           Streamlit UI                      │
├─────────────────────────────────────────────┤
│    YAML Editor    │    Validation Runner    │
│                   │    Validation Report    │
└────────┬──────────┴────────┬────────────────┘
         │                   │
         v                   v
┌──────────────┐    ┌───────────────────────┐
│ YAML Configs │───>│  SQL Generator        │
└──────────────┘    │  (Dynamic from YAML)  │
                    └──────────┬────────────┘
                               │
                               v
                    ┌──────────────────────┐
                    │  Single SQL Query    │
                    │  • All validations   │
                    │  • Grain-based ctx   │
                    │  • One execution     │
                    └──────────┬───────────┘
                               │
                               v
                    ┌──────────────────────┐
                    │ Snowflake Database   │
                    │ (All compute here)   │
                    └──────────┬───────────┘
                               │
                               v
                    ┌──────────────────────┐
                    │  Result Parser       │
                    │  (GX-compatible)     │
                    └──────────────────────┘
```

**Key Differences:**
- ❌ No Query Builder UI needed
- ❌ No persisted SQL queries
- ❌ No GX engine
- ❌ No chunking logic
- ✅ Direct YAML → SQL → Results
- ✅ Single query execution
- ✅ All compute in Snowflake

---

## File Size Comparison

### Before (Current Branch)
```bash
$ find . -name "*.py" -not -path "./venv/*" | xargs wc -l | tail -1
  ~7000 total
```

### After (Proposed Cleanup)
```bash
$ find . -name "*.py" -not -path "./venv/*" | xargs wc -l | tail -1
  ~4000 total
```

**Reduction: ~43% less code to maintain**

---

## Questions for User

Before proceeding with cleanup, please decide:

### 1. Docker Infrastructure
**Keep or Remove?**
- **Keep:** If you want to support local development outside SiS
- **Remove:** If going SiS-only

Files affected:
- `Dockerfile`
- `docker-compose.yaml`
- `docker_launcher.py`

**Recommendation:** Keep for now, remove after SiS migration is stable.

---

### 2. Performance Comparison Script
**Keep or Remove?**
- **Keep:** Useful for benchmarking and demonstrating speed improvements
- **Remove:** Not needed after migration complete

File affected:
- `scripts/compare_validation_performance.py`

**Recommendation:** Keep until migration is complete and results are documented.

---

### 3. Column Cache System
**Keep or Remove?**
- **Keep:** Useful for UI column selection dropdowns
- **Remove:** If not using Query Builder anymore

Files affected:
- `core/column_cache.py`

**Recommendation:** Keep - still useful for YAML Editor autocomplete.

---

### 4. Old Snowflake Validator
**Keep or Remove?**
- **Keep:** As reference implementation
- **Remove:** Superseded by `snowflake_runner.py`

File affected:
- `validations/snowflake_native_validator.py`

**Recommendation:** Remove - no longer needed, runner is better.

---

### 5. Cache Manager
**Keep or Remove?**
- **Keep:** If caching validation results
- **Remove:** If not using result caching

File affected:
- `core/cache_manager.py`

**Recommendation:** Review what it does first, then decide.

---

## Proposed Action Plan

**Step 1: Safe Removals** (No controversy)
- Remove all GX-related code
- Remove Query Builder
- Remove utility scripts
- Update `validations/__init__.py`
- Update `requirements.txt`

**Step 2: User Decisions** (Need your input)
- Decide on Docker files
- Decide on benchmarking scripts
- Decide on cache systems
- Decide on old validator

**Step 3: Testing**
- Run tests after cleanup
- Verify Streamlit still works
- Verify validations still work

**Step 4: Commit & Document**
- Commit cleaned branch
- Update documentation
- Create migration guide

---

## Ready to Proceed?

Please review and provide decisions on:
1. ✅ or ❌ Docker infrastructure
2. ✅ or ❌ Performance comparison script
3. ✅ or ❌ Column cache system
4. ✅ or ❌ Old snowflake_native_validator.py
5. ✅ or ❌ Cache manager

Once you approve, I'll execute the cleanup!
