# MONM-MDM-DQP Features

Master Data Management - Data Quality Platform

---

## Overview

MONM-MDM-DQP is a production data validation platform built with Streamlit and Great Expectations. This document outlines the key features currently available.

---

## Web Application Features

### Monthly Overview Dashboard
The main landing page providing a high-level view of data quality metrics.

- **GX Rules Tracking** - Monitor Level 1, Level 2, and Level 3 rule counts with monthly additions
- **Validated Materials Metrics** - Current month vs. previous month comparison with delta calculation
- **Product Hierarchy Breakdown** - Visual breakdown by product group
- **Data Quality Updates** - Total updates, success/failure counts, and top updated fields
- **Archived History** - Multi-month trend visualization
- **Daily Caching** - Automatic metric caching to avoid re-computation

### Aurora Validation Report
Dedicated reporting page for Aurora Motors catalog validation.

- **Dual View Interface** - Toggle between Overview and Details views
- **Overview View**
  - Failure distribution pie chart (passing vs. failing materials)
  - Key metrics: total materials, materials with failures
  - Top failing columns bar chart
- **Details View**
  - Drill-down by expectation type and column
  - Interactive filtering with cascading dropdowns
  - Failed materials table with context columns
  - Data Lark integration for sending failures

### Level 1 Validation Report
Baseline data integrity validation reporting.

- **Validation Summary** - KPIs for total, passed, failed, and pass/fail rates
- **Top Failing Columns** - Bar chart with percentage breakdown
- **Drill-Down Interface** - Expectation type and column filtering
- **Context Preservation** - Failed materials with Sales Org, Plant, Distribution Channel, etc.
- **Data Lark Integration** - Send failures for rectification

### YAML Suite Editor
Modern, form-based validation suite builder.

- **Mode Selection** - Create new suites or edit existing ones
- **Suite Metadata Editor** - Configure suite name, index column, description, and data source
- **Form-Based Rule Builder** - No YAML syntax knowledge required
- **YAML Preview** - Real-time preview with syntax highlighting
- **Rule Management** - Add, edit, remove, and clear operations
- **Column Autocomplete** - Cached column metadata with 7-day TTL
- **Distinct Value Suggestions** - Auto-suggest values for in-set validations

---

## Validation Capabilities

### Supported Expectation Types
The platform supports 12 Great Expectations validation types:

| Type | Description |
|------|-------------|
| `expect_column_values_to_not_be_null` | Null checks |
| `expect_column_values_to_be_in_set` | Fixed value validation |
| `expect_column_values_to_not_be_in_set` | Exclusion lists |
| `expect_column_values_to_match_regex` | Pattern matching |
| `expect_column_values_to_not_match_regex` | Pattern exclusion |
| `expect_column_pair_values_a_to_be_greater_than_b` | Numeric comparisons |
| `expect_column_pair_values_to_be_equal` | Column equality |
| `expect_column_value_lengths_to_equal` | Fixed length validation |
| `expect_column_value_lengths_to_be_between` | Variable length ranges |
| `expect_column_values_to_be_between` | Numeric ranges |
| `expect_column_values_to_be_unique` | Uniqueness checks |
| `expect_compound_columns_to_be_unique` | Composite key validation |

### Active Validation Suites

**Aurora Motors Validation**
- 40+ individual validation rules
- Null checks on 8 critical fields
- Column pair comparisons (e.g., gross weight >= net weight)
- Excluded value validation (e.g., PROFIT_CENTER != "UNDEFINED")
- Regex pattern matching for whitespace detection
- Fixed value expectations (26 fields)
- Column equality checks

**Level 1 Baseline Validation**
- 25+ baseline integrity checks
- Material number and identifier validation
- Product hierarchy and classification validation
- Weight and dimension attributes
- MRP and procurement settings
- Pricing and organizational attributes

### YAML-Driven Validation Architecture
- **No Python Code Required** - Define validations entirely in YAML
- **Dynamic Expectation Building** - Runtime construction from YAML configuration
- **WYSIWYG Editing** - Visual editor for creating and modifying suites
- **Easy Modification** - Update rules without code deployment

### Custom Expectations Framework
Extensible plugin-based system for validation logic beyond standard GX capabilities.

**Available Custom Expectation Types:**

| Type | Description | Use Case |
|------|-------------|----------|
| `custom:lookup_in_reference_column` | Validate values exist in reference set | Foreign key validation, code lookups |
| `custom:conditional_required` | If X then Y must not be null | Business rules requiring dependent fields |
| `custom:conditional_value_in_set` | If X then Y must be in allowed set | Conditional value constraints |

**Key Features:**
- **Plugin Architecture** - Create new custom expectations by extending base class
- **YAML Integration** - Use `custom:` prefix in YAML validation files
- **Multiple Reference Sources** - Static values, query functions, or same-DataFrame columns
- **Flexible Conditions** - Support for value matching, regex patterns, and not-null checks
- **Seamless UI Integration** - Results flow through existing failure extraction and reporting

**Condition Types for Conditional Expectations:**
- `condition_values` - Match specific values (OR logic)
- `condition_not_null` - Trigger when column has any value
- `condition_regex` - Match regex pattern

**YAML Usage Example:**
```yaml
validations:
  # Lookup validation
  - type: "custom:lookup_in_reference_column"
    column: "PLANT_CODE"
    reference_query: "get_aurora_motor_dataframe"
    reference_column: "PLANT"

  # Conditional required
  - type: "custom:conditional_required"
    condition_column: "MATERIAL_TYPE"
    condition_values: ["FERT", "HALB"]
    required_column: "BOM_STATUS"

  # Conditional value in set
  - type: "custom:conditional_value_in_set"
    condition_column: "OBJECT_CODE"
    condition_values: ["AA", "RA"]
    target_column: "MG4_EXPECTED"
    allowed_values: ["RAE"]
```

---

## Data Integration

### Snowflake Integration
- **Dual Authentication Modes**
  - Development: External Browser (SSO)
  - Production: JWT with private key
- **Parameterized Queries** - Safe SQL execution via pandas
- **Query Registry Pattern** - Dynamic data source resolution

### Data Lark API Integration
- **Failure Transmission** - Send validation failures for tracking
- **Bearer Token Authentication** - Secure API communication
- **Bidirectional Sync** - Track rectification status
- **Automated Issue Creation** - Material failure notification workflow

### Available Query Functions
| Function | Description |
|----------|-------------|
| `get_aurora_motor_dataframe()` | Aurora Motors product data |
| `get_level_1_dataframe()` | Full product data for baseline validation |
| `get_mg4_dataframe()` | Material Group 4 end item definition |

---

## Caching & Performance

### Multi-Level Caching Strategy
1. **Session State Cache** - Fastest, survives page interactions
2. **Daily File Cache** - Persists across sessions, 7-day TTL
3. **Fresh Validation Run** - Authoritative data when cache misses

### Cache Features
- Automatic invalidation at midnight
- Refreshed data at 6 AM daily
- Per-suite cache files
- Cache hit/miss logging
- Graceful fallback on cache miss

---

## Reporting & Export

### Failure Extraction
- Material Number extraction from validation failures
- Unexpected value capture with context
- DataFrame conversion for reporting
- Context column preservation (Sales Org, Plant, Distribution Channel, etc.)

### Result Persistence
- Timestamped JSON output files
- Clean JSON formatting with numpy type conversion
- Organized by suite: `validation_results/<SuiteName>/<SuiteName>_<timestamp>.json`

### Rulebook Registry
- Auto-updated rule tracking (`rulebook_registry.json`)
- Expectation types and configurations
- Column information and allowed values
- Date added tracking
- Deduplication of identical rules

---

## UI Components

### Drill-Down Component
- Interactive cascading filters (expectation type, column)
- Pass/fail summary metrics
- Failed materials table with context
- Data Lark send button with status tracking
- Rectification status indicators

### Shared Utilities
- Failure summary rendering
- Data Lark integration buttons
- Error handling wrappers

---

## Deployment & Operations

### Docker Deployment
- Docker Compose single-service configuration
- Volume mounts for YAML and Python files
- Docker secret integration for credentials
- Auto-restart policy
- Health check endpoint

### PowerShell Scripts (Windows)
- `deploy.ps1` - Docker Swarm deployment automation
- `teardown.ps1` - Container lifecycle management

### Archive & Data Management
- Monthly log archival with summary calculations
- Validation result compression
- Historical trend preservation
- `--dry-run` mode for preview

---

## Configuration

### Environment Support
- Streamlit secrets for web deployment
- Environment variable fallback
- Docker secret file reading
- Configurable per environment

### Context Columns
Preserved across all failure reports for traceability:
- Sales Organization
- Plant
- Distribution Channel
- Warehouse Number
- Storage Type
- Storage Location

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Web Pages | 4 |
| Validation Suites | 2 active |
| GX Expectation Types | 12 supported |
| Custom Expectation Types | 3 available |
| Core Modules | 10 |
| Query Functions | 3 |
| Total Rules | 65+ |

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Web Framework | Streamlit |
| Data Validation | Great Expectations |
| Database | Snowflake |
| Data Processing | pandas, numpy, matplotlib |
| API Integration | requests |
| Deployment | Docker Compose / Swarm |
| Configuration | YAML, JSON |
