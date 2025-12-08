# CLAUDE.md - AI Assistant Guide for MONM-MDM-DQP

## Project Overview

**MONM-MDM-DQP** (Master Data Management - Data Quality Platform) is a production data validation web application built with Streamlit and Great Expectations (GX). It validates master data records from Snowflake, focusing on Aurora Motors catalog data and Level 1 baseline validations.

### Core Purpose
- Validate MDM data quality using Great Expectations
- Track validation failures by Material Number
- Provide interactive reporting dashboards
- Integrate with Data Lark for issue tracking

## Technology Stack

| Component | Technology |
|-----------|------------|
| Web Framework | Streamlit |
| Data Validation | Great Expectations (GX) |
| Database | Snowflake (external browser auth) |
| Data Processing | pandas, numpy |
| API Integration | requests (Data Lark) |
| Deployment | Docker Compose / Swarm |
| Platform | Windows (PowerShell scripts) |

## Directory Structure

```
MONM-MDM-DQP/
├── app/                          # Streamlit web application
│   ├── index.py                  # Main entry point (Control Center)
│   ├── suite_discovery.py        # Suite discovery from YAML files
│   ├── shared_ui.py              # Shared UI utilities & failure extraction
│   ├── components/               # Reusable UI components
│   │   └── drill_down.py         # Drill-down interface
│   └── pages/                    # Multi-page app pages
│       ├── Validation_Report.py  # Dynamic validation report (all suites)
│       ├── Monthly_Overview.py   # Monthly trends
│       └── YAML_Editor.py        # Interactive YAML suite editor
├── core/                         # Core business logic
│   ├── config.py                 # Snowflake/DataLark configuration
│   ├── queries.py                # Database query functions
│   ├── gx_runner.py              # Validation orchestration engine
│   ├── rulebook_manager.py       # Rule registry management
│   ├── cache_manager.py          # Result caching system
│   └── utils.py                  # Utility functions
├── custom_expectations/          # Custom validation expectations
│   ├── __init__.py               # Registry and exports
│   ├── base.py                   # CustomExpectation base class
│   ├── lookup_validation.py      # Cross-table lookup validation
│   └── conditional_rules.py      # Conditional business rules
├── validations/                  # Validation suite framework
│   └── base_validation.py        # Base class for YAML-driven validation
├── data_lark/                    # Data Lark API integration
│   └── client.py                 # API client
├── validation_results/           # Output directory (JSON files)
├── app_launcher.py               # Entry point script
├── deploy.ps1                    # Docker deployment script
├── teardown.ps1                  # Docker cleanup script
├── docker-compose.yaml           # Docker configuration
└── rulebook_registry.json        # Auto-updated rule registry
```

## Key Concepts

### Validation Suites

All validation suites inherit from `BaseValidationSuite` in `validations/base_validation.py`:

```python
class YourValidation(BaseValidationSuite):
    SUITE_NAME = "Your_Validation_Name"
    INDEX_COLUMN = "Material Number"

    def __init__(self):
        df = get_your_dataframe()  # From core/queries.py
        super().__init__(df)

    def define_expectations(self):
        # Define GX expectations using self.expect()
        self.expect(gx.expectations.ExpectColumnValuesToNotBeNull(
            column="Column Name",
            result_format=self.DEFAULT_RESULT_FORMAT
        ))
```

### Result Format

Always use `self.DEFAULT_RESULT_FORMAT` for expectations:
```python
{
    "result_format": "COMPLETE",
    "unexpected_index_column_names": ["Material Number"],
    "include_unexpected_rows": True,
    "partial_unexpected_list_size": 0,
}
```

### GX Runner

The `core/gx_runner.py` orchestrates validation:
1. Patches the validator's `expect()` method to capture rules
2. Instantiates the validator (triggering `define_expectations()`)
3. Executes `validator.run()`
4. Extracts failed Material Numbers
5. Registers rules in `rulebook_registry.json`
6. Saves timestamped JSON results

### Dynamic Report System

The application uses a **dynamic report generation system** that eliminates the need for suite-specific Python pages. All validation reports are rendered through a single `Validation_Report.py` page that:

1. **Discovers suites automatically** - Scans `validation_yaml/*.yaml` files at runtime
2. **Provides suite selection** - Dropdown in sidebar to choose which suite to view
3. **Hydrates reports dynamically** - Loads the selected suite's results and renders:
   - **Overview**: Metrics, pie chart, and top failing columns bar chart
   - **Details**: Drill-down by expectation type with Data Lark integration
4. **Maintains consistency** - All suites use the same proven report structure

**Key benefit**: Business users can create new validation suites purely by adding YAML files—no Python coding required.

### Available Validation Suites

| Suite | Class | Status | Purpose |
|-------|-------|--------|---------|
| Level 1 | `Level1Validation` | Active | Baseline data integrity checks |
| Aurora | `AuroraValidation` | Active | Aurora Motors catalog validation |
| MG4 - End Item Definition | `MG4EndItemDefinition` | In Development | Material Group 4 validations |

## Development Workflows

### Running Locally

```bash
python app_launcher.py
```

This launches Streamlit at `http://localhost:8501`

### Docker Deployment

```powershell
# Deploy
./deploy.ps1

# Teardown
./teardown.ps1
```

### Adding a New Validation Suite

**With Dynamic Reports**: Business users can create new validation suites with **zero Python code**—just create a YAML file!

#### Option 1: YAML-Only (Recommended for Business Users)

1. **Create YAML file** in `validation_yaml/`:
   - Use the YAML Editor page in the Streamlit app, or
   - Copy an existing YAML file (e.g., `Aurora_Motors_Validation.yaml`)

2. **Define metadata**:
   ```yaml
   metadata:
     suite_name: "Your_Validation_Name"
     index_column: "MATERIAL_NUMBER"
     description: "What this suite validates"
     data_source: "get_your_dataframe"  # Query function from core/queries.py
   ```

3. **Add validations** (see YAML examples in existing files):
   ```yaml
   validations:
     - type: "expect_column_values_to_not_be_null"
       columns: ["COLUMN_A", "COLUMN_B"]
     - type: "expect_column_values_to_be_in_set"
       rules:
         "STATUS": ["ACTIVE", "PENDING"]
   ```

4. **That's it!** The suite will automatically appear in the Validation Report page dropdown.

**Note**: You still need to create the query function in `core/queries.py` if it doesn't exist yet.

#### Option 2: Python Class (Advanced)

For complex logic not supported by YAML, you can still create custom Python validation classes by inheriting from `BaseValidationSuite` in `validations/base_validation.py`. This is rarely needed as YAML supports most validation patterns, including custom expectations.

## Code Conventions

### Import Patterns

```python
# Standard library
import os
import json
from datetime import datetime

# Third-party
import great_expectations as gx
import pandas as pd
import numpy as np
import streamlit as st

# Local modules
from core.config import SNOWFLAKE_CONFIG
from core.queries import get_aurora_motor_dataframe
from validations.base_validation import BaseValidationSuite
```

### Configuration Access

```python
from core.config import safe_secret

# Handles both st.secrets and os.getenv
value = safe_secret("KEY_NAME", "default_value")
```

### Error Handling

```python
try:
    # Operation
except Exception as e:
    print(f"⚠️ Error message: {e}")
    return []  # Graceful fallback
```

### Console Output

Use emoji prefixes for status:
- `▶` Starting operation
- `✅` Success
- `⚠️` Warning
- `❌` Error
- `📘` Information (rulebook)
- `📦` Data capture

### JSON Safety

Always use `make_json_safe()` from `core/utils.py` for numpy types:
```python
from core.utils import make_json_safe

value = make_json_safe(numpy_value)
```

### Session State

Cache validation results in Streamlit using suite-specific keys:
```python
# Suite-specific result caching
if "aurora_results" not in st.session_state:
    st.session_state["aurora_results"] = None

if "level1_results" not in st.session_state:
    st.session_state["level1_results"] = None
```

### Shared UI Utilities

The `app/shared_ui.py` module provides reusable UI components and data extraction:

```python
from app.shared_ui import extract_failures, render_failure_summary, render_send_to_datalark_button

# Extract failures into a DataFrame with context columns
df = extract_failures(results)

# Render failure summary in Streamlit
render_failure_summary(results)

# Add Data Lark integration button
render_send_to_datalark_button(payload)
```

### Context Columns

When extracting failures, preserve these contextual columns for traceability:
```python
CONTEXT_COLS = [
    "Sales Organization",
    "Plant",
    "Distribution Channel",
    "Warehouse Number",
    "Storage Type",
    "Storage Location",
]
```

## Great Expectations Patterns

### GX Telemetry

Disable in validation files:
```python
os.environ["GX_DISABLE_TELEMETRY"] = "true"
```

### GX Result Format

The `BaseValidationSuite` class uses the `COMPLETE` result format with `partial_unexpected_list_size: 0` to ensure all validation failures are captured without truncation.

### Common Expectation Types

```python
# Null checks
gx.expectations.ExpectColumnValuesToNotBeNull(column=col)

# Value in set
gx.expectations.ExpectColumnValuesToBeInSet(column=col, value_set=[...])

# Value not in set
gx.expectations.ExpectColumnValuesToNotBeInSet(column=col, value_set=[...])

# Regex match
gx.expectations.ExpectColumnValuesToMatchRegex(column=col, regex=r"pattern")

# Column comparison
gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="A", column_B="B", or_equal=True
)

# Column equality
gx.expectations.ExpectColumnPairValuesToBeEqual(column_A="A", column_B="B")
```

## Custom Expectations Framework

The project supports custom expectations for validation logic that goes beyond standard GX capabilities. Custom expectations are defined in `custom_expectations/` and integrate seamlessly with the YAML-driven validation system.

### Directory Structure

```
custom_expectations/
├── __init__.py              # Registry and exports
├── base.py                  # CustomExpectation base class
├── lookup_validation.py     # Cross-table lookup validation
└── conditional_rules.py     # Conditional business rules
```

### Available Custom Expectations

| Type | Purpose | Use Case |
|------|---------|----------|
| `lookup_in_reference_column` | Validate values exist in reference set | Foreign key validation, code lookups |
| `conditional_required` | If X then Y must not be null | Business rule: FERT requires BOM_STATUS |
| `conditional_value_in_set` | If X then Y must be in set | Business rule: Object code determines MG4 |

### YAML Usage

Custom expectations use the `custom:` prefix in YAML:

```yaml
validations:
  # Standard GX expectation
  - type: "expect_column_values_to_not_be_null"
    columns: [MATERIAL_NUMBER]

  # Custom: Lookup validation
  - type: "custom:lookup_in_reference_column"
    column: "PLANT_CODE"
    reference_values: ["P001", "P002", "P003"]

  # Custom: Lookup from query
  - type: "custom:lookup_in_reference_column"
    column: "SALES_ORG"
    reference_query: "get_aurora_motor_dataframe"
    reference_column: "SALES_ORGANIZATION"

  # Custom: Conditional required
  - type: "custom:conditional_required"
    condition_column: "MATERIAL_TYPE"
    condition_values: ["FERT", "HALB"]
    required_column: "BOM_STATUS"

  # Custom: Conditional value in set
  - type: "custom:conditional_value_in_set"
    condition_column: "OBJECT_CODE"
    condition_values: ["AA", "RA"]
    target_column: "MG4_EXPECTED"
    allowed_values: ["RAE"]
```

### Creating New Custom Expectations

1. **Create a new file** in `custom_expectations/`:

```python
from custom_expectations.base import CustomExpectation, register_custom_expectation

@register_custom_expectation
class MyCustomExpectation(CustomExpectation):
    expectation_type = "my_custom_check"  # Used as: custom:my_custom_check
    description = "Validates my business rule"

    def validate(self) -> list[dict]:
        """Return list of failures."""
        column = self.kwargs.get("column")
        failures = []

        for idx, row in self.df.iterrows():
            if not self._passes_rule(row):
                failures.append({
                    self.index_column: row[self.index_column],
                    "Unexpected Value": f"Failed: {row[column]}",
                })

        return failures

    def _passes_rule(self, row) -> bool:
        # Your validation logic here
        return True
```

2. **Register in `__init__.py`**:

```python
from custom_expectations.my_custom import MyCustomExpectation

__all__ = [
    # ... existing exports
    "MyCustomExpectation",
]
```

3. **Use in YAML**:

```yaml
- type: "custom:my_custom_check"
  column: "MY_COLUMN"
  # ... other parameters
```

### Condition Types for Conditional Expectations

Both `conditional_required` and `conditional_value_in_set` support:

| Condition Type | Description | Example |
|----------------|-------------|---------|
| `condition_values` | Match specific values (OR logic) | `["FERT", "HALB"]` |
| `condition_not_null` | Trigger when column has any value | `true` |
| `condition_regex` | Match regex pattern | `"^AM-.*"` |

### Result Integration

Custom expectation results automatically integrate with:
- Failure extraction (`extract_failures()`)
- UI reporting (same format as GX expectations)
- JSON result files
- Rulebook registry

## Important Files

### Entry Points
- `app_launcher.py` - Primary entry (sets PYTHONPATH, launches Streamlit)
- `app/index.py` - Streamlit main page (Control Center UI)

### Core Logic
- `core/gx_runner.py` - Orchestration engine (run validations)
- `core/queries.py` - All Snowflake query functions
- `core/config.py` - Configuration (Snowflake, Data Lark)

### Validation Framework
- `validations/base_validation.py` - Base class for YAML-driven validation (inherit for advanced use cases)
- `validation_yaml/*.yaml` - YAML validation suite definitions (Aurora Motors, Level 1, etc.)

### UI Components
- `app/pages/Validation_Report.py` - Dynamic report generator for all validation suites
- `app/suite_discovery.py` - Suite discovery and YAML parsing utilities
- `app/components/drill_down.py` - Reusable drill-down interface with Data Lark integration
- `app/shared_ui.py` - Legacy shared utilities (extraction, rendering)

### Data Flow
```
YAML Suite Definition → app/suite_discovery.py (discover suites)
                     ↓
Snowflake → core/queries.py → YAML → gx_runner.py → JSON Results
                                                         ↓
                                      app/pages/Validation_Report.py (dynamic rendering)
```

## Snowflake Configuration

Located in `core/config.py`:
```python
SNOWFLAKE_CONFIG = {
    "account": "ABB-ABB_MO",
    "user": "blake.lillard@us.abb.com",
    "authenticator": "externalbrowser",  # SSO auth
    "role": "R_IS_MO_MONM",
    "warehouse": "WH_MO_MONM",
    "database": "PROD_MO_STAGING",
    "schema": "SAP_BUS",
}
```

## Testing

### Running Tests

Before pushing changes, run the test suite:

```powershell
.\scripts\test.ps1
```

This runs:
1. **YAML validation** - Checks all `validation_yaml/*.yaml` files for syntax errors
2. **Unit tests** - Pytest tests in `tests/` directory

### Test Structure

```
tests/
├── conftest.py                  # Shared fixtures (sample DataFrames, configs)
├── test_yaml_schema.py          # YAML structure validation tests
├── test_column_validation.py    # Column/INDEX_COLUMN validation tests
└── test_custom_expectations.py  # Custom expectations framework tests
```

### What Tests Cover

- YAML schema validation (missing fields, unknown types, typos)
- INDEX_COLUMN existence checks
- Column existence validation before expectation creation
- Empty DataFrame handling
- Custom expectations (lookup, conditional required, conditional value in set)
- Custom expectation registry and integration

### Running Individual Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_yaml_schema.py -v

# Run specific test
python -m pytest tests/test_yaml_schema.py::TestYAMLSchemaValidation::test_valid_config_passes -v

# Validate YAML files only
python scripts/validate_yaml.py validation_yaml/*.yaml
```

### Additional Verification

- Streamlit UI review for visual checks
- Timestamped JSON output files in `validation_results/`
- Console output during validation runs

## Deployment Notes

- **Platform**: Windows (PowerShell scripts)
- **Orchestration**: Docker Swarm
- **Secrets**: Docker secrets (not environment variables)
- **Image**: Expects pre-built `snowflake-test` Docker image

## Common Tasks

### View Validation Results
Results are saved as JSON in `validation_results/<SuiteName>/<SuiteName>_<timestamp>.json`

### Update Rulebook
The rulebook (`rulebook_registry.json`) updates automatically when validations run. Manual editing is not recommended.

### Add New Columns to Check
In the validation's `define_expectations()` method:
```python
for col in ["New Column 1", "New Column 2"]:
    self.expect(gx.expectations.ExpectColumnValuesToNotBeNull(
        column=col, result_format=self.DEFAULT_RESULT_FORMAT
    ))
```

### Add Fixed Value Rules
```python
fixed_value_rules = {
    "Column Name": ["Allowed Value 1", "Allowed Value 2"],
}
for col, allowed in fixed_value_rules.items():
    self.expect(gx.expectations.ExpectColumnValuesToBeInSet(
        column=col, value_set=allowed, result_format=self.DEFAULT_RESULT_FORMAT
    ))
```

## Troubleshooting

### GX Result Truncation
Results should not be truncated when using YAML-driven validation. The `BaseValidationSuite` uses `result_format: COMPLETE` with `partial_unexpected_list_size: 0` to capture all failures.

### Snowflake Auth Issues
Uses external browser authentication - a browser window will open for SSO.

### Missing Material Numbers in Results
Ensure `INDEX_COLUMN = "Material Number"` is set and the column exists in your DataFrame.

### Import Errors
The `app_launcher.py` adds the project root to `sys.path`. If running files directly, you may need to adjust imports.

## Git Workflow

- Main development branch for changes
- Commit messages should describe the "why" not just the "what"
- Test locally before deploying to Docker

## Contact

For Data Lark integration issues, see the API configuration in `core/config.py` and client implementation in `data_lark/client.py`.
