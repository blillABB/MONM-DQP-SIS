"""
YAML Suite Editor - Form-based editor for creating and editing validation suites.

This page provides a unified form-based interface for:
- Creating new validation suites from scratch
- Loading and editing existing YAML suites
- Adding/removing/editing validation rules through forms
- Previewing generated YAML
- Saving YAML validation suites (no Python generation needed)
"""

import hashlib
import re
import streamlit as st
import yaml
from pathlib import Path
from validations.sql_generator import build_scoped_expectation_id
from validations.derived_status_resolver import DerivedStatusResolver
from core.column_cache import get_cached_column_metadata, get_cache_info
from core.queries import QUERY_REGISTRY

# ----------------------------------------------------
# Page setup
# ----------------------------------------------------
st.set_page_config(page_title="YAML Suite Editor", layout="wide")
st.title("📝 YAML Suite Editor")
st.caption("Create new or edit existing validation suites using forms")

# ----------------------------------------------------
# Constants
# ----------------------------------------------------
YAML_DIR = Path("validation_yaml")
DEFAULT_TABLE = 'PROD_MO_MONM.REPORTING."vw_ProductDataAll"'

# ----------------------------------------------------
# Sidebar: Column Cache Management
# ----------------------------------------------------
with st.sidebar:
    st.subheader("Column Data Cache")
    cache_info = get_cache_info()

    if cache_info["exists"]:
        st.success(f"Cached: {cache_info['column_count']} columns")
        st.caption(f"Last updated: {cache_info['timestamp_display']}")
    else:
        st.warning("No cache available")
        st.caption("Using fallback column list")

    if st.button("🔄 Refresh Column Data", help="Fetch fresh column data from Snowflake. This may take several minutes."):
        with st.spinner("Fetching column metadata from Snowflake... This may take 2-3 minutes."):
            try:
                # Force refresh from Snowflake
                get_cached_column_metadata(force_refresh=True)
                st.success("Column data refreshed!")
                st.rerun()
            except Exception as e:
                st.error(f"Failed to refresh: {e}")

    st.divider()

# ----------------------------------------------------
# Load column metadata (uses file cache, no TTL)
# ----------------------------------------------------
# No Streamlit cache - file cache handles persistence
metadata = get_cached_column_metadata()
columns = metadata["columns"]
column_types = metadata["column_types"]
distinct_values = metadata["distinct_values"]

# ----------------------------------------------------
# Helper functions
# ----------------------------------------------------
def get_yaml_files():
    """Get all YAML files from the validation_yaml directory."""
    if not YAML_DIR.exists():
        return []
    yaml_files = list(YAML_DIR.glob("*.yaml")) + list(YAML_DIR.glob("*.yml"))
    return sorted(yaml_files, key=lambda x: x.name)

def load_yaml_file(yaml_path: Path) -> dict:
    """Load and parse YAML file."""
    with open(yaml_path, 'r') as f:
        return yaml.safe_load(f)


def extract_validation_targets(validation: dict) -> list[str]:
    """Return a stable list of target fields/columns for a validation."""

    targets = []

    for key, value in validation.items():
        if key in {"type", "expectation_id"}:
            continue

        if key == "rules" and isinstance(value, dict):
            targets.extend(list(value.keys()))
            continue

        if "column" in key or "field" in key:
            if isinstance(value, list):
                targets.extend(str(v) for v in value)
            elif value is not None:
                targets.append(str(value))

    # Return sorted/unique for stability
    seen = set()
    deduped = []
    for target in sorted(targets):
        if target and target not in seen:
            deduped.append(target)
            seen.add(target)

    return deduped


def build_stable_expectation_id(validation: dict, existing_ids: set[str]) -> str:
    """Create a deterministic expectation_id from type/targets when missing."""

    base_type = validation.get("type", "validation")
    targets = extract_validation_targets(validation)

    # Use a hash of targets instead of concatenating all names (keeps IDs short)
    if targets:
        targets_str = "|".join(sorted(targets))  # Sort for determinism
        targets_hash = hashlib.md5(targets_str.encode()).hexdigest()[:8]
        target_identifier = f"cols_{targets_hash}"
    else:
        target_identifier = "notarget"

    raw_base = f"{base_type}_{target_identifier}".lower()
    safe_base = re.sub(r"[^a-z0-9]+", "_", raw_base).strip("_") or "validation"
    candidate = f"exp_{safe_base}"

    counter = 1
    while candidate in existing_ids:
        counter += 1
        candidate = f"exp_{safe_base}_{counter}"

    return candidate


def annotate_session_validations_with_expectation_ids(validations: list[dict]):
    """Ensure every validation has an expectation_id for downstream mapping."""

    existing_ids = {val.get("expectation_id") for val in validations if val.get("expectation_id")}

    for val in validations:
        if not val.get("expectation_id"):
            val["expectation_id"] = build_stable_expectation_id(val, existing_ids)
            existing_ids.add(val["expectation_id"])


def render_conditional_on_controls(editing_rule: dict = None, key_suffix: str = ""):
    """
    Render UI controls for conditional_on clause.

    Args:
        editing_rule: Existing rule being edited (for pre-population)
        key_suffix: Unique suffix for widget keys

    Returns:
        dict or None: conditional_on configuration if enabled, else None
    """
    st.divider()
    st.subheader("⚙️ Advanced: Conditional Logic (Optional)")

    # Get available derived groups
    available_groups = []
    for derived in st.session_state.get("derived_statuses", []):
        exp_id = derived.get("expectation_id")
        status = derived.get("status")
        if exp_id:
            available_groups.append({
                "id": exp_id,
                "label": f"{status} ({exp_id})" if status else exp_id
            })

    if not available_groups:
        st.info("💡 No derived groups available yet. Create derived status groups in Section 7 to use conditional logic.")
        return None

    # Check if editing and has existing conditional_on
    default_enabled = False
    default_group_id = available_groups[0]["id"] if available_groups else None
    default_membership = "exclude"

    if editing_rule and "conditional_on" in editing_rule:
        default_enabled = True
        cond = editing_rule["conditional_on"]
        default_group_id = cond.get("derived_group", default_group_id)
        default_membership = cond.get("membership", "exclude")

    # Enable checkbox
    enable_conditional = st.checkbox(
        "Enable conditional logic",
        value=default_enabled,
        help="Apply this validation only when materials are in or not in a specific derived group",
        key=f"enable_conditional_{key_suffix}"
    )

    if not enable_conditional:
        return None

    col1, col2 = st.columns(2)

    with col1:
        # Find index of default group
        group_options = [g["label"] for g in available_groups]
        group_ids = [g["id"] for g in available_groups]

        try:
            default_idx = group_ids.index(default_group_id)
        except (ValueError, TypeError):
            default_idx = 0

        selected_group_label = st.selectbox(
            "Derived Group",
            options=group_options,
            index=default_idx,
            help="Select which derived group to use as condition",
            key=f"conditional_group_{key_suffix}"
        )

        selected_group_id = group_ids[group_options.index(selected_group_label)]

    with col2:
        membership = st.selectbox(
            "Membership",
            options=["exclude", "include"],
            index=0 if default_membership == "exclude" else 1,
            help=(
                "exclude: Apply validation when NOT in group\n"
                "include: Apply validation only when IN group"
            ),
            key=f"conditional_membership_{key_suffix}"
        )

    st.caption(
        f"✨ This validation will {'**only**' if membership == 'include' else '**not**'} "
        f"apply to materials in the '{selected_group_label}' group"
    )

    return {
        "derived_group": selected_group_id,
        "membership": membership
    }


# NOTE: Catalog building has been moved to DerivedStatusResolver
# This eliminates code duplication and ensures UI and runtime use the same logic.


def format_validation_summary(validation: dict) -> str:
    """Human-friendly description combining type and targets."""

    validation_type = validation.get("type", "Unknown validation")
    targets = extract_validation_targets(validation)

    if targets:
        target_text = ", ".join(targets)
        return f"{validation_type} on {target_text}"

    return validation_type

def save_yaml_suite(
    suite_metadata: dict,
    validations: list,
    data_source: dict | None = None,
    derived_statuses: list | None = None,
    derived_lists: list | None = None,
) -> bool:
    """
    Save YAML validation suite file.

    With the simplified YAML-based validation architecture, we no longer
    need to generate Python classes. Validators are created dynamically
    from YAML at runtime using BaseValidationSuite.from_yaml().
    """
    suite_name = suite_metadata["suite_name"]

    if not suite_name:
        st.error("Please enter a suite name")
        return False

    # Build YAML structure
    yaml_content = {
        "metadata": suite_metadata,
        "data_source": data_source or {},
        "validations": validations,
        "derived_statuses": derived_statuses or [],
        "derived_lists": derived_lists or [],
    }

    # Save YAML file
    yaml_file = YAML_DIR / f"{suite_name}.yaml"
    yaml_file.parent.mkdir(exist_ok=True)

    try:
        with open(yaml_file, 'w') as f:
            yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            f.flush()

        # Verify file was written
        if yaml_file.exists():
            file_size = yaml_file.stat().st_size
            st.success(f"✅ Saved YAML suite: {yaml_file} ({file_size} bytes)")
            st.info("Validations will run directly from YAML - no Python generation needed.")
            return True
        else:
            st.error(f"❌ File does not exist after write attempt!")
            return False

    except Exception as e:
        st.error(f"❌ Error saving YAML file: {e}")
        return False


# Keep old function name as alias for backward compatibility
save_yaml_and_generate_python = save_yaml_suite


# ----------------------------------------------------
# Session state initialization
# ----------------------------------------------------
if "suite_metadata" not in st.session_state:
    st.session_state.suite_metadata = {
        "suite_name": "",
        "index_column": "MATERIAL_NUMBER",
        "description": "",
        "data_source": "get_level_1_dataframe"
    }

if "validations" not in st.session_state:
    st.session_state.validations = []

if "derived_statuses" not in st.session_state:
    st.session_state.derived_statuses = []

if "derived_lists" not in st.session_state:
    st.session_state.derived_lists = []

if "data_source" not in st.session_state:
    st.session_state.data_source = {
        "table": DEFAULT_TABLE,
        "filters": {},
        "distinct": False
    }

if "current_mode" not in st.session_state:
    st.session_state.current_mode = "new"  # "new" or "edit"

if "editing_index" not in st.session_state:
    st.session_state.editing_index = None  # None or index of rule being edited

if "editing_derived_index" not in st.session_state:
    st.session_state.editing_derived_index = None  # None or index of derived group being edited

if "editing_derived_list_index" not in st.session_state:
    st.session_state.editing_derived_list_index = None  # None or index of derived list being edited

# ----------------------------------------------------
# Tab Structure
# ----------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Suite Setup",
    "✅ Validations",
    "📊 Groups & Lists",
    "💾 Preview & Save"
])

# ====================================================
# TAB 1: SUITE SETUP
# ====================================================
with tab1:
    # Workflow Guidance for Suite Setup
    with st.expander("📚 How to use Suite Setup", expanded=False):
        st.markdown("""
        **Suite Setup Workflow:**

        1. **Choose Your Mode**: Create a new suite or edit an existing one
        2. **Configure Metadata**:
           - Suite Name: Used for file naming (e.g., "ABB_SHOP_DATA_PRESENCE")
           - Index Column: Usually MATERIAL_NUMBER - identifies rows in validation results
           - Data Source: Which query function fetches your data
        3. **Add Filters**: Narrow down the data to validate
           - Product hierarchy filters
           - Date range filters
           - Status filters

        💡 **Tip**: Start with broad filters, then refine as you add validations
        """)

    # ----------------------------------------------------
    # Section 1: Mode Selection
    # ----------------------------------------------------
    st.header("1. Select Mode")

    mode = st.radio(
        "What would you like to do?",
        options=["Create New Suite", "Edit Existing Suite"],
        index=0 if st.session_state.current_mode == "new" else 1,
        horizontal=True
    )
    
    st.session_state.current_mode = "new" if mode == "Create New Suite" else "edit"
    
    # ----------------------------------------------------
    # Section 2: Load Existing Suite (if in edit mode)
    # ----------------------------------------------------
    if st.session_state.current_mode == "edit":
        st.header("2. Load Existing Suite")
    
        yaml_files = get_yaml_files()
    
        if not yaml_files:
            st.warning("⚠️ No YAML files found in validation_yaml/ directory")
            st.info("💡 Switch to 'Create New Suite' mode to create your first suite")
        else:
            yaml_file_names = [f.name for f in yaml_files]
    
            selected_file_name = st.selectbox(
                f"Select a YAML file ({len(yaml_files)} available)",
                options=yaml_file_names,
                key="yaml_file_selector"
            )
    
            if st.button("📂 Load Suite", type="primary"):
                selected_yaml_file = YAML_DIR / selected_file_name
    
                try:
                    data = load_yaml_file(selected_yaml_file)
    
                    # Load into session state
                    st.session_state.suite_metadata = data.get("metadata", {})
                    st.session_state.validations = data.get("validations", []) or []
                    st.session_state.derived_statuses = data.get("derived_statuses", []) or []
                    st.session_state.derived_lists = data.get("derived_lists", []) or []
                    loaded_data_source = data.get("data_source")
                    if not isinstance(loaded_data_source, dict):
                        loaded_data_source = {"table": DEFAULT_TABLE, "filters": {}, "distinct": False}
                    else:
                        loaded_data_source.setdefault("distinct", False)
                    st.session_state.data_source = loaded_data_source
                    st.session_state.editing_derived_index = None
                    st.session_state.editing_derived_list_index = None
    
                    st.success(f"✅ Loaded suite: {selected_file_name}")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error loading file: {e}")
    
    # ----------------------------------------------------
    # Section 3: Suite Metadata (only show when creating new suite)
    # ----------------------------------------------------
    if st.session_state.current_mode == "new":
        st.header("3. Suite Metadata")
    
        col1, col2 = st.columns(2)
    
        with col1:
            suite_name = st.text_input(
                "Suite Name",
                value=st.session_state.suite_metadata.get("suite_name", ""),
                placeholder="My_Custom_Validation",
                help="Name of the validation suite (will be used for file naming)",
                key="suite_name_input"
            )
            st.session_state.suite_metadata["suite_name"] = suite_name
    
            index_column = st.selectbox(
                "Index Column",
                options=columns,
                index=columns.index(st.session_state.suite_metadata.get("index_column", "MATERIAL_NUMBER")) if st.session_state.suite_metadata.get("index_column") in columns else 0,
                help="Column to use as the index for tracking failures",
                key="index_column_input"
            )
            st.session_state.suite_metadata["index_column"] = index_column
    
        with col2:
            description = st.text_area(
                "Description",
                value=st.session_state.suite_metadata.get("description", ""),
                placeholder="Describe what this validation suite validates...",
                help="Brief description of the validation suite's purpose",
                height=100,
                key="description_input"
            )
            st.session_state.suite_metadata["description"] = description
    
            # Available data sources (dynamically loaded from QUERY_REGISTRY)
            available_data_sources = sorted(list(QUERY_REGISTRY.keys()))
            current_data_source = st.session_state.suite_metadata.get("data_source", "get_level_1_dataframe")
    
            # If current data source is not in the list, add it as an option
            if current_data_source and current_data_source not in available_data_sources:
                available_data_sources.append(current_data_source)
    
            # Find the index safely
            try:
                current_index = available_data_sources.index(current_data_source)
            except ValueError:
                current_index = 0
    
            data_source = st.selectbox(
                "Data Source",
                options=available_data_sources,
                index=current_index,
                help="Query function to use for fetching data (from core/queries.py)",
                key="data_source_input"
            )
            st.session_state.suite_metadata["data_source"] = data_source
    else:
        # In edit mode, allow editing of suite metadata
        st.header("3. Suite Metadata (Editing)")
    
        col1, col2 = st.columns(2)
    
        with col1:
            # Suite name is read-only in edit mode (it's the filename)
            suite_name = st.session_state.suite_metadata.get("suite_name", "")
            st.text_input(
                "Suite Name (read-only)",
                value=suite_name,
                disabled=True,
                help="Suite name cannot be changed in edit mode (rename the file to change it)",
                key="suite_name_edit_readonly"
            )
    
            index_column = st.selectbox(
                "Index Column",
                options=columns,
                index=columns.index(st.session_state.suite_metadata.get("index_column", "MATERIAL_NUMBER")) if st.session_state.suite_metadata.get("index_column") in columns else 0,
                help="Column to use as the index for tracking failures",
                key="index_column_edit"
            )
            st.session_state.suite_metadata["index_column"] = index_column
    
        with col2:
            description = st.text_area(
                "Description",
                value=st.session_state.suite_metadata.get("description", ""),
                placeholder="Describe what this validation suite validates...",
                help="Brief description of the validation suite's purpose",
                height=100,
                key="description_edit"
            )
            st.session_state.suite_metadata["description"] = description
    
            # Available data sources (dynamically loaded from QUERY_REGISTRY)
            available_data_sources = sorted(list(QUERY_REGISTRY.keys()))
            current_data_source = st.session_state.suite_metadata.get("data_source", "get_level_1_dataframe")
    
            # If current data source is not in the list, add it as an option
            if current_data_source and current_data_source not in available_data_sources:
                available_data_sources.append(current_data_source)
    
            # Find the index safely
            try:
                current_index = available_data_sources.index(current_data_source)
            except ValueError:
                current_index = 0
    
            data_source = st.selectbox(
                "Data Source",
                options=available_data_sources,
                index=current_index,
                help="Query function to use for fetching data (from core/queries.py)",
                key="data_source_edit"
            )
            st.session_state.suite_metadata["data_source"] = data_source
    
        # Delete Suite button
        st.divider()
        if st.button("🗑️ Delete This Suite", type="secondary", key="delete_suite_button"):
            st.session_state.confirm_delete = True
    
        # Confirmation dialog for delete
        if st.session_state.get("confirm_delete", False):
            st.warning("⚠️ Are you sure you want to delete this suite? This cannot be undone!")
            col_confirm, col_cancel = st.columns(2)
            with col_confirm:
                if st.button("✅ Yes, Delete It", type="primary", key="confirm_delete_yes"):
                    yaml_file = YAML_DIR / f"{suite_name}.yaml"
                    if yaml_file.exists():
                        yaml_file.unlink()
                        st.success(f"✅ Deleted suite: {suite_name}")
                        # Reset session state
                        st.session_state.suite_metadata = {
                            "suite_name": "",
                            "index_column": "MATERIAL_NUMBER",
                            "description": "",
                            "data_source": "get_level_1_dataframe"
                        }
                        st.session_state.validations = []
                        st.session_state.derived_statuses = []
                        st.session_state.derived_lists = []
                        st.session_state.data_source = {
                            "table": DEFAULT_TABLE,
                            "filters": {},
                            "distinct": False
                        }
                        st.session_state.current_mode = "new"
                        st.session_state.confirm_delete = False
                        st.session_state.editing_derived_index = None
                        st.session_state.editing_derived_list_index = None
                        st.rerun()
                    else:
                        st.error(f"❌ File not found: {yaml_file}")
            with col_cancel:
                if st.button("❌ Cancel", key="confirm_delete_no"):
                    st.session_state.confirm_delete = False
                    st.rerun()
    
    # ----------------------------------------------------
    # Section 4: Data Source & Query Filters
    # ----------------------------------------------------
    st.header("4. Data Source & Query Filters")
    
    table_value = st.text_input(
        "Source Table",
        value=st.session_state.data_source.get("table", DEFAULT_TABLE),
        help="Fully qualified table or view name used in the generated query",
    )
    st.session_state.data_source["table"] = table_value.strip() or DEFAULT_TABLE
    
    distinct_rows = st.checkbox(
        "Select distinct rows in base CTE",
        value=st.session_state.data_source.get("distinct", False),
        help="Apply SELECT DISTINCT when building the base data set to remove duplicates.",
    )
    st.session_state.data_source["distinct"] = distinct_rows
    
    st.caption(
        "Filters can target any column from the source table. Distinct values are shown when "
        "available from cached metadata."
    )
    
    current_filters = st.session_state.data_source.get("filters", {})
    
    with st.expander("Current Filters", expanded=bool(current_filters)):
        if current_filters:
            for col, condition in list(current_filters.items()):
                col_display, col_remove = st.columns([4, 1])
                with col_display:
                    st.write(f"**{col}**: {condition}")
                with col_remove:
                    if st.button("Remove", key=f"remove_filter_{col}"):
                        del st.session_state.data_source["filters"][col]
                        st.rerun()
        else:
            st.info("No filters defined. Add one using the form below.")
    
    with st.form("add_filter_form", enter_to_submit=False):
        col1, col2 = st.columns([2, 1])
        selected_field = col1.selectbox(
            "Field",
            options=columns,
            help="Choose any column from the source table to filter by",
        )
        filter_type = col2.selectbox(
            "Filter Type",
            options=["Equals", "Not Equals", "One of (IN)", "LIKE pattern", "Date Comparison"],
            help="How should the filter be applied?",
        )
    
        filter_value = None
        if filter_type == "Equals":
            if selected_field in distinct_values:
                filter_value = st.selectbox(
                    "Value",
                    options=distinct_values[selected_field],
                )
            else:
                filter_value = st.text_input(
                    "Value",
                    placeholder="Exact match value",
                )
        elif filter_type == "Not Equals":
            if selected_field in distinct_values:
                filter_value = st.selectbox(
                    "Value",
                    options=distinct_values[selected_field],
                )
            else:
                filter_value = st.text_input(
                    "Value",
                    placeholder="Value to exclude",
                )
        elif filter_type == "One of (IN)":
            if selected_field in distinct_values:
                filter_value = st.multiselect(
                    "Allowed Values",
                    options=distinct_values[selected_field],
                )
            else:
                values_text = st.text_area(
                    "Allowed Values (one per line)",
                    placeholder="Value A\nValue B\nValue C",
                )
                filter_value = [
                    v.strip()
                    for v in values_text.split("\n")
                    if v.strip()
                ]
        elif filter_type == "LIKE pattern":
            filter_value = st.text_input(
                "Pattern",
                placeholder="LIKE ABC%",
                help="Use SQL LIKE syntax (e.g., ABC%, %XYZ)",
            )
        elif filter_type == "Date Comparison":
            st.markdown("**Relative Date Filter** (e.g., 'last 3 years', 'last 6 months')")
    
            col_a, col_b, col_c = st.columns([1, 1, 1])
            with col_a:
                date_operator = st.selectbox(
                    "Operator",
                    options=[">", ">=", "<", "<=", "=", "!="],
                    help="Comparison operator"
                )
            with col_b:
                date_amount = st.number_input(
                    "Amount",
                    min_value=-1000,
                    max_value=0,
                    value=-3,
                    step=1,
                    help="Negative number for past dates (e.g., -3 for '3 years ago')"
                )
            with col_c:
                date_unit = st.selectbox(
                    "Unit",
                    options=["years", "months", "weeks", "days", "quarters"],
                    help="Time unit"
                )
    
            # Build the filter value
            filter_value = f"{date_operator} {date_amount} {date_unit}"
            st.caption(f"Will generate: `{selected_field} {filter_value}`")
    
        submitted = st.form_submit_button("Add / Update Filter", type="primary")
        if submitted:
            if filter_type == "Equals" and filter_value:
                st.session_state.data_source.setdefault("filters", {})[
                    selected_field
                ] = filter_value
                st.success(f"Added filter: {selected_field} = {filter_value}")
                st.rerun()
            elif filter_type == "Not Equals" and filter_value:
                # Store as "<> value" to indicate not-equal comparison
                not_equal_value = f"<> {filter_value}"
                st.session_state.data_source.setdefault("filters", {})[
                    selected_field
                ] = not_equal_value
                st.success(f"Added filter: {selected_field} <> {filter_value}")
                st.rerun()
            elif filter_type == "One of (IN)" and filter_value:
                st.session_state.data_source.setdefault("filters", {})[
                    selected_field
                ] = filter_value
                st.success(
                    f"Added filter: {selected_field} IN ({', '.join(map(str, filter_value))})"
                )
                st.rerun()
            elif filter_type == "LIKE pattern" and filter_value:
                like_value = filter_value.strip()
                if not like_value.upper().startswith("LIKE"):
                    like_value = f"LIKE '{like_value}'"
                st.session_state.data_source.setdefault("filters", {})[
                    selected_field
                ] = like_value
                st.success(f"Added filter: {selected_field} {like_value}")
                st.rerun()
            elif filter_type == "Date Comparison" and filter_value:
                st.session_state.data_source.setdefault("filters", {})[
                    selected_field
                ] = filter_value
                st.success(f"Added date filter: {selected_field} {filter_value}")
                st.rerun()
            else:
                st.error("Please provide a filter value.")

# ====================================================
# TAB 2: VALIDATIONS
# ====================================================
with tab2:
    # Workflow Guidance for Validations
    with st.expander("📚 How to create Validations", expanded=False):
        st.markdown("""
        **Validation Workflow:**

        1. **Choose Validation Type**: Select from categorized expectations
           - Basic Checks: Not Null, Values in Set
           - String/Pattern: Regex matching, length checks
           - Numeric: Value ranges, comparisons
           - Uniqueness: Single or compound keys
        2. **Configure Parameters**: Specify columns, values, or patterns
        3. **Add Conditional Logic** (Optional): Apply validation only when materials are in/not in a derived group
        4. **Review**: Check current rules list before saving

        💡 **Tips**:
        - Use conditional logic to exclude certain materials from validations
        - Create derived groups first if you need conditional validations
        - Not Null checks are the most common starting point
        """)

    # ----------------------------------------------------
    # Section 5: Add/Edit Validation Rules
    # ----------------------------------------------------
    is_editing = st.session_state.editing_index is not None

    if is_editing:
        st.header("5. Edit Validation Rule")
        st.info(f"✏️ Editing Rule #{st.session_state.editing_index + 1}")
    
        # Load the rule being edited
        editing_rule = st.session_state.validations[st.session_state.editing_index]
        default_type = editing_rule.get("type", "expect_column_values_to_not_be_null")
    
        # Cancel edit button
        if st.button("❌ Cancel Edit", key="cancel_edit"):
            st.session_state.editing_index = None
            st.rerun()
    else:
        st.header("5. Add Validation Rules")
        editing_rule = None
        default_type = "expect_column_values_to_not_be_null"
    
    EXPECTATION_OPTIONS = [
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_not_be_in_set",
        "expect_column_values_to_match_regex",
        "expect_column_values_to_not_match_regex",
        "expect_column_value_lengths_to_equal",
        "expect_column_value_lengths_to_be_between",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_unique",
        "expect_compound_columns_to_be_unique",
        "expect_column_pair_values_a_to_be_greater_than_b",
        "expect_column_pair_values_to_be_equal"
    ]
    
    validation_type = st.selectbox(
        "Validation Type",
        options=EXPECTATION_OPTIONS,
        index=EXPECTATION_OPTIONS.index(default_type) if default_type in EXPECTATION_OPTIONS else 0,
        help="Select the type of validation expectation",
        key="validation_type_selector"
    )
    
    st.subheader(f"Configure: {validation_type}")
    
    # --- NOT NULL ---
    if validation_type == "expect_column_values_to_not_be_null":
        # Pre-populate if editing
        default_columns = editing_rule.get("columns", []) if is_editing and editing_rule else []
    
        # Use unique key for edit mode to force re-initialization
        widget_key = f"not_null_columns_edit_{st.session_state.editing_index}" if is_editing else "not_null_columns"
    
        selected_columns = st.multiselect(
            "Select columns that must not be null",
            options=columns,
            default=default_columns,
            help="Choose one or more columns to check for null values",
            key=widget_key
        )
    
        # Render conditional logic controls
        conditional_on = render_conditional_on_controls(
            editing_rule=editing_rule,
            key_suffix="not_null"
        )
    
        button_label = "Update Rule" if is_editing else "Add Not Null Rule"
        if st.button(button_label, key="add_not_null"):
            if selected_columns:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns
                }
                if conditional_on:
                    rule["conditional_on"] = conditional_on
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated not null check for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added not null check for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                st.error("Please select at least one column")
    
    # --- VALUE IN SET ---
    elif validation_type == "expect_column_values_to_be_in_set":
        # Pre-populate if editing
        if is_editing and editing_rule:
            rules_dict = editing_rule.get("rules", {})
            default_column = list(rules_dict.keys())[0] if rules_dict else columns[0]
            default_values = rules_dict.get(default_column, [])
        else:
            default_column = columns[0]
            default_values = []
    
        col_key = f"value_in_set_column_edit_{st.session_state.editing_index}" if is_editing else "value_in_set_column"
        selected_column = st.selectbox(
            "Select Column",
            options=columns,
            index=columns.index(default_column) if default_column in columns else 0,
            key=col_key
        )
    
        if selected_column in distinct_values:
            st.info(f"📊 Available values for {selected_column}")
            values_key = f"value_in_set_values_edit_{st.session_state.editing_index}" if is_editing else "value_in_set_values"
            allowed_values = st.multiselect(
                "Select allowed values",
                options=distinct_values[selected_column],
                default=[v for v in default_values if v in distinct_values[selected_column]],
                help="Choose which values are valid for this column",
                key=values_key
            )
        else:
            default_text = "\n".join(str(v) for v in default_values) if default_values else ""
            text_key = f"value_in_set_text_edit_{st.session_state.editing_index}" if is_editing else "value_in_set_text"
            allowed_values_text = st.text_area(
                "Enter allowed values (one per line)",
                value=default_text,
                placeholder="Value1\nValue2\nValue3",
                help="Enter allowed values, one per line",
                key=text_key
            )
            allowed_values = [line.strip() for line in allowed_values_text.split('\n') if line.strip()]
    
            # Try to convert to numbers
            converted_values = []
            for val in allowed_values:
                try:
                    converted_values.append(int(val))
                except ValueError:
                    try:
                        converted_values.append(float(val))
                    except ValueError:
                        converted_values.append(val)
            allowed_values = converted_values
    
        # Render conditional logic controls
        conditional_on = render_conditional_on_controls(
            editing_rule=editing_rule,
            key_suffix="value_in_set"
        )
    
        button_label = "Update Rule" if is_editing else "Add Value In Set Rule"
        if st.button(button_label, key="add_value_in_set"):
            if allowed_values:
                rule = {
                    "type": validation_type,
                    "rules": {selected_column: allowed_values}
                }
                if conditional_on:
                    rule["conditional_on"] = conditional_on
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated value_in_set rule for {selected_column}")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added value_in_set rule for {selected_column}")
                st.rerun()
            else:
                st.error("Please specify at least one allowed value")
    
    # --- VALUE NOT IN SET ---
    elif validation_type == "expect_column_values_to_not_be_in_set":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_column = editing_rule.get("column", columns[0])
            default_excluded = editing_rule.get("value_set", [])
            default_text = ", ".join(str(v) for v in default_excluded)
        else:
            default_column = columns[0]
            default_text = ""
    
        col_key = f"value_not_in_set_column_edit_{st.session_state.editing_index}" if is_editing else "value_not_in_set_column"
        selected_column = st.selectbox(
            "Select Column",
            options=columns,
            index=columns.index(default_column) if default_column in columns else 0,
            key=col_key
        )
    
        text_key = f"value_not_in_set_text_edit_{st.session_state.editing_index}" if is_editing else "value_not_in_set_text"
        excluded_values_text = st.text_input(
            "Enter excluded values (comma-separated)",
            value=default_text,
            placeholder="UNDEFINED, INVALID, N/A",
            help="Values that should NOT appear in this column",
            key=text_key
        )
        excluded_values = [val.strip() for val in excluded_values_text.split(',') if val.strip()]
    
        button_label = "Update Rule" if is_editing else "Add Value Not In Set Rule"
        if st.button(button_label, key="add_value_not_in_set"):
            if excluded_values:
                rule = {
                    "type": validation_type,
                    "column": selected_column,
                    "value_set": excluded_values
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated value_not_in_set rule for {selected_column}")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added value_not_in_set rule for {selected_column}")
                st.rerun()
            else:
                st.error("Please specify at least one excluded value")
    
    # --- REGEX MATCH ---
    elif validation_type == "expect_column_values_to_match_regex":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("columns", [])
            default_regex = editing_rule.get("regex", "")
        else:
            default_columns = []
            default_regex = ""
    
        cols_key = f"regex_columns_edit_{st.session_state.editing_index}" if is_editing else "regex_columns"
        selected_columns = st.multiselect(
            "Select columns to validate with regex",
            options=columns,
            default=default_columns,
            help="Choose columns that must match the pattern",
            key=cols_key
        )
    
        pattern_key = f"regex_pattern_edit_{st.session_state.editing_index}" if is_editing else "regex_pattern"
        regex_pattern = st.text_input(
            "Regular Expression Pattern",
            value=default_regex,
            placeholder="^\\d{8}$",
            help="Enter a regex pattern",
            key=pattern_key
        )
    
        st.caption("Common patterns: `^\\s*$` (blank), `^\\d{8}$` (8 digits), `^[A-Z]{2,3}$` (2-3 letters)")
    
        button_label = "Update Rule" if is_editing else "Add Regex Match Rule"
        if st.button(button_label, key="add_regex"):
            if selected_columns and regex_pattern:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns,
                    "regex": regex_pattern
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated regex match rule for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added regex match rule for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                st.error("Please select columns and enter a regex pattern")
    
    # --- COLUMN COMPARISON ---
    elif validation_type == "expect_column_pair_values_a_to_be_greater_than_b":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_col_a = editing_rule.get("column_a", columns[0])
            default_col_b = editing_rule.get("column_b", columns[1] if len(columns) > 1 else columns[0])
            default_or_equal = editing_rule.get("or_equal", True)
        else:
            default_col_a = columns[0]
            default_col_b = columns[1] if len(columns) > 1 else columns[0]
            default_or_equal = True
    
        col1, col2 = st.columns(2)
    
        col_a_key = f"comp_col_a_edit_{st.session_state.editing_index}" if is_editing else "comp_col_a"
        col_b_key = f"comp_col_b_edit_{st.session_state.editing_index}" if is_editing else "comp_col_b"
        or_equal_key = f"comp_or_equal_edit_{st.session_state.editing_index}" if is_editing else "comp_or_equal"
    
        with col1:
            column_a = st.selectbox(
                "Column A (left side)",
                options=columns,
                index=columns.index(default_col_a) if default_col_a in columns else 0,
                key=col_a_key
            )
    
        with col2:
            column_b = st.selectbox(
                "Column B (right side)",
                options=columns,
                index=columns.index(default_col_b) if default_col_b in columns else 0,
                key=col_b_key
            )
    
        or_equal = st.checkbox("Allow equal values (>=)", value=default_or_equal, key=or_equal_key)
    
        button_label = "Update Rule" if is_editing else "Add Column Comparison Rule"
        if st.button(button_label, key="add_comparison"):
            if column_a != column_b:
                rule = {
                    "type": validation_type,
                    "column_a": column_a,
                    "column_b": column_b,
                    "or_equal": or_equal
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    operator = ">=" if or_equal else ">"
                    st.success(f"✅ Updated rule: {column_a} {operator} {column_b}")
                else:
                    st.session_state.validations.append(rule)
                    operator = ">=" if or_equal else ">"
                    st.success(f"✅ Added rule: {column_a} {operator} {column_b}")
                st.rerun()
            else:
                st.error("Please select different columns")
    
    # --- COLUMN EQUALITY ---
    elif validation_type == "expect_column_pair_values_to_be_equal":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_col_a = editing_rule.get("column_a", columns[0])
            default_col_b = editing_rule.get("column_b", columns[1] if len(columns) > 1 else columns[0])
        else:
            default_col_a = columns[0]
            default_col_b = columns[1] if len(columns) > 1 else columns[0]
    
        col1, col2 = st.columns(2)
    
        col_a_key = f"eq_col_a_edit_{st.session_state.editing_index}" if is_editing else "eq_col_a"
        col_b_key = f"eq_col_b_edit_{st.session_state.editing_index}" if is_editing else "eq_col_b"
    
        with col1:
            column_a = st.selectbox(
                "Column A",
                options=columns,
                index=columns.index(default_col_a) if default_col_a in columns else 0,
                key=col_a_key
            )
    
        with col2:
            column_b = st.selectbox(
                "Column B",
                options=columns,
                index=columns.index(default_col_b) if default_col_b in columns else 0,
                key=col_b_key
            )
    
        button_label = "Update Rule" if is_editing else "Add Column Equality Rule"
        if st.button(button_label, key="add_equality"):
            if column_a != column_b:
                rule = {
                    "type": validation_type,
                    "column_a": column_a,
                    "column_b": column_b
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated rule: {column_a} must equal {column_b}")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added rule: {column_a} must equal {column_b}")
                st.rerun()
            else:
                st.error("Please select different columns")
    
    # --- REGEX NOT MATCH ---
    elif validation_type == "expect_column_values_to_not_match_regex":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("columns", [])
            default_regex = editing_rule.get("regex", "")
        else:
            default_columns = []
            default_regex = ""
    
        cols_key = f"not_regex_columns_edit_{st.session_state.editing_index}" if is_editing else "not_regex_columns"
        selected_columns = st.multiselect(
            "Select columns to validate (values must NOT match pattern)",
            options=columns,
            default=default_columns,
            help="Choose columns that must NOT match the exclusion pattern",
            key=cols_key
        )
    
        pattern_key = f"not_regex_pattern_edit_{st.session_state.editing_index}" if is_editing else "not_regex_pattern"
        regex_pattern = st.text_input(
            "Regular Expression Pattern (exclusion)",
            value=default_regex,
            placeholder="^TEMP.*|^TEST.*",
            help="Enter a regex pattern to exclude",
            key=pattern_key
        )
    
        st.caption("Common patterns: `^TEMP.*` (starts with TEMP), `^TEST.*` (starts with TEST)")
    
        button_label = "Update Rule" if is_editing else "Add Regex Exclusion Rule"
        if st.button(button_label, key="add_not_regex"):
            if selected_columns and regex_pattern:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns,
                    "regex": regex_pattern
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated regex exclusion rule for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added regex exclusion rule for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                st.error("Please select columns and enter a regex pattern")
    
    # --- VALUE LENGTH EQUALS ---
    elif validation_type == "expect_column_value_lengths_to_equal":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("columns", [])
            default_length = editing_rule.get("value", 0)
        else:
            default_columns = []
            default_length = 8
    
        cols_key = f"length_equal_columns_edit_{st.session_state.editing_index}" if is_editing else "length_equal_columns"
        selected_columns = st.multiselect(
            "Select columns with fixed-length values",
            options=columns,
            default=default_columns,
            help="Choose columns where all values must have the same length",
            key=cols_key
        )
    
        value_key = f"length_equal_value_edit_{st.session_state.editing_index}" if is_editing else "length_equal_value"
        value_length = st.number_input(
            "Required Length",
            min_value=1,
            max_value=1000,
            value=default_length,
            help="The exact length all values must have",
            key=value_key
        )
    
        button_label = "Update Rule" if is_editing else "Add Fixed Length Rule"
        if st.button(button_label, key="add_length_equal"):
            if selected_columns:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns,
                    "value": value_length
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated length={value_length} rule for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added length={value_length} rule for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                st.error("Please select at least one column")
    
    # --- VALUE LENGTH BETWEEN ---
    elif validation_type == "expect_column_value_lengths_to_be_between":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("columns", [])
            default_min = editing_rule.get("min_value", 1)
            default_max = editing_rule.get("max_value", 100)
        else:
            default_columns = []
            default_min = 1
            default_max = 100
    
        cols_key = f"length_between_columns_edit_{st.session_state.editing_index}" if is_editing else "length_between_columns"
        selected_columns = st.multiselect(
            "Select columns with variable-length values",
            options=columns,
            default=default_columns,
            help="Choose columns where values must have length within a range",
            key=cols_key
        )
    
        col1, col2 = st.columns(2)
        min_key = f"length_between_min_edit_{st.session_state.editing_index}" if is_editing else "length_between_min"
        max_key = f"length_between_max_edit_{st.session_state.editing_index}" if is_editing else "length_between_max"
        with col1:
            min_length = st.number_input(
                "Minimum Length",
                min_value=0,
                max_value=10000,
                value=default_min,
                key=min_key
            )
        with col2:
            max_length = st.number_input(
                "Maximum Length",
                min_value=0,
                max_value=10000,
                value=default_max,
                key=max_key
            )
    
        button_label = "Update Rule" if is_editing else "Add Length Range Rule"
        if st.button(button_label, key="add_length_between"):
            if selected_columns and min_length <= max_length:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns,
                    "min_value": min_length,
                    "max_value": max_length
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated length between [{min_length}, {max_length}] for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added length between [{min_length}, {max_length}] for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                if not selected_columns:
                    st.error("Please select at least one column")
                else:
                    st.error("Minimum length must be <= maximum length")
    
    # --- VALUE BETWEEN (numeric) ---
    elif validation_type == "expect_column_values_to_be_between":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("columns", [])
            default_min = editing_rule.get("min_value", 0)
            default_max = editing_rule.get("max_value", 100)
        else:
            default_columns = []
            default_min = 0.0
            default_max = 100.0
    
        cols_key = f"value_between_columns_edit_{st.session_state.editing_index}" if is_editing else "value_between_columns"
        selected_columns = st.multiselect(
            "Select numeric columns",
            options=columns,
            default=default_columns,
            help="Choose numeric columns where values must be within a range",
            key=cols_key
        )
    
        col1, col2 = st.columns(2)
        min_key = f"value_between_min_edit_{st.session_state.editing_index}" if is_editing else "value_between_min"
        max_key = f"value_between_max_edit_{st.session_state.editing_index}" if is_editing else "value_between_max"
        with col1:
            min_value = st.number_input(
                "Minimum Value",
                value=float(default_min),
                format="%.2f",
                key=min_key
            )
        with col2:
            max_value = st.number_input(
                "Maximum Value",
                value=float(default_max),
                format="%.2f",
                key=max_key
            )
    
        button_label = "Update Rule" if is_editing else "Add Numeric Range Rule"
        if st.button(button_label, key="add_value_between"):
            if selected_columns and min_value <= max_value:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns,
                    "min_value": min_value,
                    "max_value": max_value
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated value between [{min_value}, {max_value}] for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added value between [{min_value}, {max_value}] for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                if not selected_columns:
                    st.error("Please select at least one column")
                else:
                    st.error("Minimum value must be <= maximum value")
    
    # --- UNIQUE VALUES ---
    elif validation_type == "expect_column_values_to_be_unique":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("columns", [])
        else:
            default_columns = []
    
        cols_key = f"unique_columns_edit_{st.session_state.editing_index}" if is_editing else "unique_columns"
        selected_columns = st.multiselect(
            "Select columns that must have unique values (no duplicates)",
            options=columns,
            default=default_columns,
            help="Each value in these columns must appear only once",
            key=cols_key
        )
    
        button_label = "Update Rule" if is_editing else "Add Uniqueness Rule"
        if st.button(button_label, key="add_unique"):
            if selected_columns:
                rule = {
                    "type": validation_type,
                    "columns": selected_columns
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated uniqueness rule for {len(selected_columns)} column(s)")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added uniqueness rule for {len(selected_columns)} column(s)")
                st.rerun()
            else:
                st.error("Please select at least one column")
    
    # --- COMPOUND UNIQUE ---
    elif validation_type == "expect_compound_columns_to_be_unique":
        # Pre-populate if editing
        if is_editing and editing_rule:
            default_columns = editing_rule.get("column_list", [])
        else:
            default_columns = []
    
        cols_key = f"compound_unique_columns_edit_{st.session_state.editing_index}" if is_editing else "compound_unique_columns"
        selected_columns = st.multiselect(
            "Select columns that form a composite key",
            options=columns,
            default=default_columns,
            help="The combination of these columns must be unique (composite key)",
            key=cols_key
        )
    
        st.info("💡 This checks that the combination of selected columns is unique (like a composite primary key)")
    
        button_label = "Update Rule" if is_editing else "Add Composite Uniqueness Rule"
        if st.button(button_label, key="add_compound_unique"):
            if len(selected_columns) >= 2:
                rule = {
                    "type": validation_type,
                    "column_list": selected_columns
                }
                if is_editing:
                    st.session_state.validations[st.session_state.editing_index] = rule
                    st.session_state.editing_index = None
                    st.success(f"✅ Updated composite key uniqueness for {len(selected_columns)} columns")
                else:
                    st.session_state.validations.append(rule)
                    st.success(f"✅ Added composite key uniqueness for {len(selected_columns)} columns")
                st.rerun()
            else:
                st.error("Please select at least 2 columns for a composite key")
    
    # ----------------------------------------------------
    # Section 6: Current Validation Rules
    # ----------------------------------------------------
    st.header("6. Current Validation Rules")
    
    annotate_session_validations_with_expectation_ids(st.session_state.validations)
    
    if st.session_state.validations:
        st.success(f"📋 {len(st.session_state.validations)} validation rule(s) configured")
    
        for idx, validation in enumerate(st.session_state.validations):
            with st.expander(f"Rule {idx + 1}: {validation['type']}", expanded=False):
                st.json(validation)
    
                col1, col2 = st.columns(2)
                with col1:
                    if st.button(f"✏️ Edit Rule {idx + 1}", key=f"edit_{idx}"):
                        st.session_state.editing_index = idx
                        st.rerun()
                with col2:
                    if st.button(f"🗑️ Remove Rule {idx + 1}", key=f"remove_{idx}"):
                        st.session_state.validations.pop(idx)
                        # Reset editing if we were editing this rule
                        if st.session_state.editing_index == idx:
                            st.session_state.editing_index = None
                        # Adjust editing index if we removed a rule before the one being edited
                        elif st.session_state.editing_index and st.session_state.editing_index > idx:
                            st.session_state.editing_index -= 1
                        st.rerun()
    
        if st.button("🗑️ Clear All Rules", key="clear_all"):
            st.session_state.validations = []
            st.session_state.editing_index = None
            st.rerun()
    else:
        st.info("No validation rules added yet. Use the form above to add rules.")

# ====================================================
# TAB 3: GROUPS & LISTS
# ====================================================
with tab3:
    # Workflow Guidance for Groups & Lists
    with st.expander("📚 How to use Derived Groups & Lists", expanded=False):
        st.markdown("""
        **Derived Groups & Lists Explained:**

        **Derived Status Groups** identify materials that fail specific validations:
        - **Use Case**: Group materials by data quality issues (e.g., "ABP DATA INCOMPLETE")
        - **Standard Mode**: Quick setup for common patterns
        - **Advanced Mode**: Embedded rules for conditional logic without creating validation failures

        **Derived Lists** combine multiple status groups:
        - **Use Case**: Identify materials ready for next steps (e.g., "READY FOR ABP DATA LOAD")
        - Excludes materials that have ANY of the selected statuses

        💡 **Workflow Tips**:
        1. Create validation rules first (in Validations tab)
        2. Create derived status groups based on those validations
        3. Create derived lists to combine status groups
        4. Use derived groups for conditional validations

        **Standard vs Advanced Mode**:
        - Standard: Simpler, auto-generates settings
        - Advanced: Full control, embedded rules, custom IDs
        """)

    # ----------------------------------------------------
    # Section 7: Derived Status Groups
    # ----------------------------------------------------
    st.header("7. Derived Status Groups")

    is_editing_derived = st.session_state.editing_derived_index is not None
    
    if is_editing_derived:
        st.info(f"✏️ Editing Derived Group #{st.session_state.editing_derived_index + 1}")
        derived_group = st.session_state.derived_statuses[st.session_state.editing_derived_index]
        default_status_label = derived_group.get("status", "")
        default_columns = derived_group.get("columns", []) or []  # New filter-based format
        default_expectation_ids = derived_group.get("expectation_ids", []) or []  # Legacy format
        default_expectation_type = derived_group.get("expectation_type", "")
        default_expectation_id = derived_group.get("expectation_id", "")
    
        if st.button("❌ Cancel Derived Edit", key="cancel_derived_edit"):
            st.session_state.editing_derived_index = None
            st.rerun()
    else:
        derived_group = None
        default_status_label = ""
        default_columns = []
        default_expectation_ids = []
        default_expectation_type = ""
        default_expectation_id = ""
    
    # Use DerivedStatusResolver to build catalog - single source of truth with runtime
    resolver = DerivedStatusResolver(st.session_state.validations, st.session_state.derived_statuses)
    (
        expectation_catalog,
        expectation_label_lookup,
        target_lookup,
    ) = resolver.get_catalog_for_ui()
    
    available_expectation_types = {
        val.get("type") for val in st.session_state.validations if val.get("type")
    }
    
    with st.form("derived_status_form", enter_to_submit=False):
        # Use dynamic keys to prevent Streamlit from caching form state across submissions
        form_suffix = f"{st.session_state.editing_derived_index if is_editing_derived else f'new_{len(st.session_state.derived_statuses)}'}"

        # Mode selector: Standard vs Advanced
        st.caption("💡 **Standard Mode**: Quick setup for common patterns | **Advanced Mode**: Full control with embedded rules and custom settings")
        mode = st.radio(
            "Creation Mode",
            options=["Standard", "Advanced"],
            index=0,
            horizontal=True,
            help="Standard: Simplified interface for common cases. Advanced: Full control with all options.",
            key=f"derived_mode_{form_suffix}"
        )
        is_advanced_mode = mode == "Advanced"
        st.divider()

        status_label = st.text_input(
            "Status Label",
            value=default_status_label,
            placeholder="Warning / Critical / Info",
            help="Label for this derived status grouping",
            key=f"derived_status_label_{form_suffix}",
        )
    
        # Add common types for embedded rules even if they don't exist in validations yet
        embedded_rule_types = {"expect_column_values_to_be_in_set"}
        all_available_types = available_expectation_types | embedded_rule_types

        type_options = ["(All types)"] + sorted(all_available_types)
        if default_expectation_type and default_expectation_type not in type_options:
            type_options.append(default_expectation_type)

        # Expectation Type - Only show in Advanced mode
        if is_advanced_mode:
            type_default_index = type_options.index(default_expectation_type) if default_expectation_type in type_options else 0
            expectation_type = st.selectbox(
                "Filter validations by expectation type (optional)",
                options=type_options,
                index=type_default_index,
                help="Limit the selection list to a specific expectation type and store it with the derived group.",
                key=f"derived_expectation_type_{form_suffix}",
            )
        else:
            # Standard mode: Auto-detect expectation type from first matching validation
            expectation_type = "(All types)"
    
        # Build column/field filter options based on the selected expectation type
        filtered_catalog = [
            entry for entry in expectation_catalog
            if expectation_type in {"(All types)", entry["type"]}
        ]
    
        # For derived groups, ALWAYS show all available columns from the table
        # This allows creating conditional groups for any column, not just those in existing validations
        # Users can create derived groups based on filter columns (like PRODUCT_HIERARCHY) or any other column
        target_options = sorted(columns)
    
        # Default to existing selection when editing, empty when creating new
        default_targets = []
        if is_editing_derived:
            # Prefer new format (columns) if available
            if default_columns:
                default_targets = default_columns
            # Fallback to legacy format (extract from expectation IDs)
            elif default_expectation_ids:
                for exp_id in default_expectation_ids:
                    entry = next((e for e in expectation_catalog if e.get("id") == exp_id), None)
                    if entry and entry.get("targets"):
                        default_targets.extend(entry["targets"])
                default_targets = sorted(set(default_targets))
        # else: leave empty for new groups - user must explicitly select
    
        selected_targets = st.multiselect(
            "Columns/fields to include",
            options=target_options,
            default=default_targets,
            help="Select which columns/fields to include in this derived status. Only expectations targeting these columns will be included.",
            key=f"derived_target_filter_{form_suffix}",
        )
    
        # Advanced: Embed rules for conditional-only groups (Advanced mode only)
        embedded_rules = None
        if is_advanced_mode:
            st.divider()
            embed_rules = st.checkbox(
                "⚙️ Embed rules directly (for conditional logic only)",
                value=bool(derived_group and derived_group.get("rules")),
                help="Enable this to create a grouping that's ONLY used for conditional logic, without creating a separate validation rule.",
                key=f"derived_embed_rules_{form_suffix}"
            )

            if embed_rules and expectation_type == "expect_column_values_to_be_in_set":
                st.info("💡 This derived group will define the condition without reporting validation failures")
        
                # Allow manual column input if needed (workaround for form reactivity)
                manual_column = st.text_input(
                    "Column name (type manually if not in dropdown above)",
                    placeholder="e.g., PRODUCT_HIERARCHY",
                    help="If you can't find the column in the dropdown above, type it here manually",
                    key=f"derived_manual_column_{form_suffix}"
                )
        
                # Use manual input if provided, otherwise use selected_targets
                if manual_column:
                    target_col = manual_column.strip().upper()
                    st.caption(f"Using manually entered column: {target_col}")
                elif len(selected_targets) == 1:
                    target_col = selected_targets[0]
                else:
                    target_col = None
        
                if target_col:
        
                    # Pre-populate if editing
                    default_values = []
                    if is_editing_derived and derived_group:
                        existing_rules = derived_group.get("rules", {})
                        default_values = existing_rules.get(target_col, [])
        
                    # Check if we have distinct values for this column
                    if target_col in distinct_values:
                        allowed_values = st.multiselect(
                            f"Allowed values for {target_col}",
                            options=distinct_values[target_col],
                            default=[v for v in default_values if v in distinct_values[target_col]],
                            help="Select which values define membership in this group",
                            key=f"derived_embed_values_{form_suffix}"
                        )
                    else:
                        default_text = "\n".join(str(v) for v in default_values) if default_values else ""
                        allowed_values_text = st.text_area(
                            f"Allowed values for {target_col} (one per line)",
                            value=default_text,
                            placeholder="-\nA\nB",
                            help="Enter allowed values, one per line",
                            key=f"derived_embed_values_text_{form_suffix}"
                        )
                        allowed_values = [line.strip() for line in allowed_values_text.split('\n') if line.strip()]
        
                    if allowed_values:
                        embedded_rules = {target_col: allowed_values}
                        st.caption(f"✓ Group will include materials where {target_col} is in: {', '.join(map(str, allowed_values))}")
                else:
                    st.warning("⚠️ Please select exactly one column from the dropdown above, OR type a column name manually")
    
        def _matches_target(entry_targets: list[str]) -> bool:
            if not selected_targets:
                return False  # No columns selected = no matches (explicit selection required)
            if not entry_targets:
                return "(no column/field)" in selected_targets
            return any(target in selected_targets for target in entry_targets)
    
        filtered_ids = [
            entry["id"] for entry in filtered_catalog
            if entry.get("id") and _matches_target(entry.get("targets", []))
        ]
    
        selection_label_lookup = {
            entry["id"]: entry["label"]
            for entry in filtered_catalog
            if entry.get("id") in filtered_ids
        }
    
        # Keep any pre-existing defaults visible even if the current filter would hide them
        for exp_id in default_expectation_ids:
            if exp_id and exp_id not in filtered_ids:
                filtered_ids.append(exp_id)
                selection_label_lookup.setdefault(exp_id, expectation_label_lookup.get(exp_id, exp_id))
    
        preserved_defaults = [exp_id for exp_id in default_expectation_ids if exp_id and exp_id not in filtered_ids]
        selected_expectation_ids = filtered_ids + preserved_defaults
    
        # Display clean summary only
        if selected_expectation_ids:
            st.success(f"✓ {len(selected_expectation_ids)} expectation(s) will be included in this derived status")
    
            # Group by validation type for cleaner display
            type_counts = {}
            for exp_id in selected_expectation_ids:
                entry = next((e for e in filtered_catalog if e.get("id") == exp_id), None)
                if entry:
                    val_type = entry.get("type", "unknown")
                    type_counts[val_type] = type_counts.get(val_type, 0) + 1
    
            if type_counts:
                st.caption("Breakdown by validation type:")
                for val_type, count in sorted(type_counts.items()):
                    st.caption(f"  • {val_type}: {count} expectation(s)")
        elif not expectation_catalog:
            st.info("Add validation rules to populate selectable expectation IDs.")
        else:
            st.warning("No validations match the current filters. Adjust the type or column selection.")

        # Custom Expectation ID - Only show in Advanced mode
        if is_advanced_mode:
            expectation_id = st.text_input(
                "Derived Expectation ID (auto-generated if blank)",
                value=default_expectation_id,
                help="Provide a custom identifier for the derived group. If left blank, an ID will be auto-generated from the status label. Required for conditional validations.",
                key=f"derived_expectation_id_{form_suffix}",
            )
        else:
            # Standard mode: Always auto-generate
            expectation_id = ""
    
        submit_label = "Update Derived Group" if is_editing_derived else "Add Derived Group"
        submitted = st.form_submit_button(submit_label, type="primary")
    
        if submitted:
            if not status_label:
                st.error("Please provide a status label")
            elif not selected_targets and not embedded_rules:
                st.error("Please select at least one column/field to include in this derived status, or provide embedded rules")
            elif not selected_expectation_ids and not embedded_rules:
                st.error("No expectations match your selection. Please adjust the expectation type or column selection, or enable embedded rules.")
            else:
                # Use filter-based format (columns + type) instead of pre-resolved expectation_ids
                # If using embedded rules, extract columns from the rules
                columns_to_use = selected_targets if selected_targets else list(embedded_rules.keys()) if embedded_rules else []
    
                derived_entry = {
                    "status": status_label,
                    "columns": columns_to_use,  # Store selected columns for filtering
                }
    
                # Expectation type is required for filter-based resolution
                if expectation_type and expectation_type != "(All types)":
                    derived_entry["expectation_type"] = expectation_type
    
                # Add embedded rules if provided (for conditional-only groups)
                if embedded_rules:
                    derived_entry["rules"] = embedded_rules
    
                # Auto-generate expectation_id if not provided (required for conditional_on)
                if expectation_id:
                    derived_entry["expectation_id"] = expectation_id
                else:
                    # Generate a stable ID from status label
                    safe_label = re.sub(r"[^a-z0-9]+", "_", status_label.lower()).strip("_")
                    auto_id = f"exp_derived_{safe_label}"
                    # Ensure uniqueness
                    existing_ids = {d.get("expectation_id") for d in st.session_state.derived_statuses if d.get("expectation_id")}
                    counter = 1
                    final_id = auto_id
                    while final_id in existing_ids:
                        counter += 1
                        final_id = f"{auto_id}_{counter}"
                    derived_entry["expectation_id"] = final_id
    
                if is_editing_derived:
                    st.session_state.derived_statuses[st.session_state.editing_derived_index] = derived_entry
                    st.session_state.editing_derived_index = None
                    st.success("✅ Updated derived status group")
                else:
                    st.session_state.derived_statuses.append(derived_entry)
                    st.success("✅ Added derived status group")
    
                st.rerun()
    
    st.divider()
    
    if st.session_state.derived_statuses:
        st.success(f"📋 {len(st.session_state.derived_statuses)} derived group(s) configured")
    
        for idx, derived in enumerate(st.session_state.derived_statuses):
            status_title = derived.get("status", f"Group {idx + 1}") or f"Group {idx + 1}"
            with st.expander(f"Derived Group {idx + 1}: {status_title}", expanded=False):
                # Support both new (filter-based) and old (expectation_ids) formats
                columns = derived.get("columns", [])
                expectation_type = derived.get("expectation_type")
                expectation_ids = derived.get("expectation_ids", [])  # Legacy format
    
                if columns:
                    # NEW format: Filter-based (expectation_type + columns)
                    st.markdown(f"**Expectation type:** {expectation_type or '(Any)'}")
                    st.markdown(f"**Columns:** {len(columns)} selected")
                    st.markdown(", ".join(columns[:10]) + (f", ... and {len(columns) - 10} more" if len(columns) > 10 else ""))
                elif expectation_ids:
                    # LEGACY format: Pre-resolved expectation IDs
                    st.warning("⚠️ This group uses the legacy expectation_ids format. Consider recreating it with the new column-based approach.")
                    st.markdown("**Selected validations**")
                    summary_lines = []
                    for exp_id in expectation_ids[:5]:  # Show first 5
                        label = expectation_label_lookup.get(exp_id, exp_id)
                        summary_lines.append(f"- {label}")
                    if len(expectation_ids) > 5:
                        summary_lines.append(f"- ... and {len(expectation_ids) - 5} more")
                    st.markdown("\n".join(summary_lines))
                else:
                    st.info("No filters configured for this group.")
    
                st.json(derived)
    
                col1, col2 = st.columns(2)
                with col1:
                    if st.button(f"✏️ Edit Derived {idx + 1}", key=f"edit_derived_{idx}"):
                        st.session_state.editing_derived_index = idx
                        st.rerun()
                with col2:
                    if st.button(f"🗑️ Remove Derived {idx + 1}", key=f"remove_derived_{idx}"):
                        st.session_state.derived_statuses.pop(idx)
                        if st.session_state.editing_derived_index == idx:
                            st.session_state.editing_derived_index = None
                        elif (
                            st.session_state.editing_derived_index is not None
                            and st.session_state.editing_derived_index > idx
                        ):
                            st.session_state.editing_derived_index -= 1
                        st.rerun()
    
        if st.button("🗑️ Clear All Derived Groups", key="clear_all_derived"):
            st.session_state.derived_statuses = []
            st.session_state.editing_derived_index = None
            st.rerun()
    else:
        st.info("No derived status groups defined. Use the form above to add groups.")
    
    # ----------------------------------------------------
    # Section 8: Derived Lists
    # ----------------------------------------------------
    st.header("8. Derived Lists")
    
    st.markdown("""
    **Derived Lists** allow you to define material sets based on exclusion criteria from derived statuses.
    For example, you can create a "Ready for ABP Load" list that includes materials NOT in "ENG DATA INCOMPLETE" AND NOT in "Z01 DATA INCOMPLETE".
    """)
    
    is_editing_list = st.session_state.editing_derived_list_index is not None
    
    # Get available derived status labels for the multiselect
    available_statuses = [ds.get("status") for ds in st.session_state.derived_statuses if ds.get("status")]
    
    if not available_statuses:
        st.warning("⚠️ No derived status groups configured. Please add derived status groups in Section 7 before creating derived lists.")
    else:
        # Form for creating/editing derived lists
        form_suffix_list = "edit_list" if is_editing_list else "new_list"
    
        # Pre-populate if editing
        default_list_name = ""
        default_list_description = ""
        default_exclude_statuses = []
    
        if is_editing_list:
            editing_list = st.session_state.derived_lists[st.session_state.editing_derived_list_index]
            default_list_name = editing_list.get("name", "")
            default_list_description = editing_list.get("description", "")
            default_exclude_statuses = editing_list.get("exclude_statuses", [])
    
        with st.form(f"derived_list_form_{form_suffix_list}", enter_to_submit=False):
            st.subheader("Add/Edit Derived List")
    
            list_name = st.text_input(
                "List Name",
                value=default_list_name,
                help="A descriptive name for this material list (e.g., 'Ready for ABP Load')",
                key=f"list_name_{form_suffix_list}",
            )
    
            list_description = st.text_area(
                "Description (optional)",
                value=default_list_description,
                help="Describe what this list represents",
                key=f"list_description_{form_suffix_list}",
            )
    
            exclude_statuses = st.multiselect(
                "Exclude Statuses",
                options=available_statuses,
                default=default_exclude_statuses,
                help="Materials with ANY of these statuses will be excluded from the list",
                key=f"exclude_statuses_{form_suffix_list}",
            )
    
            submit_label = "Update Derived List" if is_editing_list else "Add Derived List"
            submitted = st.form_submit_button(submit_label, type="primary")
    
            if submitted:
                if not list_name:
                    st.error("Please provide a list name")
                elif not exclude_statuses:
                    st.error("Please select at least one status to exclude")
                else:
                    list_entry = {
                        "name": list_name,
                        "exclude_statuses": exclude_statuses,
                    }
    
                    if list_description:
                        list_entry["description"] = list_description
    
                    if is_editing_list:
                        st.session_state.derived_lists[st.session_state.editing_derived_list_index] = list_entry
                        st.session_state.editing_derived_list_index = None
                        st.success("✅ Updated derived list")
                    else:
                        st.session_state.derived_lists.append(list_entry)
                        st.success("✅ Added derived list")
    
                    st.rerun()
    
    st.divider()
    
    if st.session_state.derived_lists:
        st.success(f"📋 {len(st.session_state.derived_lists)} derived list(s) configured")
    
        for idx, derived_list in enumerate(st.session_state.derived_lists):
            list_name = derived_list.get("name", f"List {idx + 1}")
            with st.expander(f"Derived List {idx + 1}: {list_name}", expanded=False):
                description = derived_list.get("description", "")
                exclude_statuses = derived_list.get("exclude_statuses", [])
    
                if description:
                    st.markdown(f"**Description:** {description}")
    
                st.markdown(f"**Excludes materials with statuses:** {', '.join(exclude_statuses)}")
                st.json(derived_list)
    
                col1, col2 = st.columns(2)
                with col1:
                    if st.button(f"✏️ Edit List {idx + 1}", key=f"edit_list_{idx}"):
                        st.session_state.editing_derived_list_index = idx
                        st.rerun()
                with col2:
                    if st.button(f"🗑️ Remove List {idx + 1}", key=f"remove_list_{idx}"):
                        st.session_state.derived_lists.pop(idx)
                        if st.session_state.editing_derived_list_index == idx:
                            st.session_state.editing_derived_list_index = None
                        elif (
                            st.session_state.editing_derived_list_index is not None
                            and st.session_state.editing_derived_list_index > idx
                        ):
                            st.session_state.editing_derived_list_index -= 1
                        st.rerun()
    
        if st.button("🗑️ Clear All Derived Lists", key="clear_all_lists"):
            st.session_state.derived_lists = []
            st.session_state.editing_derived_list_index = None
            st.rerun()
    else:
        st.info("No derived lists defined. Use the form above to add lists.")

# ====================================================
# TAB 4: PREVIEW & SAVE
# ====================================================
with tab4:
    # Workflow Guidance for Preview & Save
    with st.expander("📚 How to Preview & Save", expanded=False):
        st.markdown("""
        **Preview & Save Workflow:**

        1. **Review YAML**: Check the generated YAML structure before saving
           - Verify metadata is correct
           - Confirm all validations are present
           - Check derived groups and lists
        2. **Save Suite**: Click "Save Suite" to write the YAML file
        3. **No Python Generation Needed**: Validations run directly from YAML

        💡 **Best Practices**:
        - Review the YAML preview before saving to catch any issues
        - Use meaningful suite names (they become filenames)
        - Save frequently when making complex changes
        - The suite can be edited again later from the Edit mode
        """)

    # ----------------------------------------------------
    # Section 9: YAML Preview & Save
    # ----------------------------------------------------
    st.header("9. YAML Preview & Save")

    if st.session_state.suite_metadata["suite_name"]:
        # Generate YAML preview
        yaml_content = yaml.dump({
            "metadata": st.session_state.suite_metadata,
            "data_source": st.session_state.data_source,
            "validations": st.session_state.validations,
            "derived_statuses": st.session_state.derived_statuses,
            "derived_lists": st.session_state.derived_lists,
        }, default_flow_style=False, sort_keys=False, allow_unicode=True)
    
        st.code(yaml_content, language="yaml")
    
        # Save button
        col1, col2 = st.columns([3, 1])
    
        with col1:
            if st.button("💾 Save Suite (YAML + Generate Python)", type="primary", key="save_suite"):
                if save_yaml_and_generate_python(
                    st.session_state.suite_metadata,
                    st.session_state.validations,
                    st.session_state.data_source,
                    st.session_state.derived_statuses,
                    st.session_state.derived_lists,
                ):
                    st.balloons()
                    st.info("🎉 Suite saved successfully! You can now use it in validations.")
    
        with col2:
            if st.button("🔄 Clear Form", key="clear_form"):
                st.session_state.suite_metadata = {
                    "suite_name": "",
                    "index_column": "MATERIAL_NUMBER",
                    "description": "",
                    "data_source": "get_level_1_dataframe"
                }
                st.session_state.validations = []
                st.session_state.derived_statuses = []
                st.session_state.derived_lists = []
                st.session_state.data_source = {
                    "table": DEFAULT_TABLE,
                    "filters": {},
                    "distinct": False
                }
                st.session_state.editing_derived_index = None
                st.session_state.editing_derived_list_index = None
                st.rerun()
    
    else:
        st.warning("⚠️ Please enter a suite name in Section 3 to preview and save")
