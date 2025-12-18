"""
Dynamic Validation Report - Unified page for all validation suites.

This page dynamically loads and displays any validation suite based on sidebar selection.
Replaces individual suite-specific pages (Aurora.py, level_1_report.py, etc.).
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from datetime import date

from validations.snowflake_runner import run_validation_from_yaml_snowflake, run_validation_simple
from core.cache_manager import get_cached_results, save_cached_results, clear_cache, get_cached_failures_csv, save_cached_failures_csv, save_daily_suite_artifacts
from core.config import ensure_snowflake_config, snowflake_config_summary
from core.validation_metrics import calculate_validation_metrics, get_failed_materials, get_summary_table
from core.expectation_metadata import lookup_expectation_metadata
from validations.base_validation import BaseValidationSuite
from app.components.drill_down import render_expectation_drill_down
from app.suite_discovery import discover_suites, get_suite_by_name
from io import StringIO

# ----------------------------------------------------------
# Page setup
# ----------------------------------------------------------
st.set_page_config(page_title="Validation Report", layout="wide")

# Ensure Snowflake configuration is valid
ensure_snowflake_config()

# ----------------------------------------------------------
# Suite discovery and selection
# ----------------------------------------------------------
# Discover available suites from validation_yaml/
available_suites = discover_suites()

if not available_suites:
    st.error("No validation suites found in validation_yaml/")
    st.info("Create a YAML file in validation_yaml/ to get started")
    st.stop()

# Create suite selector in sidebar
with st.sidebar:
    st.subheader("Validation Suite")
    suite_names = [s["suite_name"] for s in available_suites]

    # Default to first suite or restore from session state
    default_index = 0
    if "selected_suite_name" in st.session_state:
        try:
            default_index = suite_names.index(st.session_state["selected_suite_name"])
        except ValueError:
            pass

    selected_suite_name = st.selectbox(
        "Select Suite",
        options=suite_names,
        index=default_index,
        key="suite_selector",
        label_visibility="collapsed"
    )

    # Store selection in session state
    st.session_state["selected_suite_name"] = selected_suite_name

    # Get selected suite config
    suite_config = get_suite_by_name(selected_suite_name, available_suites)

# ----------------------------------------------------------
# Page title and description
# ----------------------------------------------------------
st.title(suite_config["suite_name"].replace("_", " "))
if suite_config["description"]:
    st.caption(suite_config["description"])

# Show active Snowflake connection parameters so missing/incorrect settings are obvious
try:
    ensure_snowflake_config()
    with st.expander("Snowflake connection (externalbrowser)", expanded=False):
        st.write({k: v for k, v in snowflake_config_summary().items()})
except RuntimeError as e:
    st.error(str(e))
    st.stop()

# ----------------------------------------------------------
# Adapter function for backward compatibility
# ----------------------------------------------------------
def _adapt_simple_to_legacy_format(simple_payload, yaml_path):
    """
    Convert simplified validation payload to legacy format for UI compatibility.

    This adapter allows us to use run_validation_simple() while maintaining
    compatibility with existing UI components that expect the old format.

    Args:
        simple_payload: Output from run_validation_simple()
        yaml_path: Path to YAML file for metadata lookup

    TODO: Progressively remove this adapter as UI components are updated.
    """
    df = simple_payload["df"]
    metrics = simple_payload["metrics"]
    suite_config = simple_payload["suite_config"]

    # Extract expectation and derived columns
    exp_columns = [col for col in df.columns if col.startswith("exp_")]
    derived_columns = [col for col in df.columns if col.startswith("derived_")]

    # Get index column from suite config
    index_column = suite_config.get("metadata", {}).get("index_column", "material_number").lower()

    # Load YAML to get expected values for each validation
    import yaml
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)

    # Build a lookup of expectation_id -> validation config
    validation_lookup = {}
    for validation in yaml_config.get('validations', []):
        # This will be populated by sql_generator with expectation_id
        # For now, we'll need to match by type and column
        validation_lookup[validation.get('type', '')] = validation

    # Build legacy results list
    results = []
    for exp_col in exp_columns:
        exp_metrics = metrics["expectation_metrics"].get(exp_col, {})

        # Get failed materials for this expectation
        failed_df = get_failed_materials(df, exp_id=exp_col, index_column=index_column)

        # Look up expectation metadata from YAML
        metadata = lookup_expectation_metadata(exp_col, yaml_path)

        if metadata:
            expectation_type = metadata.get("expectation_type", exp_col)
            column = metadata.get("column", exp_col)
        else:
            # Fallback if lookup fails
            expectation_type = exp_col
            column = exp_col

        # Get expected values from YAML validation config
        expected = None
        validation_config = validation_lookup.get(expectation_type)
        if validation_config:
            # Extract expected values based on validation type
            if expectation_type == 'expect_column_values_to_be_in_set':
                # For be_in_set, expected is in rules[column]
                rules = validation_config.get('rules', {})
                expected = rules.get(column, rules.get(column.upper()))
            elif 'value_set' in validation_config:
                expected = validation_config.get('value_set')
            elif 'expected_value' in validation_config:
                expected = validation_config.get('expected_value')

        # Build failed_materials list by extracting actual unexpected values
        # from the source column being validated
        failed_materials = []
        source_column = column.lower()  # Column being validated

        for _, row in failed_df.iterrows():
            material_number = row.get(index_column, "")

            # Extract the unexpected value from the source column
            # Try both lowercase and uppercase versions
            unexpected_value = row.get(source_column, row.get(source_column.upper(), ""))

            failed_materials.append({
                "material_number": material_number,
                "MATERIAL_NUMBER": str(material_number).upper() if material_number else "",
                "Unexpected Value": unexpected_value,
            })

        results.append({
            "expectation_id": exp_col,
            "expectation_type": expectation_type,
            "column": column,
            "element_count": exp_metrics.get("total", 0),
            "unexpected_count": exp_metrics.get("failures", 0),
            "unexpected_percent": 100 - exp_metrics.get("pass_rate", 100),
            "table_grain": suite_config.get("metadata", {}).get("table_grain", "MATERIAL"),
            "unique_by": suite_config.get("metadata", {}).get("unique_by", ["MATERIAL_NUMBER"]),
            "failed_materials": failed_materials,
            "expected": expected,  # Now included!
        })

    # Build legacy derived_status_results list
    derived_status_results = []

    # Get derived status definitions from YAML to know which expectations they aggregate
    derived_statuses_config = yaml_config.get("derived_statuses", [])

    for derived_col in derived_columns:
        derived_metrics = metrics["derived_metrics"].get(derived_col, {})

        # Get failed materials for this derived status
        failed_df = get_failed_materials(df, derived_id=derived_col, index_column=index_column)

        # Extract status label (remove "derived_" prefix)
        status_label = derived_col.replace("derived_", "").replace("_", " ").title()

        # Find the corresponding derived status config to get constituent expectations
        derived_config = None
        for ds_config in derived_statuses_config:
            if ds_config.get("name", "").lower().replace(" ", "_") == derived_col.replace("derived_", ""):
                derived_config = ds_config
                break

        # Get list of constituent expectation IDs from config
        constituent_exp_ids = derived_config.get("expectation_ids", []) if derived_config else []

        # Build failed materials list with detailed failure information
        failed_materials = []
        for _, row in failed_df.iterrows():
            material_number = row.get(index_column, "")

            # Analyze which constituent expectations failed for this material
            failed_expectations = []
            failed_columns = set()

            for exp_id in constituent_exp_ids:
                # Check if this expectation column exists and failed for this material
                if exp_id in row.index and row[exp_id] == 'FAIL':
                    failed_expectations.append(exp_id)

                    # Look up which column this expectation validates
                    exp_metadata = lookup_expectation_metadata(exp_id, yaml_path)
                    if exp_metadata:
                        failed_columns.add(exp_metadata.get("column", ""))

            failed_materials.append({
                "MATERIAL_NUMBER": str(material_number).upper() if material_number else "",
                "failed_columns": list(failed_columns),
                "failure_count": len(failed_expectations),
                "failed_expectations": failed_expectations
            })

        derived_status_results.append({
            "status_label": status_label,
            "expectation_id": derived_col,
            "expectation_type": "Derived Status",
            "unexpected_count": derived_metrics.get("failures", 0),
            "unexpected_percent": 100 - derived_metrics.get("pass_rate", 100),
            "context_columns": [index_column.upper()],
            "failed_materials": failed_materials,
        })

    # Get list of validated materials
    if index_column in df.columns:
        validated_materials = df[index_column].unique().tolist()
    else:
        validated_materials = []

    return {
        "results": results,
        "derived_status_results": derived_status_results,
        "validated_materials": validated_materials,
        "total_validated_count": metrics.get("total_materials", len(validated_materials)),
        "full_results_df": df,
    }

# ----------------------------------------------------------
# Load or run validation
# ----------------------------------------------------------
def load_or_run_validation(suite_config):
    """Load cached results or run validation if needed."""
    suite_key = suite_config["suite_key"]
    session_key = f"{suite_key}_results"
    session_materials_key = f"{suite_key}_validated_materials"
    session_df_key = f"{suite_key}_full_results_df"
    session_failures_csv_key = f"{suite_key}_failures_df"
    session_raw_results_key = f"{suite_key}_raw_results_csv"
    session_date_key = f"{suite_key}_data_date"
    session_derived_key = f"{suite_key}_derived_status_results"
    today = date.today().isoformat()

    print(f"📦 DEBUG: load_or_run_validation called for suite_key={suite_key}", flush=True)

    # Check session state first (fastest) - but verify it's from today
    if session_key in st.session_state and st.session_state.get(session_key):
        cached_date = st.session_state.get(session_date_key, "")
        print(f"📦 DEBUG: Found session state, cached_date={cached_date}, today={today}", flush=True)
        if cached_date == today:
            print(f"✅ Using session state results for {suite_key} (from today)", flush=True)
            return {
                "results": st.session_state[session_key],
                "derived_status_results": st.session_state.get(session_derived_key, []),
                "validated_materials": st.session_state.get(session_materials_key, []),
                "full_results_df": st.session_state.get(session_df_key),
            }
        else:
            print(f"⚠️ Session state for {suite_key} is stale (from {cached_date}), clearing...", flush=True)
            st.session_state.pop(session_key, None)
            st.session_state.pop(session_materials_key, None)
            st.session_state.pop(session_df_key, None)
            st.session_state.pop(session_failures_csv_key, None)
            st.session_state.pop(session_raw_results_key, None)
            st.session_state.pop(session_date_key, None)
            st.session_state.pop(session_derived_key, None)
    else:
        print(f"📦 DEBUG: No session state found for {session_key}", flush=True)

    # Check daily file cache
    print(f"📦 DEBUG: Checking file cache for suite_key={suite_key}", flush=True)
    cached = get_cached_results(suite_key)
    print(f"📦 DEBUG: get_cached_results returned: {cached is not None}", flush=True)
    if cached:
        with st.spinner("Loading cached results..."):
            print(f"✅ Using file cache for {suite_key}", flush=True)
            st.session_state[session_key] = cached["results"]
            st.session_state[session_derived_key] = cached.get("derived_status_results", [])
            st.session_state[session_materials_key] = cached.get("validated_materials", [])
            st.session_state[session_date_key] = today

            # Also load the failures CSV from file cache if available
            file_cached_csv = get_cached_failures_csv(suite_key)
            if file_cached_csv:
                st.session_state[session_failures_csv_key] = file_cached_csv
                print(f"✅ Loaded failures CSV from file cache for {suite_key}", flush=True)
        return cached

    # No cache - run validation from YAML
    print(f"📦 DEBUG: No valid cache found for {suite_key}, running fresh validation...")
    placeholder = st.empty()
    with placeholder.container():
        st.markdown(
            f"""
            <div style="text-align:center; padding-top: 100px;">
                <h3>Calculating Validation Results. Please Wait.</h3>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.spinner(f"Running {suite_config['suite_name']} validation..."):
            # Use simplified validation approach
            simple_payload = run_validation_simple(
                suite_config["yaml_path"]
            )

            # Convert to legacy format for UI compatibility
            payload = _adapt_simple_to_legacy_format(simple_payload, suite_config["yaml_path"])

            results = payload.get("results", []) if isinstance(payload, dict) else payload
            derived_status_results = payload.get("derived_status_results", []) if isinstance(payload, dict) else []
            validated_materials = payload.get("validated_materials", []) if isinstance(payload, dict) else []
            total_validated_count = payload.get("total_validated_count", 0) if isinstance(payload, dict) else 0
            full_results_df = payload.get("full_results_df") if isinstance(payload, dict) else None

            # Use total_validated_count if available (more accurate), otherwise fall back to list length
            actual_total = total_validated_count if total_validated_count > 0 else len(validated_materials)

            print(f"📦 DEBUG: Validation returned {len(results) if results else 0} results", flush=True)
            print(f"📦 DEBUG: Validation returned {len(derived_status_results) if derived_status_results else 0} derived status results", flush=True)
            print(f"📦 DEBUG: Validation processed {actual_total} materials", flush=True)

            if results is not None:
                # Save to both session state and file cache
                print(f"📦 DEBUG: Saving to session state key={session_key}", flush=True)
                st.session_state[session_key] = results
                st.session_state[session_derived_key] = derived_status_results
                st.session_state[session_materials_key] = validated_materials
                st.session_state[session_df_key] = full_results_df
                st.session_state[session_date_key] = today
                print(f"📦 DEBUG: Calling save_cached_results for suite_key={suite_key}", flush=True)
                save_cached_results(suite_key, results, validated_materials, derived_status_results)
                if suite_key == "abb_shop_abp_data_presence":
                    save_daily_suite_artifacts(
                        suite_key,
                        results,
                        validated_materials,
                        full_results_df,
                        today,
                        derived_status_results,
                    )
                print(f"✅ Fresh validation completed and cached for {suite_key}", flush=True)
            else:
                print(f"⚠️ Validation returned None for {suite_key}", flush=True)
    placeholder.empty()
    return {
        "results": results,
        "derived_status_results": derived_status_results,
        "validated_materials": validated_materials,
        "full_results_df": full_results_df
    }


# Handle cache clear request
with st.sidebar:
    if st.button("🔄 Re-run Validation Suite", key=f"{suite_config['suite_key']}_clear_cache"):
        clear_cache(suite_config["suite_key"])
        st.session_state.pop(f"{suite_config['suite_key']}_results", None)
        st.session_state.pop(f"{suite_config['suite_key']}_derived_status_results", None)
        st.session_state.pop(f"{suite_config['suite_key']}_validated_materials", None)
        st.session_state.pop(f"{suite_config['suite_key']}_full_results_df", None)
        st.session_state.pop(f"{suite_config['suite_key']}_failures_df", None)
        st.session_state.pop(f"{suite_config['suite_key']}_raw_results_csv", None)
        st.session_state.pop(f"{suite_config['suite_key']}_data_date", None)
        print(f"🗑️ Cleared all caches for {suite_config['suite_key']}")
        st.rerun()

# Load validation results with a final safety net so any connection/runtime
# errors still surface as friendly UI messages instead of stack traces.
try:
    payload = load_or_run_validation(suite_config)
except RuntimeError as e:
    st.error(str(e))
    st.info(
        "Tip: If you recently switched SSO users, sign out of the IdP or use an "
        "incognito window so externalbrowser opens the correct account."
    )
    st.stop()
except Exception as e:
    st.error(f"❌ Validation failed: {e}")
    st.stop()

# Extract results and metadata
if isinstance(payload, dict):
    results = payload.get("results", [])
    derived_status_results = payload.get("derived_status_results", [])
    validated_materials = payload.get("validated_materials", [])
    total_validated_count = payload.get("total_validated_count", 0)
    full_results_df = payload.get("full_results_df")
else:
    results = payload
    derived_status_results = []
    validated_materials = []
    total_validated_count = 0
    full_results_df = None

# Use total_validated_count if available (more accurate), otherwise fall back to list length
actual_total = total_validated_count if total_validated_count > 0 else len(validated_materials)

# DEBUG: Log what we extracted
print(f"📊 DEBUG: Extracted results type={type(results)}, len={len(results) if results else 0}", flush=True)
print(f"📊 DEBUG: Extracted derived_status_results len={len(derived_status_results)}", flush=True)
print(f"📊 DEBUG: Extracted validated_materials len={len(validated_materials)}, actual_total={actual_total}", flush=True)

# ----------------------------------------------------------
# Handle validation failure
# ----------------------------------------------------------
if results is None:
    st.error("❌ Validation failed or returned no results.")
    st.info("This usually means:")
    st.markdown("""
    - Snowflake connection failed (check credentials/network)
    - No data returned from query
    - Validation suite encountered an error

    Check container logs with: `docker compose logs`
    """)
    st.stop()

# ----------------------------------------------------------
# Build or reuse DataFrame of failures (cached raw Snowflake results as CSV)
# ----------------------------------------------------------
suite_key = suite_config["suite_key"]
session_failures_csv_key = f"{suite_key}_failures_df"
session_raw_results_key = f"{suite_key}_raw_results_csv"
session_date_key = f"{suite_key}_data_date"
today = date.today().isoformat()

cached_raw_csv = st.session_state.get(session_raw_results_key)
cached_failures_csv = st.session_state.get(session_failures_csv_key)
cached_date = st.session_state.get(session_date_key)

raw_results_df = None
df = None

if cached_raw_csv and cached_date == today:
    print(
        f"📊 DEBUG: Using cached raw Snowflake results for {suite_key} from session state",
        flush=True,
    )
    raw_results_df = pd.read_csv(StringIO(cached_raw_csv))
elif cached_date == today:
    file_cached_raw = get_cached_failures_csv(suite_key)
    if file_cached_raw:
        print(
            f"📊 DEBUG: Hydrating raw Snowflake results for {suite_key} from file cache",
            flush=True,
        )
        raw_results_df = pd.read_csv(StringIO(file_cached_raw))
        st.session_state[session_raw_results_key] = file_cached_raw

if raw_results_df is None and isinstance(full_results_df, pd.DataFrame):
    raw_results_df = full_results_df

if raw_results_df is not None:
    csv_payload = raw_results_df.to_csv(index=False)
    st.session_state[session_raw_results_key] = csv_payload
    st.session_state[session_date_key] = today
    save_cached_failures_csv(suite_key, raw_results_df)
else:
    print("⚠️ No raw Snowflake results available to cache", flush=True)

if cached_failures_csv and cached_date == today:
    print(
        f"📊 DEBUG: Using cached failures DataFrame for {suite_key} from session state",
        flush=True,
    )
    df = pd.read_csv(StringIO(cached_failures_csv))

if df is None:
    print(f"📊 DEBUG: Calling results_to_dataframe with {len(results)} results", flush=True)
    df = BaseValidationSuite.results_to_dataframe(results, raw_results_df)
    print(f"📊 DEBUG: DataFrame created with {len(df)} rows", flush=True)
    failures_csv_payload = df.to_csv(index=False)
    st.session_state[session_failures_csv_key] = failures_csv_payload

if not df.empty:
    print(f"📊 DEBUG: DataFrame columns: {list(df.columns)}", flush=True)

# ----------------------------------------------------------
# View Selection (persists across reruns via key)
# ----------------------------------------------------------
view = st.segmented_control(
    "View",
    ["Overview", "Details"],
    default="Overview",
    key=f"{suite_config['suite_key']}_view_selection",
    label_visibility="collapsed",
)

st.divider()

# ----------------------------------------------------------
# Helper functions for overview metrics
# ----------------------------------------------------------
def calc_overall_kpis(df, validated_materials_count=0):
    """
    Calculate overall pass/fail metrics.

    IMPORTANT: Always use validated_materials_count (unique materials) as the total,
    NOT element_count (total rows). Many datasets have multiple rows per material
    (e.g., different plants, storage locations), so element_count inflates metrics.
    """
    # Prefer validated_materials_count (unique materials validated)
    total = validated_materials_count

    # Fall back to element_count only if validated_materials_count is unavailable
    # (backward compatibility with older cached results)
    if total == 0 and not df.empty and "Element Count" in df.columns:
        max_count = df["Element Count"].max()
        total = int(max_count) if pd.notna(max_count) else 0
        # Warn that metrics may be inaccurate due to mixing rows and unique materials
        if total > 0:
            print(
                "⚠️ WARNING: Using element_count for total (rows, not unique materials). "
                "Metrics may be inflated. Clear cache and re-run validation for accurate metrics.",
                flush=True
            )

    failed = df["Material Number"].dropna().nunique() if not df.empty else 0
    passed = max(total - failed, 0)
    fail_rate = (failed / total * 100) if total > 0 else 0
    pass_rate = 100 - fail_rate
    return total, passed, failed, pass_rate, fail_rate


def calc_column_fail_counts(df):
    """Calculate failure counts per column."""
    return (
        df.groupby("Column")["Material Number"]
        .nunique()
        .reset_index(name="Failed Materials")
        .sort_values("Failed Materials", ascending=False)
    )


# =====================================================
# OVERVIEW VIEW
# =====================================================
if view == "Overview":
    st.subheader("Validation Summary")

    total, passed, failed, pass_rate, fail_rate = calc_overall_kpis(df, actual_total)

    # =====================================================
    # METRICS ROW - Enhanced with color coding
    # =====================================================
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Total Materials",
            f"{total:,}",
            help="Total number of materials validated"
        )

    with col2:
        st.metric(
            "Materials Passing",
            f"{passed:,}",
            delta=f"{pass_rate:.1f}%",
            delta_color="normal",
            help="Materials with zero validation failures"
        )

    with col3:
        st.metric(
            "Materials Failing",
            f"{failed:,}",
            delta=f"-{fail_rate:.1f}%",
            delta_color="inverse",
            help="Materials with one or more validation failures"
        )

    with col4:
        # Pass rate with visual indicator
        status_color = "🟢" if pass_rate >= 95 else "🟡" if pass_rate >= 85 else "🔴"
        st.metric(
            "Pass Rate",
            f"{status_color} {pass_rate:.1f}%",
            help="Percentage of materials passing all validations"
        )

    st.divider()

    # =====================================================
    # DERIVED STATUS SUMMARY
    # =====================================================
    derived_status_rows = []
    for result in derived_status_results or []:
        status_label = result.get("status_label")
        if not status_label:
            continue

        derived_status_rows.append({
            "Status": status_label,
            "Expectation ID": result.get("expectation_id") or status_label,
            "Expectation Type": result.get("expectation_type") or "Derived Status",
            "Failed Materials": result.get("unexpected_count", 0),
            "Failure %": result.get("unexpected_percent", 0.0),
            "Context": ", ".join(result.get("context_columns") or []),
        })

    st.divider()
    with st.expander("Derived Statuses", expanded=False):
        st.caption(
            "Derived statuses synthesize multiple expectations into a single flag. "
            "This view shows any derived groups that triggered during the run."
        )

        if derived_status_rows:
            derived_df = pd.DataFrame(derived_status_rows)
            derived_df = derived_df[[
                "Status",
                "Expectation ID",
                "Expectation Type",
                "Failed Materials",
                "Failure %",
                "Context",
            ]]

            st.dataframe(
                derived_df,
                hide_index=True,
                use_container_width=True,
            )

            # Show detailed failure breakdown for each derived status
            st.divider()
            st.caption("**Detailed Failure Breakdown**")

            for result in derived_status_results or []:
                status_label = result.get("status_label")
                if not status_label:
                    continue

                failed_materials = result.get("failed_materials", [])
                if not failed_materials:
                    continue

                with st.expander(f"📋 {status_label} - {len(failed_materials)} materials", expanded=False):
                    st.caption(
                        f"Materials that failed at least one expectation in the '{status_label}' group. "
                        "Sorted by number of failures (most issues first)."
                    )

                    # Build a cleaner display dataframe
                    detail_rows = []
                    for failed_material in failed_materials:
                        # Extract context columns (material number, etc.)
                        context_cols = result.get("context_columns", [])
                        row_data = {}

                        for col in context_cols:
                            row_data[col] = failed_material.get(col, "")

                        # Add the new tracking fields
                        row_data["Failed Columns"] = ", ".join(failed_material.get("failed_columns", []))
                        row_data["# Failures"] = failed_material.get("failure_count", 0)

                        # Optionally show expectation IDs (can be verbose)
                        if st.session_state.get("show_expectation_ids", False):
                            row_data["Failed Expectations"] = ", ".join(failed_material.get("failed_expectations", []))

                        detail_rows.append(row_data)

                    if detail_rows:
                        detail_df = pd.DataFrame(detail_rows)
                        st.dataframe(
                            detail_df,
                            hide_index=True,
                            use_container_width=True,
                            height=min(400, len(detail_rows) * 35 + 38),
                        )

                        # Download option
                        csv = detail_df.to_csv(index=False)
                        st.download_button(
                            label=f"⬇️ Download {status_label} failures as CSV",
                            data=csv,
                            file_name=f"{status_label.replace(' ', '_').lower()}_failures.csv",
                            mime="text/csv",
                        )

        else:
            st.info("No derived statuses were triggered for this validation run.")

    # =====================================================
    # DERIVED LISTS - Materials filtered by status exclusion
    # =====================================================
    # Load YAML to get derived_lists configuration
    import yaml
    with open(suite_config["yaml_path"], 'r') as f:
        yaml_config = yaml.safe_load(f)

    derived_lists_config = yaml_config.get("derived_lists", [])

    if derived_lists_config:
        st.divider()
        with st.expander("Derived Lists", expanded=False):
            st.caption(
                "Derived lists identify materials based on derived status membership. "
                "These lists can be downloaded for further processing."
            )

            # Build a map of status_label -> failed materials for quick lookup
            status_to_materials = {}
            for result in derived_status_results or []:
                status_label = result.get("status_label")
                if not status_label:
                    continue

                # Collect all failed material numbers from this status
                failed_materials = result.get("failed_materials", [])
                material_numbers = set()
                for fm in failed_materials:
                    # Get material number from context columns
                    mat_num = fm.get("MATERIAL_NUMBER") or fm.get("Material Number") or fm.get("material_number")
                    if mat_num:
                        material_numbers.add(str(mat_num))

                status_to_materials[status_label] = material_numbers

            # Calculate and display each derived list
            for derived_list in derived_lists_config:
                list_name = derived_list.get("name", "Unnamed List")
                description = derived_list.get("description", "")
                exclude_statuses = derived_list.get("exclude_statuses", [])

                # Calculate materials in this list (all materials minus excluded)
                # Use validated_materials (contains all materials since we return all rows)
                all_materials_list = payload.get("validated_materials", []) if isinstance(payload, dict) else []
                all_material_numbers = set(str(m) for m in all_materials_list) if all_materials_list else set()

                # If we don't have the materials list, we can't calculate derived lists
                if len(all_material_numbers) == 0:
                    st.warning(f"⚠️ Cannot calculate '{list_name}' - material list unavailable")
                    continue

                # Collect all materials to exclude
                excluded_materials = set()
                for status_label in exclude_statuses:
                    if status_label in status_to_materials:
                        excluded_materials.update(status_to_materials[status_label])

                # Materials in this list = all materials - excluded materials
                list_materials = all_material_numbers - excluded_materials

                # Display the list
                st.write(f"**{list_name}**")
                if description:
                    st.caption(description)

                col1, col2 = st.columns([2, 1])
                with col1:
                    st.metric(
                        "Materials in List",
                        f"{len(list_materials):,}",
                        delta=f"{len(list_materials) / actual_total * 100:.1f}% of total" if actual_total > 0 else None
                    )

                with col2:
                    if list_materials:
                        # Generate CSV with just material numbers
                        csv_content = "MATERIAL_NUMBER\n" + "\n".join(sorted(list_materials))
                        st.download_button(
                            label=f"⬇️ Download CSV ({len(list_materials):,} materials)",
                            data=csv_content,
                            file_name=f"{list_name.replace(' ', '_').lower()}.csv",
                            mime="text/csv",
                            key=f"download_list_{list_name.replace(' ', '_')}"
                        )
                    else:
                        st.info("No materials in this list")

                # Show which statuses were excluded
                if exclude_statuses:
                    with st.expander(f"Excluded Statuses ({len(exclude_statuses)})", expanded=False):
                        for status in exclude_statuses:
                            excluded_count = len(status_to_materials.get(status, []))
                            st.write(f"- {status}: {excluded_count:,} materials")

                st.divider()

    # =====================================================
    # CHARTS ROW - Plotly visualizations
    # =====================================================
    col_left, col_right = st.columns([1, 2])

    # --- DONUT CHART ---
    with col_left:
        st.write("**Pass/Fail Distribution**")

        if passed >= 0 and failed >= 0 and (passed + failed) > 0:
            # Modern donut chart with Plotly (dark mode compatible)
            fig_pie = go.Figure(data=[go.Pie(
                labels=["Passing", "Failing"],
                values=[passed, failed],
                hole=0.4,  # Donut style
                marker=dict(
                    colors=["#10b981", "#ef4444"],  # Green and red
                    line=dict(color='rgba(255,255,255,0.2)', width=2)
                ),
                textinfo='label+percent',
                textfont_size=13,
                hovertemplate="<b>%{label}</b><br>" +
                              "Materials: %{value:,}<br>" +
                              "Percentage: %{percent}<br>" +
                              "<extra></extra>",
            )])

            fig_pie.update_layout(
                height=350,
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.15,
                    xanchor="center",
                    x=0.5
                ),
                margin=dict(t=30, b=30, l=30, r=30),
                paper_bgcolor='rgba(0,0,0,0)',  # Transparent background for dark mode
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(size=12),
                annotations=[dict(
                    text=f"{total:,}<br>Total",
                    x=0.5, y=0.5,
                    font_size=16,
                    showarrow=False
                )]
            )

            st.plotly_chart(fig_pie, use_container_width=True, key="pie_chart")
        else:
            st.info("Run a validation to see distribution.")

    # --- BAR CHART ---
    with col_right:
        st.write("**Top Failing Columns**")

        failed_expect_counts = calc_column_fail_counts(df)

        if not failed_expect_counts.empty:
            top_n = 15  # Show more with Plotly's better space usage
            chart_data = failed_expect_counts.head(top_n)

            # Horizontal bar chart with gradient coloring (dark mode compatible)
            fig_bar = go.Figure(data=[go.Bar(
                y=chart_data["Column"],
                x=chart_data["Failed Materials"],
                orientation='h',
                marker=dict(
                    color=chart_data["Failed Materials"],
                    colorscale=[
                        [0, "#fbbf24"],      # Yellow (low failures)
                        [0.5, "#f97316"],    # Orange (medium)
                        [1, "#ef4444"]       # Red (high failures)
                    ],
                    line=dict(color='rgba(255,255,255,0.1)', width=1),
                    showscale=False
                ),
                text=chart_data["Failed Materials"],
                textposition='outside',
                textfont=dict(size=11),
                hovertemplate="<b>%{y}</b><br>" +
                              "Failed Materials: %{x:,}<br>" +
                              "<extra></extra>",
            )])

            fig_bar.update_layout(
                height=500,
                xaxis_title="Number of Failed Materials",
                yaxis_title=None,
                yaxis={'categoryorder':'total ascending'},
                margin=dict(t=30, b=30, l=10, r=60),
                paper_bgcolor='rgba(0,0,0,0)',  # Transparent for dark mode
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(128,128,128,0.2)',
                    zeroline=True
                ),
                font=dict(size=11)
            )

            st.plotly_chart(fig_bar, use_container_width=True, key="bar_chart")
        else:
            st.success("✅ No failing columns detected!")

    # =====================================================
    # ADDITIONAL INSIGHTS - Rule Type Breakdown
    # =====================================================
    if not df.empty and "Expectation Type" in df.columns:
        st.divider()
        st.write("**Failure Breakdown by Rule Type**")

        expectation_counts = (
            df.groupby("Expectation Type")["Material Number"]
            .nunique()
            .reset_index(name="Failed Materials")
            .sort_values("Failed Materials", ascending=False)
        )

        if not expectation_counts.empty:
            # Compact bar chart (dark mode compatible)
            fig_exp = px.bar(
                expectation_counts.head(10),
                x="Expectation Type",
                y="Failed Materials",
                color="Failed Materials",
                color_continuous_scale=["#3b82f6", "#8b5cf6"],  # Blue to purple
                text="Failed Materials",
                labels={"Expectation Type": "Rule Type"}
            )

            fig_exp.update_layout(
                height=300,
                showlegend=False,
                xaxis_tickangle=-45,
                margin=dict(t=30, b=100, l=30, r=30),
                paper_bgcolor='rgba(0,0,0,0)',  # Transparent for dark mode
                plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(
                    showgrid=True,
                    gridcolor='rgba(128,128,128,0.2)'
                ),
                font=dict(size=11)
            )

            fig_exp.update_traces(textposition='outside')

            st.plotly_chart(fig_exp, use_container_width=True, key="expectation_chart")

# =====================================================
# DETAILS VIEW
# =====================================================
elif view == "Details":
    st.subheader("Drill-down by Expectation")

    render_expectation_drill_down(
        results=results,
        df=df,
        suite_name=suite_config["suite_name"],
        cache_suite_name=suite_config["suite_key"],
        show_expected_values=True,
    )
