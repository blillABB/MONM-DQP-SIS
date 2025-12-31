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

from validations.snowflake_runner import run_validation_simple
from core.cache_manager import get_cached_results, save_cached_results, clear_cache, get_cached_failures_csv, save_cached_failures_csv, save_daily_suite_artifacts
from core.config import ensure_snowflake_config, snowflake_config_summary
from core.validation_metrics import calculate_validation_metrics, get_failed_materials, get_summary_table
from core.expectation_metadata import lookup_expectation_metadata
from app.components.columnar_drill_down import render_columnar_drill_down, render_derived_status_drill_down
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
            return st.session_state[session_key]
        else:
            print(f"⚠️ Session state for {suite_key} is stale (from {cached_date}), clearing...", flush=True)
            st.session_state.pop(session_key, None)
            st.session_state.pop(session_date_key, None)
    else:
        print(f"📦 DEBUG: No session state found for {session_key}", flush=True)

    # File caching temporarily disabled - will be updated to support columnar format
    # TODO: Update file cache to store simple payload format

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
            # Run simplified validation
            payload = run_validation_simple(
                suite_config["yaml_path"]
            )

            # Extract components from simple payload
            df = payload.get("df")
            metrics = payload.get("metrics", {})
            suite_name = payload.get("suite_name", "")

            # Get index column
            index_column = payload.get("suite_config", {}).get("metadata", {}).get("index_column", "material_number").lower()

            # Get validated materials count
            total_validated_count = metrics.get("total_materials", 0)
            actual_total = total_validated_count

            print(f"📦 DEBUG: Validation completed with {df.shape[0] if df is not None else 0} rows", flush=True)
            print(f"📦 DEBUG: Validation processed {actual_total} materials", flush=True)

            if df is not None:
                # Save to both session state and file cache
                print(f"📦 DEBUG: Saving to session state key={session_key}", flush=True)
                st.session_state[session_key] = payload
                st.session_state[session_date_key] = today
                # Note: File caching not yet updated for simple format
                print(f"✅ Fresh validation completed and cached for {suite_key}", flush=True)
            else:
                print(f"⚠️ Validation returned None for {suite_key}", flush=True)
    placeholder.empty()
    return payload


# Handle cache clear request
with st.sidebar:
    if st.button("🔄 Re-run Validation Suite", key=f"{suite_config['suite_key']}_clear_cache"):
        clear_cache(suite_config["suite_key"])
        st.session_state.pop(f"{suite_config['suite_key']}_results", None)
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

# Extract components from simple payload
df = payload.get("df")
metrics = payload.get("metrics", {})
suite_name = payload.get("suite_name", "")
yaml_path = suite_config["yaml_path"]

# Get total materials count
actual_total = metrics.get("total_materials", 0)

# DEBUG: Log what we extracted
print(f"📊 DEBUG: Extracted DataFrame shape={df.shape if df is not None else 'None'}", flush=True)
print(f"📊 DEBUG: Total materials={actual_total}", flush=True)
print(f"📊 DEBUG: Overall pass rate={metrics.get('overall_pass_rate', 0)}%", flush=True)

# ----------------------------------------------------------
# Handle validation failure
# ----------------------------------------------------------
if df is None or df.empty:
    st.error("❌ Validation failed or returned no results.")
    st.info("This usually means:")
    st.markdown("""
    - Snowflake connection failed (check credentials/network)
    - No data returned from query
    - Validation suite encountered an error

    Check container logs with: `docker compose logs`
    """)
    st.stop()

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
def calc_overall_kpis_from_metrics(metrics):
    """Calculate overall pass/fail metrics from metrics dict."""
    total = metrics.get("total_materials", 0)
    overall_pass_rate = metrics.get("overall_pass_rate", 100)
    pass_rate = overall_pass_rate
    fail_rate = 100 - pass_rate

    # Calculate passed/failed from pass rate
    failed = int(total * (fail_rate / 100))
    passed = total - failed

    return total, passed, failed, pass_rate, fail_rate


def calc_column_fail_counts_from_metrics(metrics, yaml_path):
    """Calculate failure counts per column from metrics dict."""
    rows = []

    for exp_id, exp_metrics in metrics.get("expectation_metrics", {}).items():
        failures = exp_metrics.get("failures", 0)

        if failures > 0:
            # Look up which column this expectation validates
            metadata = lookup_expectation_metadata(exp_id, yaml_path)
            if metadata:
                column = metadata.get("column", exp_id)
            else:
                column = exp_id

            rows.append({
                "Column": column,
                "Failed Materials": failures
            })

    if rows:
        result_df = pd.DataFrame(rows)
        # Group by column in case multiple expectations validate the same column
        return (
            result_df.groupby("Column")["Failed Materials"]
            .sum()
            .reset_index()
            .sort_values("Failed Materials", ascending=False)
        )
    else:
        return pd.DataFrame(columns=["Column", "Failed Materials"])


# =====================================================
# OVERVIEW VIEW
# =====================================================
if view == "Overview":
    st.subheader("Validation Summary")

    total, passed, failed, pass_rate, fail_rate = calc_overall_kpis_from_metrics(metrics)

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
    # Extract derived status columns from DataFrame
    derived_columns = [col for col in df.columns if col.startswith("derived_")]

    if derived_columns:
        st.divider()
        with st.expander("Derived Statuses", expanded=False):
            st.caption(
                "Derived statuses synthesize multiple expectations into a single flag. "
                "This view shows any derived groups that triggered during the run."
            )

            # Build summary table from metrics
            derived_status_rows = []
            for derived_col in derived_columns:
                derived_metrics_data = metrics["derived_metrics"].get(derived_col, {})
                status_label = derived_col.replace("derived_", "").replace("_", " ").title()

                derived_status_rows.append({
                    "Status": status_label,
                    "Derived ID": derived_col,
                    "Failed Materials": derived_metrics_data.get("failures", 0),
                    "Pass Rate": f"{derived_metrics_data.get('pass_rate', 0)}%",
                })

            if derived_status_rows:
                derived_summary_df = pd.DataFrame(derived_status_rows)
                st.dataframe(
                    derived_summary_df,
                    hide_index=True,
                    use_container_width=True,
                )

                # Show detailed failure breakdown
                st.divider()
                st.caption("**Detailed Failure Breakdown**")

                index_column = payload.get("suite_config", {}).get("metadata", {}).get("index_column", "material_number").lower()

                for derived_col in derived_columns:
                    status_label = derived_col.replace("derived_", "").replace("_", " ").title()

                    # Get failed materials
                    failed_df = get_failed_materials(df, derived_id=derived_col, index_column=index_column)

                    if len(failed_df) > 0:
                        with st.expander(f"📋 {status_label} - {len(failed_df)} materials", expanded=False):
                            st.caption(
                                f"Materials that failed the '{status_label}' derived status. "
                                "Use the Details tab for more information."
                            )

                            # Get column-level breakdown
                            # Load YAML to find which columns are in this derived status
                            import yaml
                            with open(yaml_path, 'r') as f:
                                yaml_config = yaml.safe_load(f)

                            derived_statuses = yaml_config.get("derived_statuses", [])

                            # Find this derived status config
                            derived_status_config = None
                            for ds in derived_statuses:
                                ds_label = ds.get("status", "").replace(" ", "_").lower()
                                if derived_col.replace("derived_", "") == ds_label:
                                    derived_status_config = ds
                                    break

                            # Show column breakdown if we found the config
                            if derived_status_config:
                                columns_in_status = derived_status_config.get("columns", [])
                                expectation_type = derived_status_config.get("expectation_type", "")

                                # Build column failure breakdown
                                st.write("**Column Failure Breakdown:**")
                                breakdown_rows = []

                                for col in columns_in_status:
                                    # Find the expectation that checks this column
                                    # Look for exp_* columns that correspond to this column
                                    exp_columns = [c for c in df.columns if c.startswith("exp_")]

                                    # Count failures for this column among the failed materials
                                    for exp_col in exp_columns:
                                        # Look up metadata to see if this exp checks our column
                                        metadata = lookup_expectation_metadata(exp_col, yaml_path)
                                        if metadata and metadata.get("column") == col and metadata.get("expectation_type") == expectation_type:
                                            # Count unique materials that failed this specific expectation
                                            failures_for_col = failed_df[failed_df[exp_col] == 'FAIL'][index_column].nunique()
                                            if failures_for_col > 0:
                                                breakdown_rows.append({
                                                    "Column": col,
                                                    "Failed Materials": failures_for_col
                                                })
                                            break

                                if breakdown_rows:
                                    breakdown_df = pd.DataFrame(breakdown_rows)
                                    breakdown_df = breakdown_df.sort_values("Failed Materials", ascending=False)
                                    st.dataframe(
                                        breakdown_df,
                                        hide_index=True,
                                        use_container_width=True,
                                    )
                                    st.divider()

                            # Show material numbers
                            st.write(f"**Failed Materials ({len(failed_df)} total):**")
                            display_df = failed_df[[index_column]].copy()
                            display_df.columns = ["Material Number"]

                            st.dataframe(
                                display_df,
                                hide_index=True,
                                use_container_width=True,
                                height=min(400, len(display_df) * 35 + 38),
                            )

                            # Download option
                            csv = display_df.to_csv(index=False)
                            st.download_button(
                                label=f"⬇️ Download {status_label} failures as CSV",
                                data=csv,
                                file_name=f"{status_label.replace(' ', '_').lower()}_failures.csv",
                                mime="text/csv",
                                key=f"download_derived_{derived_col}"
                            )

    # =====================================================
    # DERIVED LISTS - Materials filtered by status exclusion
    # =====================================================
    # Load YAML to get derived_lists configuration
    import yaml
    with open(yaml_path, 'r') as f:
        yaml_config = yaml.safe_load(f)

    derived_lists_config = yaml_config.get("derived_lists", [])

    if derived_lists_config:
        st.divider()
        with st.expander("Derived Lists", expanded=False):
            st.caption(
                "Derived lists identify materials based on derived status membership. "
                "These lists can be downloaded for further processing."
            )

            # Get index column
            index_column = payload.get("suite_config", {}).get("metadata", {}).get("index_column", "material_number").lower()

            # Get all materials
            all_material_numbers = set(str(m) for m in df[index_column].unique())

            # Build a map of status_label -> failed materials for quick lookup
            status_to_materials = {}
            for derived_col in derived_columns:
                status_label = derived_col.replace("derived_", "").replace("_", " ").title()

                # Get failed materials for this derived status
                failed_df = get_failed_materials(df, derived_id=derived_col, index_column=index_column)
                material_numbers = set(str(m) for m in failed_df[index_column].unique())

                status_to_materials[status_label] = material_numbers

            # Calculate and display each derived list
            for derived_list in derived_lists_config:
                list_name = derived_list.get("name", "Unnamed List")
                description = derived_list.get("description", "")
                exclude_statuses = derived_list.get("exclude_statuses", [])

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

        failed_expect_counts = calc_column_fail_counts_from_metrics(metrics, yaml_path)

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

    # Choose between regular expectations and derived statuses
    detail_type = st.radio(
        "View",
        ["Expectations", "Derived Statuses"],
        horizontal=True,
        label_visibility="collapsed"
    )

    if detail_type == "Expectations":
        render_columnar_drill_down(
            df=df,
            metrics=metrics,
            yaml_path=yaml_path,
            suite_config=payload.get("suite_config", {}),
            suite_name=suite_name
        )
    else:
        render_derived_status_drill_down(
            df=df,
            metrics=metrics,
            yaml_path=yaml_path,
            suite_config=payload.get("suite_config", {})
        )
