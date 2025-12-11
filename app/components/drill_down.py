"""
Shared drill-down UI component for validation results.

This component provides a reusable interface for drilling down into
validation failures by expectation type and column.
"""

import streamlit as st
import pandas as pd
from app.components.ui_helpers import render_send_to_datalark_button
from core.unified_logs import get_rectified_materials


def render_expectation_drill_down(
    results: list,
    df: pd.DataFrame,
    suite_name: str = None,
    cache_suite_name: str = None,
    show_expected_values: bool = False,
):
    """
    Render an interactive drill-down interface for validation results.

    This component displays:
    - Dropdowns to select expectation type and column
    - Summary metrics (pass/fail counts and percentage)
    - Table of failed materials
    - Button to send failures to Data Lark (with sent status tracking)

    Parameters
    ----------
    results : list
        Raw validation results from run_validation_suite()
    df : pd.DataFrame
        Failure DataFrame from BaseValidationSuite.results_to_dataframe()
    suite_name : str, optional
        Name to include in Data Lark payload (e.g., "Aurora Motors")
    cache_suite_name : str, optional
        Cache key for tracking sent materials (e.g., "aurora", "level1")
    show_expected_values : bool, optional
        Whether to display expected values in the summary

    Returns
    -------
    dict or None
        Selection context if user made valid selections, None otherwise.
        Contains: expectation_type, column, match, final_df
    """
    col1, col2 = st.columns([1, 2])

    # Track selection state
    selected_expect = None
    selected_col = None
    final_df = pd.DataFrame()
    match = None

    with col1:
        expect_types = df["Expectation Type"].dropna().unique()
        if len(expect_types) == 0:
            st.info("No failed expectations to display.")
            return None

        selected_expect = st.selectbox("Expectation Type", options=expect_types)
        filtered_df = df[df["Expectation Type"] == selected_expect]
        columns = filtered_df["Column"].dropna().unique()
        selected_col = st.selectbox("Column", options=columns)

        final_df = filtered_df[filtered_df["Column"] == selected_col]

        if not final_df.empty:
            # Find matching result for metrics
            match = next(
                (r for r in results
                 if r["expectation_type"] == selected_expect
                 and r["column"] == selected_col),
                None
            )

            if match:
                _render_summary_metrics(
                    match,
                    selected_expect,
                    selected_col,
                    show_expected_values
                )

    with col2:
        if not final_df.empty and match:
            _render_failure_table(final_df, selected_expect, selected_col)
            _render_datalark_section(
                final_df,
                selected_expect,
                selected_col,
                match,
                suite_name,
                cache_suite_name
            )
        else:
            st.info("No failures for this expectation/column combination.")

    # Return selection context for any additional processing
    if not final_df.empty and match:
        return {
            "expectation_type": selected_expect,
            "column": selected_col,
            "match": match,
            "final_df": final_df,
        }
    return None


def _render_summary_metrics(
    match: dict,
    selected_expect: str,
    selected_col: str,
    show_expected_values: bool = False
):
    """Render pass/fail summary metrics with progress bar and grain information."""
    total = match.get("element_count", 0)
    fails = match.get("unexpected_count", 0)
    passes = total - fails
    pct = (passes / total * 100) if total > 0 else 0

    # Get grain metadata
    table_grain = match.get("table_grain", "UNKNOWN")
    unique_by = match.get("unique_by", ["MATERIAL_NUMBER"])

    st.write(f"### Summary for `{selected_expect}` on `{selected_col}`")

    # Show grain information
    st.caption(f"**Table Grain**: {table_grain} | **Unique By**: {', '.join(unique_by)}")

    st.progress(passes / total if total > 0 else 0)
    st.write(f"{passes} / {total} passed ({pct:.1f}%) | {fails} failed")

    # Optionally show expected values
    if show_expected_values:
        expected = match.get("expected")
        if expected is not None:
            if isinstance(expected, list):
                st.write(f"**Expected Value(s):** {', '.join(map(str, expected))}")
            else:
                st.write(f"**Expected Value:** {expected}")


def _render_failure_table(df: pd.DataFrame, selected_expect: str, selected_col: str):
    """Render the failures dataframe."""
    st.write(f"### Failures for `{selected_expect}` on `{selected_col}`")
    st.dataframe(
        df[["Material Number", "Unexpected Value"]],
        hide_index=True,
        use_container_width=True,
    )


def _render_datalark_section(
    df: pd.DataFrame,
    selected_expect: str,
    selected_col: str,
    match: dict,
    suite_name: str = None,
    cache_suite_name: str = None
):
    """Render the Data Lark send button with payload and rectification status from unified logs."""
    st.divider()
    st.subheader("Send to Data Lark")

    # --- GRAIN-BASED DEDUPLICATION ---
    # Get grain (unique key columns) for this column from the match result
    unique_by = match.get("unique_by", ["Material Number"])
    table_grain = match.get("table_grain", "UNKNOWN")

    # Validate that all grain columns exist in the DataFrame
    from core.grain_mapping import validate_grain_columns_exist, get_fallback_grain
    df_columns = df.columns.tolist()

    # Map MATERIAL_NUMBER to "Material Number" for UI compatibility
    unique_by_ui = []
    for col in unique_by:
        if col == "MATERIAL_NUMBER":
            unique_by_ui.append("Material Number")
        elif col in df_columns:
            unique_by_ui.append(col)

    if not validate_grain_columns_exist(unique_by_ui, df_columns):
        st.warning(f"⚠️ Grain columns {unique_by} not all available. Using fallback grain.")
        unique_by_ui = get_fallback_grain(unique_by_ui, df_columns)

    # Deduplicate based on grain
    total_rows = len(df)
    df_deduped = df.drop_duplicates(subset=unique_by_ui, keep="first")
    unique_count = len(df_deduped)

    # Show deduplication summary
    if unique_count < total_rows:
        st.info(
            f"📊 **{table_grain} grain**: {unique_count} unique records "
            f"({total_rows} total rows with org context)\n\n"
            f"**Deduplication key**: {', '.join(unique_by_ui)}"
        )
    else:
        st.info(f"📊 **{table_grain} grain**: {unique_count} unique records")

    # Get all material numbers from deduplicated selection
    all_materials = set(str(m) for m in df_deduped["Material Number"].dropna().unique())

    # Get already-rectified materials from unified logs
    rectified_materials = get_rectified_materials(column=selected_col)

    # Calculate unrectified materials
    unrectified_materials = all_materials - rectified_materials
    rectified_count = len(all_materials & rectified_materials)
    unrectified_count = len(unrectified_materials)

    # Show rectification status
    if rectified_count > 0:
        st.success(f"{rectified_count} of {len(all_materials)} materials already rectified (per unified logs)")

    # Option to resend all (for debugging)
    resend_all = st.checkbox(
        "Resend all materials (ignore rectification status)",
        key=f"resend_{selected_expect}_{selected_col}"
    )

    if resend_all:
        # Send all materials regardless of rectification status (using deduplicated data)
        materials_to_send = all_materials
        send_df = df_deduped
        send_count = len(all_materials)
    else:
        # Only send unrectified materials (using deduplicated data)
        if unrectified_count == 0:
            st.info("All materials for this selection have been rectified")
            return
        materials_to_send = unrectified_materials
        send_df = df_deduped[df_deduped["Material Number"].astype(str).isin(unrectified_materials)]
        send_count = unrectified_count

    # Build payload
    # Replace NaN values with None for JSON serialization
    send_df_clean = send_df.fillna(value=None)

    payload = {
        "expectation_type": selected_expect,
        "column": selected_col,
        "expected": match.get("expected"),
        "failed_materials": send_df_clean.to_dict(orient="records"),
    }

    # Include suite name if provided
    if suite_name:
        payload["suite_name"] = suite_name

    # Render button
    if resend_all:
        button_label = f"Resend All {send_count} Failures to Data Lark"
    elif rectified_count > 0:
        button_label = f"Send {send_count} Remaining Failures to Data Lark"
    else:
        button_label = f"Send {send_count} Failures to Data Lark"

    if st.button(button_label, key=f"datalark_{selected_expect}_{selected_col}"):
        from data_lark.client import send_payload

        success, message = send_payload(payload)
        if success:
            st.success(f"Sent {send_count} materials successfully!")
            st.info("Refresh the page after Data Lark processes to see updated rectification status")
        else:
            st.error(f"Failed to send to Data Lark: {message}")
