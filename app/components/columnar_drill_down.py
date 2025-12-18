"""
Columnar drill-down component for simplified validation results.

Works directly with columnar DataFrames (exp_* columns) without
requiring complex result structures or format conversions.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from core.expectation_metadata import lookup_expectation_metadata
from core.validation_metrics import get_failed_materials


def render_columnar_drill_down(
    df: pd.DataFrame,
    metrics: dict,
    yaml_path: str | Path,
    suite_config: dict,
    suite_name: str = None,
):
    """
    Render drill-down interface for columnar validation results.

    Args:
        df: Columnar DataFrame with exp_* columns (PASS/FAIL values)
        metrics: Metrics dict from calculate_validation_metrics()
        yaml_path: Path to YAML for metadata lookup
        suite_config: Suite configuration dict
        suite_name: Optional suite name for display
    """
    # Get index column
    index_column = suite_config.get("metadata", {}).get("index_column", "material_number").lower()

    # Extract expectation columns
    exp_columns = [col for col in df.columns if col.startswith("exp_")]

    if not exp_columns:
        st.info("No expectations found in results")
        return

    # Build display options with metadata
    exp_options = {}
    for exp_col in exp_columns:
        metadata = lookup_expectation_metadata(exp_col, yaml_path)
        if metadata:
            exp_type = metadata.get("expectation_type", exp_col)
            column = metadata.get("column", "")
            display_name = f"{column} - {exp_type}"
        else:
            display_name = exp_col

        exp_options[display_name] = {
            "exp_id": exp_col,
            "metadata": metadata,
        }

    col1, col2 = st.columns([1, 2])

    with col1:
        # Select expectation
        selected_display = st.selectbox(
            "Select Expectation",
            options=list(exp_options.keys()),
            key="columnar_drill_down_selector"
        )

        selected = exp_options[selected_display]
        exp_id = selected["exp_id"]
        metadata = selected["metadata"]

        # Get metrics for this expectation
        exp_metrics = metrics["expectation_metrics"].get(exp_id, {})

        # Show summary metrics
        st.write("### Summary")
        total = exp_metrics.get("total", 0)
        failures = exp_metrics.get("failures", 0)
        passes = exp_metrics.get("passes", 0)
        pass_rate = exp_metrics.get("pass_rate", 0)

        if total > 0:
            st.progress(pass_rate / 100)
            st.write(f"{passes:,} / {total:,} passed ({pass_rate:.1f}%)")
            st.write(f"{failures:,} failed")
        else:
            st.info("No data for this expectation")

        # Show expectation details
        if metadata:
            st.write("**Details:**")
            st.write(f"- **Column**: {metadata.get('column', 'N/A')}")
            st.write(f"- **Type**: {metadata.get('expectation_type', 'N/A')}")

    with col2:
        if failures > 0:
            st.write(f"### Failures ({failures:,} materials)")

            # Get failed materials
            failed_df = get_failed_materials(df, exp_id=exp_id, index_column=index_column)

            # Get the source column being validated
            if metadata:
                source_column = metadata.get("column", "").lower()

                # Build display DataFrame
                display_cols = [index_column]

                # Add source column if it exists
                if source_column in failed_df.columns:
                    display_cols.append(source_column)
                elif source_column.upper() in failed_df.columns:
                    display_cols.append(source_column.upper())

                # Show failures
                display_df = failed_df[display_cols].copy()

                # Rename columns for display
                display_df.columns = [col.replace("_", " ").title() for col in display_df.columns]

                st.dataframe(
                    display_df,
                    hide_index=True,
                    use_container_width=True,
                    height=min(400, len(display_df) * 35 + 38)
                )

                # Download button
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label=f"⬇️ Download {failures:,} failures as CSV",
                    data=csv,
                    file_name=f"{exp_id}_failures.csv",
                    mime="text/csv"
                )
            else:
                # Fallback if metadata lookup failed
                st.dataframe(
                    failed_df[[index_column]],
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.success("✅ No failures for this expectation!")


def render_derived_status_drill_down(
    df: pd.DataFrame,
    metrics: dict,
    yaml_path: str | Path,
    suite_config: dict,
):
    """
    Render drill-down for derived statuses in columnar format.

    Args:
        df: Columnar DataFrame with derived_* columns
        metrics: Metrics dict from calculate_validation_metrics()
        yaml_path: Path to YAML for metadata lookup
        suite_config: Suite configuration dict
    """
    # Get index column
    index_column = suite_config.get("metadata", {}).get("index_column", "material_number").lower()

    # Extract derived columns
    derived_columns = [col for col in df.columns if col.startswith("derived_")]

    if not derived_columns:
        st.info("No derived statuses in this suite")
        return

    # Build display options
    derived_options = {}
    for derived_col in derived_columns:
        status_label = derived_col.replace("derived_", "").replace("_", " ").title()
        derived_options[status_label] = derived_col

    col1, col2 = st.columns([1, 2])

    with col1:
        # Select derived status
        selected_label = st.selectbox(
            "Select Derived Status",
            options=list(derived_options.keys()),
            key="derived_drill_down_selector"
        )

        derived_id = derived_options[selected_label]

        # Get metrics
        derived_metrics = metrics["derived_metrics"].get(derived_id, {})

        # Show summary
        st.write("### Summary")
        total = derived_metrics.get("total", 0)
        failures = derived_metrics.get("failures", 0)
        passes = derived_metrics.get("passes", 0)
        pass_rate = derived_metrics.get("pass_rate", 0)

        if total > 0:
            st.progress(pass_rate / 100)
            st.write(f"{passes:,} / {total:,} passed ({pass_rate:.1f}%)")
            st.write(f"{failures:,} failed")
        else:
            st.info("No data")

    with col2:
        if failures > 0:
            st.write(f"### Failures ({failures:,} materials)")

            # Get failed materials
            failed_df = get_failed_materials(df, derived_id=derived_id, index_column=index_column)

            # Show which constituent expectations failed
            # Get all exp_* columns that might be related
            exp_columns = [col for col in failed_df.columns if col.startswith("exp_")]

            # Build display showing which expectations failed
            display_rows = []
            for _, row in failed_df.iterrows():
                material = row.get(index_column, "")

                # Find which expectations failed for this material
                failed_exps = []
                for exp_col in exp_columns:
                    if exp_col in row.index and row[exp_col] == 'FAIL':
                        # Look up column name
                        metadata = lookup_expectation_metadata(exp_col, yaml_path)
                        if metadata:
                            failed_exps.append(metadata.get("column", exp_col))
                        else:
                            failed_exps.append(exp_col)

                display_rows.append({
                    "Material": material,
                    "Failed Expectations": len(failed_exps),
                    "Failed Columns": ", ".join(failed_exps[:5]) + ("..." if len(failed_exps) > 5 else "")
                })

            display_df = pd.DataFrame(display_rows)

            st.dataframe(
                display_df,
                hide_index=True,
                use_container_width=True,
                height=min(400, len(display_df) * 35 + 38)
            )

            # Download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label=f"⬇️ Download {failures:,} failures as CSV",
                data=csv,
                file_name=f"{derived_id}_failures.csv",
                mime="text/csv"
            )
        else:
            st.success("✅ No failures for this derived status!")


__all__ = [
    "render_columnar_drill_down",
    "render_derived_status_drill_down",
]
