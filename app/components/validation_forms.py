"""
Reusable form components for YAML Editor.

Extracted from YAML_Editor.py to reduce duplication and improve maintainability.
"""

import streamlit as st
from typing import List, Dict, Any, Optional


def get_expectation_categories() -> Dict[str, List[str]]:
    """
    Return categorized expectation types for better organization.

    Returns:
        Dict mapping category names to lists of expectation types
    """
    return {
        "Basic Checks": [
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_in_set",
            "expect_column_values_to_not_be_in_set",
        ],
        "String/Pattern Checks": [
            "expect_column_values_to_match_regex",
            "expect_column_values_to_not_match_regex",
            "expect_column_value_lengths_to_equal",
            "expect_column_value_lengths_to_be_between",
        ],
        "Numeric Checks": [
            "expect_column_values_to_be_between",
        ],
        "Uniqueness Checks": [
            "expect_column_values_to_be_unique",
            "expect_compound_columns_to_be_unique",
        ],
        "Cross-Column Checks": [
            "expect_column_pair_values_a_to_be_greater_than_b",
            "expect_column_pair_values_to_be_equal",
        ],
    }


def get_flat_expectation_list() -> List[str]:
    """Get flat list of all expectation types in order."""
    categories = get_expectation_categories()
    result = []
    for category_types in categories.values():
        result.extend(category_types)
    return result


def render_column_selector(
    columns: List[str],
    default: List[str] = None,
    label: str = "Select columns",
    help_text: str = None,
    key: str = "",
    allow_multiple: bool = True
) -> List[str]:
    """
    Reusable column selector with consistent styling.

    Args:
        columns: List of available column names
        default: Default selected columns
        label: Label for the selector
        help_text: Help text to display
        key: Unique key for the widget
        allow_multiple: If True, use multiselect; if False, use selectbox

    Returns:
        List of selected column names (or single-item list if allow_multiple=False)
    """
    if default is None:
        default = []

    if allow_multiple:
        return st.multiselect(
            label,
            options=columns,
            default=default,
            help=help_text,
            key=key
        )
    else:
        default_single = default[0] if default else (columns[0] if columns else None)
        selected = st.selectbox(
            label,
            options=columns,
            index=columns.index(default_single) if default_single and default_single in columns else 0,
            help=help_text,
            key=key
        )
        return [selected] if selected else []


def render_conditional_section(
    derived_statuses: List[Dict[str, Any]],
    editing_rule: Dict[str, Any] = None,
    key_suffix: str = ""
) -> Optional[Dict[str, str]]:
    """
    Render conditional logic controls for validations.

    Args:
        derived_statuses: List of derived status groups from session state
        editing_rule: Existing rule being edited (for pre-population)
        key_suffix: Unique suffix for widget keys

    Returns:
        Dict with {derived_group: str, membership: str} if enabled, else None
    """
    with st.expander("⚙️ Advanced: Conditional Logic (Optional)", expanded=False):
        st.caption("Apply this validation only when materials are in or not in a specific derived group")

        # Get available derived groups
        available_groups = []
        for derived in derived_statuses:
            exp_id = derived.get("expectation_id")
            status = derived.get("status")
            if exp_id:
                available_groups.append({
                    "id": exp_id,
                    "label": f"{status} ({exp_id})" if status else exp_id
                })

        if not available_groups:
            st.info("💡 No derived groups available yet. Create derived status groups first to use conditional logic.")
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


def get_expectation_display_name(expectation_type: str) -> str:
    """
    Get user-friendly display name for expectation type.

    Args:
        expectation_type: Technical expectation type name

    Returns:
        Human-readable display name
    """
    display_names = {
        "expect_column_values_to_not_be_null": "Not Null",
        "expect_column_values_to_be_in_set": "Values in Set",
        "expect_column_values_to_not_be_in_set": "Values not in Set",
        "expect_column_values_to_match_regex": "Match Regex Pattern",
        "expect_column_values_to_not_match_regex": "Not Match Regex Pattern",
        "expect_column_value_lengths_to_equal": "Length Equals",
        "expect_column_value_lengths_to_be_between": "Length Between",
        "expect_column_values_to_be_between": "Values Between (Numeric)",
        "expect_column_values_to_be_unique": "Column Values Unique",
        "expect_compound_columns_to_be_unique": "Compound Columns Unique",
        "expect_column_pair_values_a_to_be_greater_than_b": "Column A > Column B",
        "expect_column_pair_values_to_be_equal": "Column A = Column B",
    }
    return display_names.get(expectation_type, expectation_type)


def render_expectation_type_selector(
    default_type: str = "expect_column_values_to_not_be_null",
    key: str = "validation_type_selector"
) -> str:
    """
    Render an expectation type selector with organized categories shown in help text.

    Args:
        default_type: Default expectation type to select
        key: Unique key for the selectbox widget

    Returns:
        Selected expectation type
    """
    all_types = get_flat_expectation_list()

    # Find default index
    try:
        default_idx = all_types.index(default_type)
    except ValueError:
        default_idx = 0

    # Build help text with categories
    categories = get_expectation_categories()
    help_lines = ["Available validation types by category:"]
    for category, types in categories.items():
        help_lines.append(f"\n{category}:")
        for t in types:
            help_lines.append(f"  • {get_expectation_display_name(t)}")

    selected = st.selectbox(
        "Validation Type",
        options=all_types,
        index=default_idx,
        help="\n".join(help_lines),
        key=key,
        format_func=get_expectation_display_name
    )

    return selected
