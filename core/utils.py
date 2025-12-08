"""
Shared utility functions for the validation framework.

This module consolidates common helper functions used across the codebase
to ensure consistency and reduce duplication.
"""

import os
import numpy as np

# Disable GX telemetry globally (single source of truth)
os.environ["GX_DISABLE_TELEMETRY"] = "true"


def make_json_safe(value):
    """
    Convert numpy types to native Python types for JSON serialization.

    Args:
        value: Any value that may be a numpy type

    Returns:
        Native Python type suitable for JSON serialization
    """
    if isinstance(value, np.generic):
        return value.item()
    return value


def deep_make_json_safe(value):
    """
    Recursively convert common non-JSON-serializable types to native Python
    equivalents (e.g., numpy scalars, pandas NA) so ``json.dump`` succeeds.

    Args:
        value: Arbitrary Python object

    Returns:
        A JSON-serializable representation of ``value``.
    """
    # Handle simple scalar conversions first
    value = make_json_safe(value)

    if isinstance(value, dict):
        return {k: deep_make_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [deep_make_json_safe(v) for v in value]
    return value


def safe_int(value, default=0):
    """
    Safely convert a value to int with fallback.

    Args:
        value: Value to convert
        default: Fallback value if conversion fails

    Returns:
        int: Converted value or default
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value, default=0.0):
    """
    Safely convert a value to float with fallback.

    Args:
        value: Value to convert
        default: Fallback value if conversion fails

    Returns:
        float: Converted value or default
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
