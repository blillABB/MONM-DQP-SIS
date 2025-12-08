"""
Pytest configuration and shared fixtures for MONM-MDM-DQP tests.

These tests validate the validation infrastructure itself - they don't
require Snowflake connectivity and use mock DataFrames.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Ensure project root is in path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def sample_dataframe():
    """A basic DataFrame with MATERIAL_NUMBER for testing."""
    return pd.DataFrame({
        "MATERIAL_NUMBER": ["MAT001", "MAT002", "MAT003"],
        "DESCRIPTION": ["Widget A", "Widget B", "Widget C"],
        "GROSS_WEIGHT": [10.5, 20.0, 15.3],
        "NET_WEIGHT": [8.0, 18.0, 12.0],
        "PRICE": [100.0, 200.0, 150.0],
        "STATUS": ["ACTIVE", "ACTIVE", "INACTIVE"],
    })


@pytest.fixture
def empty_dataframe():
    """An empty DataFrame with correct columns."""
    return pd.DataFrame({
        "MATERIAL_NUMBER": pd.Series(dtype=str),
        "DESCRIPTION": pd.Series(dtype=str),
    })


@pytest.fixture
def dataframe_missing_index():
    """A DataFrame without MATERIAL_NUMBER column."""
    return pd.DataFrame({
        "OTHER_COLUMN": ["A", "B", "C"],
        "VALUE": [1, 2, 3],
    })


@pytest.fixture
def valid_yaml_config():
    """A valid YAML configuration dict."""
    return {
        "metadata": {
            "suite_name": "Test_Suite",
            "index_column": "MATERIAL_NUMBER",
            "data_source": "test_source",
        },
        "validations": [
            {
                "type": "expect_column_values_to_not_be_null",
                "columns": ["MATERIAL_NUMBER", "DESCRIPTION"]
            }
        ]
    }


@pytest.fixture
def invalid_yaml_missing_metadata():
    """YAML config missing metadata section."""
    return {
        "validations": [
            {"type": "expect_column_values_to_not_be_null", "columns": ["A"]}
        ]
    }


@pytest.fixture
def invalid_yaml_missing_data_source():
    """YAML config missing data_source in metadata."""
    return {
        "metadata": {
            "suite_name": "Test_Suite",
        },
        "validations": [
            {"type": "expect_column_values_to_not_be_null", "columns": ["A"]}
        ]
    }


@pytest.fixture
def invalid_yaml_unknown_type():
    """YAML config with unknown expectation type (typo)."""
    return {
        "metadata": {
            "suite_name": "Test_Suite",
            "data_source": "test_source",
        },
        "validations": [
            {"type": "expect_column_values_to_not_be_nul", "columns": ["A"]}  # Typo
        ]
    }


@pytest.fixture
def invalid_yaml_empty_validations():
    """YAML config with empty validations list."""
    return {
        "metadata": {
            "suite_name": "Test_Suite",
            "data_source": "test_source",
        },
        "validations": []
    }
