"""
Tests for column existence validation in BaseValidationSuite.

These tests verify that missing columns are caught with clear error
messages before expectations are created.
"""

import pytest
import pandas as pd
from validations.base_validation import BaseValidationSuite


class TestColumnValidation:
    """Tests for _validate_columns() method."""

    def test_existing_columns_pass(self, sample_dataframe):
        """Should pass when all columns exist in DataFrame."""
        suite = BaseValidationSuite(sample_dataframe)

        # Should not raise
        suite._validate_columns(["MATERIAL_NUMBER", "DESCRIPTION"], "test_type")

    def test_single_missing_column_raises_error(self, sample_dataframe):
        """Should error when a single column is missing."""
        suite = BaseValidationSuite(sample_dataframe)

        with pytest.raises(ValueError) as exc_info:
            suite._validate_columns(["NONEXISTENT"], "test_type")

        error_msg = str(exc_info.value)
        assert "NONEXISTENT" in error_msg
        assert "not found" in error_msg
        assert "test_type" in error_msg

    def test_multiple_missing_columns_raises_error(self, sample_dataframe):
        """Should list all missing columns in error message."""
        suite = BaseValidationSuite(sample_dataframe)

        with pytest.raises(ValueError) as exc_info:
            suite._validate_columns(["MISSING_A", "MISSING_B"], "test_type")

        error_msg = str(exc_info.value)
        assert "MISSING_A" in error_msg
        assert "MISSING_B" in error_msg

    def test_error_shows_available_columns(self, sample_dataframe):
        """Should show available columns in error message."""
        suite = BaseValidationSuite(sample_dataframe)

        with pytest.raises(ValueError) as exc_info:
            suite._validate_columns(["NONEXISTENT"], "test_type")

        error_msg = str(exc_info.value)
        assert "Available columns" in error_msg
        assert "MATERIAL_NUMBER" in error_msg

    def test_empty_columns_list_passes(self, sample_dataframe):
        """Should pass when columns list is empty."""
        suite = BaseValidationSuite(sample_dataframe)

        # Should not raise - empty list is valid (no columns to check)
        suite._validate_columns([], "test_type")

    def test_case_sensitive_column_names(self, sample_dataframe):
        """Column names should be case-sensitive."""
        suite = BaseValidationSuite(sample_dataframe)

        # Lowercase version should fail
        with pytest.raises(ValueError):
            suite._validate_columns(["material_number"], "test_type")

    def test_partial_match_fails(self, sample_dataframe):
        """Should fail on partial column name matches."""
        suite = BaseValidationSuite(sample_dataframe)

        # "MATERIAL" is not the same as "MATERIAL_NUMBER"
        with pytest.raises(ValueError):
            suite._validate_columns(["MATERIAL"], "test_type")


class TestIndexColumnValidation:
    """Tests for INDEX_COLUMN validation in __init__."""

    def test_valid_index_column_succeeds(self, sample_dataframe):
        """Should succeed when INDEX_COLUMN exists."""
        # Should not raise
        suite = BaseValidationSuite(sample_dataframe)
        assert len(suite.df) == 3

    def test_missing_index_column_raises_error(self, dataframe_missing_index):
        """Should error when INDEX_COLUMN doesn't exist."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite(dataframe_missing_index)

        error_msg = str(exc_info.value)
        assert "INDEX_COLUMN" in error_msg
        assert "MATERIAL_NUMBER" in error_msg
        assert "not found" in error_msg

    def test_error_shows_available_columns(self, dataframe_missing_index):
        """Should show available columns when INDEX_COLUMN is missing."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite(dataframe_missing_index)

        error_msg = str(exc_info.value)
        assert "Available columns" in error_msg
        assert "OTHER_COLUMN" in error_msg

    def test_custom_index_column_class(self, sample_dataframe):
        """Should allow subclasses to override INDEX_COLUMN."""
        class CustomSuite(BaseValidationSuite):
            INDEX_COLUMN = "DESCRIPTION"
            SUITE_NAME = "CustomSuite"

            def define_expectations(self):
                pass

        # Should succeed with DESCRIPTION as index
        suite = CustomSuite(sample_dataframe)
        assert suite.INDEX_COLUMN == "DESCRIPTION"

    def test_custom_index_column_missing_fails(self):
        """Should fail when custom INDEX_COLUMN doesn't exist."""
        class CustomSuite(BaseValidationSuite):
            INDEX_COLUMN = "NONEXISTENT"
            SUITE_NAME = "CustomSuite"

            def define_expectations(self):
                pass

        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})

        with pytest.raises(ValueError) as exc_info:
            CustomSuite(df)

        assert "NONEXISTENT" in str(exc_info.value)


class TestEmptyDataFrameValidation:
    """Tests for empty DataFrame handling."""

    def test_empty_dataframe_init_succeeds(self, empty_dataframe):
        """Empty DataFrame should be allowed at init time."""
        # Init should succeed - the check happens at run() time
        suite = BaseValidationSuite(empty_dataframe)
        assert len(suite.df) == 0

    def test_empty_dataframe_run_raises_error(self, empty_dataframe):
        """Should error when running validation on empty DataFrame."""
        suite = BaseValidationSuite(empty_dataframe)
        suite._yaml_validations = [
            {"type": "expect_column_values_to_not_be_null", "columns": ["DESCRIPTION"]}
        ]

        with pytest.raises(ValueError) as exc_info:
            suite.run()

        error_msg = str(exc_info.value)
        assert "empty" in error_msg.lower()
