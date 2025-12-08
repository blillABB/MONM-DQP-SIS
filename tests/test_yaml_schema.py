"""
Tests for YAML schema validation in BaseValidationSuite.

These tests verify that malformed YAML configurations are caught
with clear error messages before any expensive operations run.
"""

import pytest
from validations.base_validation import BaseValidationSuite


class TestYAMLSchemaValidation:
    """Tests for _validate_yaml_schema() method."""

    def test_valid_config_passes(self, valid_yaml_config):
        """Should pass with a valid configuration."""
        # Should not raise any exception
        BaseValidationSuite._validate_yaml_schema(valid_yaml_config, "test.yaml")

    def test_missing_metadata_raises_error(self, invalid_yaml_missing_metadata):
        """Should error when metadata section is missing."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(
                invalid_yaml_missing_metadata, "test.yaml"
            )
        assert "Missing required 'metadata'" in str(exc_info.value)

    def test_missing_data_source_raises_error(self, invalid_yaml_missing_data_source):
        """Should error when data_source is missing from metadata."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(
                invalid_yaml_missing_data_source, "test.yaml"
            )
        assert "data_source is required" in str(exc_info.value)

    def test_missing_suite_name_raises_error(self):
        """Should error when suite_name is missing from metadata."""
        config = {
            "metadata": {
                "data_source": "test_source",
                # suite_name missing
            },
            "validations": [
                {"type": "expect_column_values_to_not_be_null", "columns": ["A"]}
            ]
        }
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(config, "test.yaml")
        assert "suite_name is required" in str(exc_info.value)

    def test_unknown_expectation_type_raises_error(self, invalid_yaml_unknown_type):
        """Should error on typo in expectation type."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(
                invalid_yaml_unknown_type, "test.yaml"
            )
        assert "unknown type" in str(exc_info.value)
        assert "expect_column_values_to_not_be_nul" in str(exc_info.value)

    def test_empty_validations_raises_error(self, invalid_yaml_empty_validations):
        """Should error when validations list is empty."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(
                invalid_yaml_empty_validations, "test.yaml"
            )
        assert "empty" in str(exc_info.value).lower()

    def test_non_dict_config_raises_error(self):
        """Should error when config is not a dict."""
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema("not a dict", "test.yaml")
        assert "must contain a mapping" in str(exc_info.value)

    def test_metadata_not_dict_raises_error(self):
        """Should error when metadata is not a dict."""
        config = {
            "metadata": "should be a dict",
            "validations": []
        }
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(config, "test.yaml")
        assert "'metadata' must be a mapping" in str(exc_info.value)

    def test_validations_not_list_raises_error(self):
        """Should error when validations is not a list."""
        config = {
            "metadata": {
                "suite_name": "Test",
                "data_source": "test_source"
            },
            "validations": "should be a list"
        }
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(config, "test.yaml")
        assert "'validations' must be a list" in str(exc_info.value)

    def test_validation_missing_type_raises_error(self):
        """Should error when a validation rule is missing type field."""
        config = {
            "metadata": {
                "suite_name": "Test",
                "data_source": "test_source"
            },
            "validations": [
                {"columns": ["A"]}  # Missing 'type'
            ]
        }
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(config, "test.yaml")
        assert "missing required 'type'" in str(exc_info.value)

    def test_multiple_errors_accumulated(self):
        """Should accumulate multiple errors into one message."""
        config = {
            "metadata": {},  # Missing suite_name and data_source
            "validations": [
                {"type": "invalid_type"},
                {"columns": ["A"]}  # Missing type
            ]
        }
        with pytest.raises(ValueError) as exc_info:
            BaseValidationSuite._validate_yaml_schema(config, "test.yaml")

        error_msg = str(exc_info.value)
        # Should contain multiple errors
        assert "suite_name is required" in error_msg
        assert "data_source is required" in error_msg

    def test_all_valid_expectation_types_accepted(self):
        """Should accept all supported expectation types."""
        valid_types = BaseValidationSuite.SUPPORTED_EXPECTATION_TYPES

        for exp_type in valid_types:
            config = {
                "metadata": {
                    "suite_name": "Test",
                    "data_source": "test_source"
                },
                "validations": [
                    {"type": exp_type, "columns": ["A"]}
                ]
            }
            # Should not raise
            BaseValidationSuite._validate_yaml_schema(config, "test.yaml")
