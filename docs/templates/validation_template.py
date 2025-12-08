"""
Validation Suite Template - All 12 Expectation Types
=====================================================

This template demonstrates ALL supported expectation types for creating
validation suites. You can use this as a reference for both:
- Python-based validation classes (this file)
- YAML-based validation suites (preferred for new suites)

RECOMMENDED: For new validation suites, use YAML files instead of Python.
See validation_yaml/ directory for examples. YAML suites are easier to
maintain and don't require code deployment to update rules.

HOW TO USE THIS FILE
--------------------
1. Copy this file to validations/your_validation.py
2. Update the class name, SUITE_NAME, and INDEX_COLUMN
3. Update get_your_dataframe() to load your data
4. In define_expectations(), keep only the rules you need
5. Register your validation in app/index.py

EXPECTATION TYPES COVERED
-------------------------
A. expect_column_values_to_not_be_null     - Required columns (no nulls)
B. expect_column_pair_values_a_to_be_greater_than_b - Numeric comparison
C. expect_column_values_to_be_in_set       - Allowed values (whitelist)
D. expect_column_values_to_not_be_in_set   - Forbidden values (blacklist)
E. expect_column_values_to_match_regex     - Pattern matching
F. expect_column_values_to_not_match_regex - Pattern exclusion
G. expect_column_pair_values_to_be_equal   - Column equality
H. expect_column_value_lengths_to_equal    - Fixed string length
I. expect_column_value_lengths_to_be_between - Variable string length
J. expect_column_values_to_be_between      - Numeric range
K. expect_column_values_to_be_unique       - No duplicates
L. expect_compound_columns_to_be_unique    - Composite key uniqueness
"""

import great_expectations as gx
import pandas as pd

from core.queries import get_aurora_motor_dataframe  # Replace with your data source
from validations.base_validation import BaseValidationSuite


class YourValidation(BaseValidationSuite):
    """
    Template validation suite demonstrating all expectation types.

    Replace 'YourValidation' with a descriptive name like:
    - AuroraMotorsValidation
    - Level1BaselineValidation
    - MG4EndItemValidation
    """

    # Human-readable name for this suite (appears in reports)
    SUITE_NAME = "Your_Validation_Suite"

    # Column that uniquely identifies each row (for failure tracking)
    INDEX_COLUMN = "MATERIAL_NUMBER"

    def __init__(self):
        """
        Load your DataFrame and initialize the validation suite.
        Replace get_aurora_motor_dataframe() with your data source.
        """
        df = get_aurora_motor_dataframe()
        super().__init__(df)

    def define_expectations(self):
        """
        Define all validation rules for this suite.

        Each self.expect(...) call adds one validation rule.
        Delete or comment out sections you don't need.
        """
        # Shorthand for result format (captures failed Material Numbers)
        rf = self.DEFAULT_RESULT_FORMAT

        # =============================================================
        # A. REQUIRED / NON-NULL COLUMNS
        # =============================================================
        # Rule type: expect_column_values_to_not_be_null
        # Use when: "This column must always have a value"
        # YAML equivalent:
        #   - type: expect_column_values_to_not_be_null
        #     columns: [Gross Weight, Net Weight, Global Product ID]

        required_columns = [
            "Gross Weight",
            "Net Weight",
            "Global Product ID",
            "Standard Price",
        ]

        for col in required_columns:
            self.expect(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=col,
                    result_format=rf,
                )
            )

        # =============================================================
        # B. NUMERIC COMPARISON: Column A >= Column B
        # =============================================================
        # Rule type: expect_column_pair_values_a_to_be_greater_than_b
        # Use when: "One column should always be >= another"
        # YAML equivalent:
        #   - type: expect_column_pair_values_a_to_be_greater_than_b
        #     column_a: Gross Weight
        #     column_b: Net Weight
        #     or_equal: true

        self.expect(
            gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
                column_A="Gross Weight",
                column_B="Net Weight",
                or_equal=True,  # True = A >= B, False = A > B
                result_format=rf,
            )
        )

        # =============================================================
        # C. ALLOWED VALUES (WHITELIST)
        # =============================================================
        # Rule type: expect_column_values_to_be_in_set
        # Use when: "Column must be one of these specific values"
        # YAML equivalent:
        #   - type: expect_column_values_to_be_in_set
        #     rules:
        #       Material Type: [CAT]
        #       Industry Sector: [M]

        fixed_value_rules = {
            "Material Type": ["CAT"],
            "Industry Sector": ["M"],
            "Weight Unit": ["LB"],
        }

        for col, allowed_values in fixed_value_rules.items():
            self.expect(
                gx.expectations.ExpectColumnValuesToBeInSet(
                    column=col,
                    value_set=allowed_values,
                    result_format=rf,
                )
            )

        # =============================================================
        # D. FORBIDDEN VALUES (BLACKLIST)
        # =============================================================
        # Rule type: expect_column_values_to_not_be_in_set
        # Use when: "Column must NOT contain these values"
        # YAML equivalent:
        #   - type: expect_column_values_to_not_be_in_set
        #     column: Profit Center
        #     value_set: [UNDEFINED, N/A, TBD]

        self.expect(
            gx.expectations.ExpectColumnValuesToNotBeInSet(
                column="Profit Center",
                value_set=["UNDEFINED", "N/A", "TBD"],
                result_format=rf,
            )
        )

        # =============================================================
        # E. PATTERN MATCHING (REGEX MUST MATCH)
        # =============================================================
        # Rule type: expect_column_values_to_match_regex
        # Use when: "Values must match this pattern"
        # Common patterns:
        #   r"^\s*$"      - Must be blank/empty
        #   r"^\d+$"      - Must be all digits
        #   r"^\d{13}$"   - Must be exactly 13 digits (EAN)
        #   r"^[A-Z]{2}\d{4}$" - Two letters followed by 4 digits
        # YAML equivalent:
        #   - type: expect_column_values_to_match_regex
        #     columns: [EAN Number]
        #     regex: "^\\d{13}$"

        # Example: EAN Number must be exactly 13 digits
        self.expect(
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="EAN Number",
                regex=r"^\d{13}$",
                result_format=rf,
            )
        )

        # Example: These columns must be blank (whitespace only)
        blank_columns = ["Pack Indicator", "Automatic PO"]
        for col in blank_columns:
            self.expect(
                gx.expectations.ExpectColumnValuesToMatchRegex(
                    column=col,
                    regex=r"^\s*$",
                    result_format=rf,
                )
            )

        # =============================================================
        # F. PATTERN EXCLUSION (REGEX MUST NOT MATCH)
        # =============================================================
        # Rule type: expect_column_values_to_not_match_regex
        # Use when: "Values must NOT match this pattern"
        # Common patterns:
        #   r"[<>:\"/\\|?*]"  - No special characters
        #   r"^\s+|\s+$"      - No leading/trailing whitespace
        #   r"test|dummy"     - No test/dummy values (case sensitive)
        # YAML equivalent:
        #   - type: expect_column_values_to_not_match_regex
        #     columns: [Description, Material Number]
        #     regex: "(?i)test|dummy|placeholder"

        # Example: Description should not contain test/placeholder text
        self.expect(
            gx.expectations.ExpectColumnValuesToNotMatchRegex(
                column="Description",
                regex=r"(?i)test|dummy|placeholder|TBD",  # (?i) = case insensitive
                result_format=rf,
            )
        )

        # Example: Material Number should not contain special characters
        self.expect(
            gx.expectations.ExpectColumnValuesToNotMatchRegex(
                column="Material Number",
                regex=r"[<>:\"/\\|?*]",  # Forbidden characters
                result_format=rf,
            )
        )

        # =============================================================
        # G. COLUMN EQUALITY
        # =============================================================
        # Rule type: expect_column_pair_values_to_be_equal
        # Use when: "These two columns should always have the same value"
        # YAML equivalent:
        #   - type: expect_column_pair_values_to_be_equal
        #     column_a: Item Category Group
        #     column_b: Sales Item Category Group

        self.expect(
            gx.expectations.ExpectColumnPairValuesToBeEqual(
                column_A="Item Category Group",
                column_B="Sales Item Category Group",
                result_format=rf,
            )
        )

        # =============================================================
        # H. FIXED STRING LENGTH
        # =============================================================
        # Rule type: expect_column_value_lengths_to_equal
        # Use when: "String must be exactly N characters"
        # YAML equivalent:
        #   - type: expect_column_value_lengths_to_equal
        #     columns: [Plant]
        #     value: 3

        # Example: Plant code must be exactly 3 characters
        self.expect(
            gx.expectations.ExpectColumnValueLengthsToEqual(
                column="Plant",
                value=3,
                result_format=rf,
            )
        )

        # Example: Material Number must be exactly 18 characters
        self.expect(
            gx.expectations.ExpectColumnValueLengthsToEqual(
                column="Material Number",
                value=18,
                result_format=rf,
            )
        )

        # =============================================================
        # I. VARIABLE STRING LENGTH (RANGE)
        # =============================================================
        # Rule type: expect_column_value_lengths_to_be_between
        # Use when: "String length must be between min and max"
        # YAML equivalent:
        #   - type: expect_column_value_lengths_to_be_between
        #     columns: [Description]
        #     min_value: 5
        #     max_value: 200

        # Example: Description must be 5-200 characters
        self.expect(
            gx.expectations.ExpectColumnValueLengthsToBeBetween(
                column="Description",
                min_value=5,
                max_value=200,
                result_format=rf,
            )
        )

        # =============================================================
        # J. NUMERIC RANGE
        # =============================================================
        # Rule type: expect_column_values_to_be_between
        # Use when: "Numeric value must be within a range"
        # YAML equivalent:
        #   - type: expect_column_values_to_be_between
        #     columns: [Lead Time Days]
        #     min_value: 1
        #     max_value: 365

        # Example: Lead time must be 1-365 days
        self.expect(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="Lead Time Days",
                min_value=1,
                max_value=365,
                result_format=rf,
            )
        )

        # Example: Standard Price must be positive (> 0)
        self.expect(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="Standard Price",
                min_value=0.01,
                max_value=999999.99,
                result_format=rf,
            )
        )

        # =============================================================
        # K. UNIQUE VALUES (NO DUPLICATES)
        # =============================================================
        # Rule type: expect_column_values_to_be_unique
        # Use when: "Each value in this column must be unique"
        # YAML equivalent:
        #   - type: expect_column_values_to_be_unique
        #     columns: [Material Number, Global Product ID]

        # Example: Material Number should be unique
        self.expect(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="Material Number",
                result_format=rf,
            )
        )

        # =============================================================
        # L. COMPOSITE KEY UNIQUENESS
        # =============================================================
        # Rule type: expect_compound_columns_to_be_unique
        # Use when: "The combination of these columns must be unique"
        # YAML equivalent:
        #   - type: expect_compound_columns_to_be_unique
        #     column_list: [Plant, Storage Location, Material Number]

        # Example: Plant + Storage Location + Material should be unique
        self.expect(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["Plant", "Storage Location", "Material Number"],
                result_format=rf,
            )
        )


# =============================================================
# HOW TO RUN THIS VALIDATION
# =============================================================
if __name__ == "__main__":
    """
    Example of running this validation directly.
    In production, validations are run through the Streamlit UI.
    """
    print("Loading validation suite...")
    suite = YourValidation()

    print("Running validation...")
    results = suite.run()

    print(f"\nResults: {len(results)} expectations evaluated")

    # Count failures
    failures = [r for r in results if not r.get("success", True)]
    print(f"Failures: {len(failures)}")

    # Optionally save results
    # suite.save_results_to_file(results)
