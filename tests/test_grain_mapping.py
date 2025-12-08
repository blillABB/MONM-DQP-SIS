"""
Tests for grain mapping functionality.
"""

import pytest
from core.grain_mapping import (
    get_grain_for_column,
    get_grain_for_columns,
    validate_grain_columns_exist,
    get_fallback_grain,
    COLUMN_TO_TABLE,
    GRAIN_DEFINITIONS,
)


class TestGrainMapping:
    """Test grain mapping from SAP tables."""

    def test_mara_column_grain(self):
        """MARA columns should have material-level grain."""
        table, grain = get_grain_for_column("GROSS_WEIGHT")
        assert table == "MARA"
        assert grain == ["MATERIAL_NUMBER"]

    def test_marc_column_grain(self):
        """MARC columns should have material-plant grain."""
        table, grain = get_grain_for_column("MRP_TYPE")
        assert table == "MARC"
        assert grain == ["MATERIAL_NUMBER", "PLANT"]

    def test_mvke_column_grain(self):
        """MVKE columns should have material-sales org-dist channel grain."""
        table, grain = get_grain_for_column("PRICING_GROUP")
        assert table == "MVKE"
        assert grain == ["MATERIAL_NUMBER", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"]

    def test_mbew_column_grain(self):
        """MBEW columns should have material-plant grain."""
        table, grain = get_grain_for_column("STANDARD_PRICE")
        assert table == "MBEW"
        assert grain == ["MATERIAL_NUMBER", "PLANT"]

    def test_unknown_column_fallback(self):
        """Unknown columns should fallback to material-level grain."""
        table, grain = get_grain_for_column("UNKNOWN_COLUMN")
        assert table == "UNKNOWN"
        assert grain == ["MATERIAL_NUMBER"]

    def test_multiple_columns_same_table(self):
        """Multiple columns from same table should return single table grain."""
        table, grain = get_grain_for_columns(["GROSS_WEIGHT", "NET_WEIGHT"])
        assert table == "MARA"
        assert grain == ["MATERIAL_NUMBER"]

    def test_multiple_columns_different_tables(self):
        """Multiple columns from different tables should return most granular grain."""
        table, grain = get_grain_for_columns(["GROSS_WEIGHT", "MRP_TYPE"])
        # Should pick the more granular MARC grain
        assert "MARC" in table
        assert set(grain) == {"MATERIAL_NUMBER", "PLANT"}

    def test_validate_grain_columns_exist_success(self):
        """Should return True when all grain columns exist."""
        available = ["MATERIAL_NUMBER", "PLANT", "SALES_ORGANIZATION"]
        assert validate_grain_columns_exist(["MATERIAL_NUMBER"], available) is True
        assert validate_grain_columns_exist(["MATERIAL_NUMBER", "PLANT"], available) is True

    def test_validate_grain_columns_exist_failure(self):
        """Should return False when grain columns are missing."""
        available = ["MATERIAL_NUMBER"]
        assert validate_grain_columns_exist(["MATERIAL_NUMBER", "PLANT"], available) is False

    def test_fallback_grain_with_material_number(self):
        """Should use available grain columns when MATERIAL_NUMBER exists."""
        available = ["MATERIAL_NUMBER", "PLANT"]
        ideal_grain = ["MATERIAL_NUMBER", "PLANT", "SALES_ORGANIZATION"]
        fallback = get_fallback_grain(ideal_grain, available)
        assert fallback == ["MATERIAL_NUMBER", "PLANT"]

    def test_fallback_grain_only_material(self):
        """Should fallback to MATERIAL_NUMBER only if other columns missing."""
        available = ["MATERIAL_NUMBER", "SOME_OTHER_COLUMN"]
        ideal_grain = ["MATERIAL_NUMBER", "PLANT"]
        fallback = get_fallback_grain(ideal_grain, available)
        assert fallback == ["MATERIAL_NUMBER"]

    def test_fallback_grain_no_material_number(self):
        """Should return empty list if MATERIAL_NUMBER not available."""
        available = ["SOME_COLUMN"]
        ideal_grain = ["MATERIAL_NUMBER", "PLANT"]
        fallback = get_fallback_grain(ideal_grain, available)
        assert fallback == []


class TestSAPTableCoverage:
    """Verify all expected SAP columns are mapped."""

    def test_common_mara_columns_mapped(self):
        """Verify common MARA columns are in mapping."""
        mara_columns = [
            "MATERIAL_NUMBER",
            "MATERIAL_TYPE",
            "GROSS_WEIGHT",
            "NET_WEIGHT",
            "WEIGHT_UNIT",
            "BASE_UNIT_OF_MEASURE",
        ]
        for col in mara_columns:
            assert col in COLUMN_TO_TABLE
            assert COLUMN_TO_TABLE[col] == "MARA"

    def test_common_marc_columns_mapped(self):
        """Verify common MARC columns are in mapping."""
        marc_columns = [
            "PLANT",
            "MRP_TYPE",
            "PROCUREMENT_TYPE",
            "PURCHASING_GROUP",
            "MRP_CONTROLLER",
        ]
        for col in marc_columns:
            assert col in COLUMN_TO_TABLE
            assert COLUMN_TO_TABLE[col] == "MARC"

    def test_common_mvke_columns_mapped(self):
        """Verify common MVKE columns are in mapping."""
        mvke_columns = [
            "SALES_ORGANIZATION",
            "DISTRIBUTION_CHANNEL",
            "PRICING_GROUP",
            "MATERIAL_GROUP_1",
            "MATERIAL_GROUP_4",
        ]
        for col in mvke_columns:
            assert col in COLUMN_TO_TABLE
            assert COLUMN_TO_TABLE[col] == "MVKE"

    def test_all_tables_have_grain_definition(self):
        """All tables in column mapping should have grain definition."""
        tables = set(COLUMN_TO_TABLE.values())
        for table in tables:
            assert table in GRAIN_DEFINITIONS, f"Table {table} missing grain definition"
