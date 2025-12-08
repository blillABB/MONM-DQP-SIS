"""
Column-to-grain mapping for SAP MDM data.

This mapping is extracted from STAGE."sp_CreateProductDataTable" stored procedure
and defines which SAP table each column originates from, along with the unique key
combination (grain) for that table.

Used for deduplicating validation failures at the appropriate level before sending
to DataLark BAPI calls.
"""

# =============================================================================
# Grain Definitions
# Maps SAP table name to unique key columns
# =============================================================================
GRAIN_DEFINITIONS = {
    "MARA": ["MATERIAL_NUMBER"],  # Material Master Basic Data
    "MAKT": ["MATERIAL_NUMBER"],  # Material Description
    "ZPDM": ["MATERIAL_NUMBER"],  # Proprietary/Object Code
    "MARC": ["MATERIAL_NUMBER", "PLANT"],  # Material Master Plant Data
    "MVKE": ["MATERIAL_NUMBER", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"],  # Sales Org Data
    "MBEW": ["MATERIAL_NUMBER", "PLANT"],  # Valuation Data
    "MARD": ["MATERIAL_NUMBER", "PLANT", "STORAGE_LOCATION"],  # Storage Location Data
    "MLGT": ["MATERIAL_NUMBER", "WAREHOUSE_NUMBER"],  # Warehouse Management Data
    "ZHIERARCHY": ["PROFIT_CENTER"],  # Product Group Hierarchy
    "MATSALES": ["MATERIAL_NUMBER", "SALES_ORGANIZATION"],  # Sales History
    "PRICECODES": ["MATERIAL_NUMBER", "SALES_ORGANIZATION"],  # Price Codes
    "CATALOG": ["MATERIAL_NUMBER"],  # Catalog Indicators
}

# =============================================================================
# Column-to-Table Mapping
# Maps each column to its source SAP table
# =============================================================================
COLUMN_TO_TABLE = {
    # MARA - Material Master Basic Data (1:1 with Material)
    "MATERIAL_NUMBER": "MARA",
    "SPEC": "MARA",
    "MATERIAL_TYPE": "MARA",
    "INDUSTRY_SECTOR": "MARA",
    "ITEM_CATEGORY_GROUP": "MARA",
    "BASE_UNIT_OF_MEASURE": "MARA",
    "MATERIAL_GROUP": "MARA",
    "OLD_MATERIAL_NUMBER": "MARA",
    "DIVISION": "MARA",
    "INDUSTRY_STANDARD_DESC": "MARA",
    "GROSS_WEIGHT": "MARA",
    "NET_WEIGHT": "MARA",
    "WEIGHT_UNIT": "MARA",
    "VOLUME": "MARA",
    "VOLUME_UNIT": "MARA",
    "TOTAL_SHELF_LIFE_DAYS": "MARA",
    "STORAGE_TEMP_CONDITIONS": "MARA",
    "MANUFACTURER_NUMBER": "MARA",
    "MANUFACTURER_PART_NUMBER": "MARA",
    "LAB_OFFICE": "MARA",
    "CROSS_DIVISION_BATCH_MGMT": "MARA",
    "GLOBAL_PRODUCT_ID": "MARA",
    "CREATED_ON": "MARA",
    "CHANGED_ON": "MARA",
    "FRAME_GROUP": "MARA",
    "WARRANTY": "MARA",
    "PACK_INDICATOR": "MARA",
    "SIZE_DIMENSIONS": "MARA",
    "MINIMUM_REM_SHELF_LIFE_DAYS": "MARA",

    # MAKT - Material Description (1:1 with Material)
    "MATERIAL_DESCRIPTION": "MAKT",

    # ZPDM_MATREV - Proprietary/Object Code (1:1 with Material)
    "PROPRIETARY": "ZPDM",
    "PROPRIETARY_TYPE": "ZPDM",
    "OBJECT_CODE": "ZPDM",
    "OBJECT_CODE_EXT": "ZPDM",

    # MARC - Material Master Plant Data (1:1 with Material + Plant)
    "PLANT": "MARC",
    "PROFIT_CENTER": "MARC",
    "MRP_TYPE": "MARC",
    "SERIAL_NUMBER_PROFILE": "MARC",
    "PLANT_STATUS": "MARC",
    "MRP_CONTROLLER": "MARC",
    "PROCUREMENT_TYPE": "MARC",
    "PURCHASING_GROUP": "MARC",
    "PURCHASING_VALUE_GROUP": "MARC",
    "AVAILABILITY_CHECK": "MARC",
    "PRODUCTION_SCHEDULER": "MARC",
    "LOADING_GROUP": "MARC",
    "SPECIAL_PROCUREMENT_TYPE": "MARC",
    "STRATEGY_GROUP": "MARC",
    "PLANNED_DELIVERY_TIME": "MARC",
    "IN_HOUSE_PRODUCTION_TIME": "MARC",
    "PLANNING_TIME_FENCE": "MARC",
    "ALTERNATIVE_BOM": "MARC",
    "BOM_USAGE": "MARC",
    "FIXED_LOT_SIZE": "MARC",
    "MINIMUM_LOT_SIZE": "MARC",
    "MAXIMUM_LOT_SIZE": "MARC",
    "PROCUREMENT_INDICATOR": "MARC",
    "VALUATION_CATEGORY": "MARC",
    "HTS_CODE": "MARC",
    "EAN_NUMBER": "MARC",
    "COUNTRY_OF_ORIGIN": "MARC",
    "BATCH_MANAGEMENT": "MARC",
    "AUTOMATIC_PO": "MARC",
    "ABC_INDICATOR": "MARC",
    "PHYSICAL_INVENTORY_INDICATOR": "MARC",
    "GOODS_RECEIPT_TIME": "MARC",
    "MRP_GROUP": "MARC",
    "SAFETY_STOCK": "MARC",

    # MVKE - Sales Organization Data (1:1 with Material + Sales Org + Dist Channel)
    "SALES_ORGANIZATION": "MVKE",
    "DISTRIBUTION_CHANNEL": "MVKE",
    "SALES_STATUS": "MVKE",
    "PRODUCT_HIERARCHY": "MVKE",
    "MATERIAL_GROUP_1": "MVKE",
    "MATERIAL_GROUP_2": "MVKE",
    "MATERIAL_GROUP_3": "MVKE",
    "MATERIAL_GROUP_4": "MVKE",
    "MATERIAL_GROUP_5": "MVKE",
    "DELIVERING_PLANT": "MVKE",
    "SALES_ITEM_CATEGORY_GROUP": "MVKE",
    "DISTRIBUTION_INDICATOR": "MVKE",
    "PRICING_GROUP": "MVKE",
    "CASH_DISCOUNT": "MVKE",
    "MATERIAL_STATISTICS_GROUP": "MVKE",
    "VOLUME_REBATE_GROUP": "MVKE",
    "ACCOUNT_ASSIGNMENT_GROUP": "MVKE",
    "COMMISSION_GROUP": "MVKE",
    "OMS_FLAG": "MVKE",

    # MBEW - Valuation Data (1:1 with Material + Plant)
    "TOTAL_STOCK": "MBEW",
    "STANDARD_PRICE": "MBEW",
    "VALUATION_CLASS": "MBEW",

    # MARD - Storage Location Data (1:1 with Material + Plant + Storage Location)
    "CURRENT_INVENTORY": "MARD",
    "STORAGE_LOCATION": "MARD",

    # MLGT - Warehouse Management Data (1:1 with Material + Warehouse)
    "WAREHOUSE_NUMBER": "MLGT",
    "STORAGE_TYPE": "MLGT",

    # ZHIERARCHY - Product Group (1:1 with Profit Center)
    "PRODUCT_GROUP": "ZHIERARCHY",

    # vw_MatSales - Sales History (1:1 with Material + Sales Org)
    "LAST_SALES_DATE": "MATSALES",

    # ZSD_PRICE_CODES - Price Codes (1:1 with Material + Sales Org)
    "DISCOUNT_SYMBOL": "PRICECODES",
    "PRICE_GROUP_CODE": "PRICECODES",

    # DRAD - Catalog Indicators (1:1 with Material)
    "CATALOG_501": "CATALOG",
}


def get_grain_for_column(column_name: str) -> tuple[str, list[str]]:
    """
    Get the grain (unique key columns) for a given column.

    Parameters
    ----------
    column_name : str
        Name of the column to look up

    Returns
    -------
    tuple[str, list[str]]
        (table_name, unique_key_columns)
        Returns ("UNKNOWN", ["MATERIAL_NUMBER"]) if column not found

    Examples
    --------
    >>> get_grain_for_column("GROSS_WEIGHT")
    ("MARA", ["MATERIAL_NUMBER"])

    >>> get_grain_for_column("MRP_TYPE")
    ("MARC", ["MATERIAL_NUMBER", "PLANT"])

    >>> get_grain_for_column("PRICING_GROUP")
    ("MVKE", ["MATERIAL_NUMBER", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"])
    """
    table = COLUMN_TO_TABLE.get(column_name, "UNKNOWN")
    grain = GRAIN_DEFINITIONS.get(table, ["MATERIAL_NUMBER"])
    return table, grain


def get_grain_for_columns(column_names: list[str]) -> tuple[str, list[str]]:
    """
    Get the most granular grain required for a set of columns.

    When multiple columns are validated together, we need the most
    granular grain that captures all of them.

    Parameters
    ----------
    column_names : list[str]
        List of column names

    Returns
    -------
    tuple[str, list[str]]
        (combined_table_name, unique_key_columns)

    Examples
    --------
    >>> get_grain_for_columns(["GROSS_WEIGHT", "NET_WEIGHT"])
    ("MARA", ["MATERIAL_NUMBER"])

    >>> get_grain_for_columns(["GROSS_WEIGHT", "MRP_TYPE"])
    ("MARA+MARC", ["MATERIAL_NUMBER", "PLANT"])

    >>> get_grain_for_columns(["MRP_TYPE", "PRICING_GROUP"])
    ("MARC+MVKE", ["MATERIAL_NUMBER", "PLANT", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"])
    """
    if not column_names:
        return "UNKNOWN", ["MATERIAL_NUMBER"]

    # Get all grains
    grains = [get_grain_for_column(col) for col in column_names]

    # Find the most granular (longest unique key list)
    most_granular = max(grains, key=lambda x: len(x[1]))

    # If all columns are from same table, return that table name
    tables = [g[0] for g in grains]
    if len(set(tables)) == 1:
        return most_granular

    # Otherwise, combine table names
    combined_table = "+".join(sorted(set(tables)))
    return combined_table, most_granular[1]


def validate_grain_columns_exist(grain: list[str], available_columns: list[str]) -> bool:
    """
    Check if all grain columns are available in the DataFrame.

    Parameters
    ----------
    grain : list[str]
        Grain column names
    available_columns : list[str]
        Available columns in the DataFrame

    Returns
    -------
    bool
        True if all grain columns exist, False otherwise
    """
    return all(col in available_columns for col in grain)


def get_fallback_grain(grain: list[str], available_columns: list[str]) -> list[str]:
    """
    Get a fallback grain if the ideal grain is not available.

    Falls back to the most granular grain that exists in the DataFrame.

    Parameters
    ----------
    grain : list[str]
        Ideal grain column names
    available_columns : list[str]
        Available columns in the DataFrame

    Returns
    -------
    list[str]
        Fallback grain (always returns at least ["MATERIAL_NUMBER"])
    """
    # Always prefer MATERIAL_NUMBER if it exists
    if "MATERIAL_NUMBER" in available_columns:
        # Filter grain to only columns that exist
        available_grain = [col for col in grain if col in available_columns]
        if available_grain:
            return available_grain
        return ["MATERIAL_NUMBER"]

    # Worst case: no deduplication possible
    return []

def get_context_columns_for_column(column_name: str) -> list[str]:
    """
    Get the minimal set of context columns needed for a column based on its grain.

    This determines which organizational context columns are needed to uniquely
    identify failures for a given column. Only returns the columns required
    by the grain - no extra columns.

    Parameters
    ----------
    column_name : str
        Column name being validated

    Returns
    -------
    list[str]
        List of context column names needed for this column's grain

    Examples
    --------
    >>> get_context_columns_for_column("GROSS_WEIGHT")
    ["MATERIAL_NUMBER"]  # MARA grain

    >>> get_context_columns_for_column("MRP_TYPE")
    ["MATERIAL_NUMBER", "PLANT"]  # MARC grain

    >>> get_context_columns_for_column("PRICING_GROUP")
    ["MATERIAL_NUMBER", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"]  # MVKE grain
    """
    table, grain_columns = get_grain_for_column(column_name)
    return grain_columns


def get_context_columns_for_columns(column_names: list[str]) -> list[str]:
    """
    Get the union of all context columns needed for a set of columns.

    When validating multiple columns, we need to include context columns
    for all their grains (taking the union).

    Parameters
    ----------
    column_names : list[str]
        List of column names being validated

    Returns
    -------
    list[str]
        Unique list of all context columns needed

    Examples
    --------
    >>> get_context_columns_for_columns(["GROSS_WEIGHT", "MRP_TYPE"])
    ["MATERIAL_NUMBER", "PLANT"]  # Union of MARA + MARC grains

    >>> get_context_columns_for_columns(["GROSS_WEIGHT", "MRP_TYPE", "PRICING_GROUP"])
    ["MATERIAL_NUMBER", "PLANT", "SALES_ORGANIZATION", "DISTRIBUTION_CHANNEL"]
    """
    if not column_names:
        return ["MATERIAL_NUMBER"]

    # Collect context columns for each column
    all_context = set()
    for col in column_names:
        context_cols = get_context_columns_for_column(col)
        all_context.update(context_cols)

    # Return as sorted list for consistency
    return sorted(list(all_context))
