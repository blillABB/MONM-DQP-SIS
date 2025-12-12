import os
import pandas as pd
import snowflake.connector
from snowflake.connector.errors import DatabaseError
from core.config import SNOWFLAKE_CONFIG


def _extract_url(text: str) -> str:
    """Return the first HTTPS URL inside the given error message (if any)."""
    import re

    match = re.search(r"https?://[^\s]+", text)
    return match.group(0) if match else ""

# =============================================================================
# Snowflake connection
# =============================================================================
SNOWFLAKE_CONN_TEMPLATE = (
    "account={account};user={user};role={role};warehouse={warehouse};"
    "database={database};schema={schema};authenticator=externalbrowser;token_cache={use_token_cache}"
)


def snowflake_config_summary(config=None):
    """Return the active Snowflake connection parameters (non-sensitive)."""
    cfg = config or SNOWFLAKE_CONFIG
    return {
        "account": cfg.get("account", ""),
        "user": cfg.get("user", ""),
        "role": cfg.get("role", ""),
        "warehouse": cfg.get("warehouse", ""),
        "database": cfg.get("database", ""),
        "schema": cfg.get("schema", ""),
        "use_token_cache": cfg.get("client_store_temporary_credential", False),
    }


def ensure_snowflake_config(config=None):
    """Validate required Snowflake settings so auth failures surface clearly."""
    summary = snowflake_config_summary(config)
    missing = [k for k, v in summary.items() if k != "use_token_cache" and not str(v).strip()]
    placeholders = [k for k, v in summary.items() if isinstance(v, str) and v.strip().startswith("<")]
    if missing:
        missing_keys = ", ".join(missing)
        raise RuntimeError(
            "❌ Missing Snowflake configuration. Set the following via environment variables "
            f"or Streamlit secrets: {missing_keys}."
        )
    if placeholders:
        raise RuntimeError(
            "❌ Snowflake configuration still uses placeholder values. Update core/config.py "
            f"for: {', '.join(placeholders)}."
        )
    return summary


# =============================================================================
# Query Function Registry
# Maps YAML data_source strings to actual query functions
# =============================================================================
QUERY_REGISTRY = {}


def register_query(name: str):
    """Decorator to register a query function in the registry."""
    def decorator(func):
        QUERY_REGISTRY[name] = func
        return func
    return decorator


def get_query_function(name: str):
    """Get a query function by name from the registry."""
    if name not in QUERY_REGISTRY:
        available = ", ".join(QUERY_REGISTRY.keys())
        raise ValueError(f"Unknown data source: '{name}'. Available: {available}")
    return QUERY_REGISTRY[name]


def get_connection():
    """
    Establish Snowflake connection and explicitly activate the warehouse.

    The warehouse parameter in the single Snowflake config sets the default but may not
    always activate it automatically. We explicitly USE the warehouse to ensure
    queries can execute.

    If warehouse activation fails, an error is raised since queries cannot
    execute without an active warehouse.
    """
    config = SNOWFLAKE_CONFIG
    ensure_snowflake_config(config)

    try:
        summary = snowflake_config_summary(config)
        print(
            "▶ Connecting to Snowflake with: "
            f"account={summary['account']} "
            f"user={summary['user']} "
            f"role={summary['role']} "
            f"warehouse={summary['warehouse']} "
            f"database={summary['database']} "
            f"schema={summary['schema']} "
            f"token_cache={summary['use_token_cache']}"
        )
        print(
            "▶ Snowflake connection string template: "
            + SNOWFLAKE_CONN_TEMPLATE.format(**summary)
        )
        conn = snowflake.connector.connect(**config)
    except DatabaseError as e:
        message = str(e)
        lower_message = message.lower()

        if "user you were trying to authenticate as differs" in lower_message:
            raise RuntimeError(
                "❌ Snowflake SSO mismatch detected. The IdP session is logged in as a different "
                "user than SNOWFLAKE_USER. Sign out of your SSO session (or use an incognito "
                "browser window) and retry, or set SNOWFLAKE_USER to match the active IdP user."
            ) from e

        # When the Snowflake connector fails to launch the external browser (or the window
        # opens blank), the raw DatabaseError only surfaces in the terminal. Streamlit users
        # end up staring at an empty screen with no hint about what to do. Detect browser
        # launch/auth failures and surface a copyable login URL plus next steps.
        if any(keyword in lower_message for keyword in ["browser", "oauth", "external"]):
            auth_url = _extract_url(message)
            guidance = (
                "❌ Snowflake SSO could not start in your browser. "
                "Copy/paste the URL below into a new tab to complete login, then rerun the suite."
            )
            if auth_url:
                guidance += f"\n\nLogin URL: {auth_url}"
            else:
                guidance += (
                    "\n\nNo login URL was returned by Snowflake. Check that a default browser is "
                    "available or set the BROWSER environment variable to a valid executable."
                )
            raise RuntimeError(guidance) from e

        raise RuntimeError(f"❌ Snowflake connection failed: {message}") from e
    except Exception as e:
        raise RuntimeError(f"❌ Snowflake connection failed: {e}") from e

    # Explicitly activate the warehouse
    warehouse = config.get("warehouse")
    if warehouse:
        cursor = conn.cursor()
        try:
            print(f"▶ Activating warehouse: {warehouse}")
            cursor.execute(f'USE WAREHOUSE "{warehouse}"')
            print(f"✅ Warehouse '{warehouse}' activated successfully")
        except Exception as e:
            conn.close()
            raise RuntimeError(
                f"❌ Failed to activate warehouse '{warehouse}': {e}\n"
                f"The role may not have USAGE privilege on this warehouse.\n"
                f"Set SNOWFLAKE_WAREHOUSE environment variable to use a different warehouse."
            ) from e
        finally:
            cursor.close()
    else:
        conn.close()
        raise RuntimeError(
            "❌ No warehouse configured. Set SNOWFLAKE_WAREHOUSE environment variable."
        )

    return conn

def run_query(sql: str) -> pd.DataFrame:
    """
    Execute a SQL query against Snowflake.

    Args:
        sql: SQL query to execute

    Returns:
        DataFrame of results
    """
    conn = get_connection()
    try:
        print("▶ Fetching query results...")
        df = pd.read_sql(sql, conn)

        if df is None:
            df = pd.DataFrame()

        print(f"✅ Retrieved {len(df)} rows from Snowflake")
        return df
    finally:
        conn.close()

@register_query("get_aurora_motor_dataframe")
def get_aurora_motor_dataframe(limit: int = None, offset: int = None) -> pd.DataFrame:
    """
    Fetch Aurora Motors product data.

    Supports chunking via limit/offset parameters.
    """
    sql = """
        SELECT * FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
        WHERE PRODUCT_HIERARCHY <> '522475549547036170'
        AND PRICING_GROUP = 'AM'
        AND SALES_ORGANIZATION = 'BEC'
        AND PLANT = '00A'
        AND STORAGE_LOCATION = '0001'
        AND STORAGE_TYPE = '001'
    """
    if limit is not None:
        sql += f"\nLIMIT {limit}"
    if offset is not None:
        sql += f"\nOFFSET {offset}"

    return run_query(sql)

@register_query("get_level_1_dataframe")
def get_level_1_dataframe(limit: int = 1000, offset: int = None) -> pd.DataFrame:
    """
    Fetch Level 1 validation data (first 1000 rows for testing).

    Fixed size query - limit defaults to 1000 to indicate this is a test dataset
    that should not be chunked dynamically.
    """
    sql = """
        SELECT * FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
    """
    if limit is not None:
        sql += f"\nLIMIT {limit}"
    if offset is not None:
        sql += f"\nOFFSET {offset}"

    return run_query(sql)

@register_query("get_mg4_dataframe")
def get_mg4_dataframe(limit: int = None, offset: int = None) -> pd.DataFrame:
    """
    Return vw_MatAll records with MG4_EXPECTED derived from CASE logic.
    This mirrors the SQL used for MG4 - End Item Definition validation.

    Supports chunking via limit/offset parameters.
    """
    sql = """
    SELECT *
    FROM (
        SELECT DISTINCT
            MATERIAL_NUMBER,
            SALES_ORGANIZATION,
            PLANT,
            DELIVERING_PLANT,
            OBJECT_CODE,
            OBJECT_CODE_EXT,
            LAST_SALES_DATE,
            SALES_STATUS,
            PROFIT_CENTER,
            Z01_MKT_MTART AS MATERIAL_TYPE,
            MATERIAL_GROUP_2,
            MATERIAL_GROUP_3,
            MATERIAL_GROUP_4,
            CASE
                WHEN OBJECT_CODE = 'AA' THEN 'RAE'
                WHEN OBJECT_CODE = 'AR' AND OBJECT_CODE_EXT = 'RA' THEN 'RAE'
                WHEN OBJECT_CODE = 'RA' THEN 'RAE'
                WHEN OBJECT_CODE = 'RC' THEN 'RAE'
                WHEN OBJECT_CODE = 'BD' AND OBJECT_CODE_EXT = 'RO' THEN 'RAE'
                WHEN OBJECT_CODE = 'DR' AND OBJECT_CODE_EXT = 'AA' THEN 'RAE'
                WHEN OBJECT_CODE = 'GE' AND OBJECT_CODE_EXT = 'RA' THEN 'RAE'
                WHEN OBJECT_CODE = 'AR' AND OBJECT_CODE_EXT = 'PT'
                     AND MATERIAL_NUMBER ILIKE '%RA%' THEN 'RAE'

                WHEN OBJECT_CODE = 'AR' AND OBJECT_CODE_EXT = 'SA' THEN 'SAE'
                WHEN OBJECT_CODE = 'AR' AND OBJECT_CODE_EXT = 'WS' THEN 'SAE'
                WHEN OBJECT_CODE = 'SA' THEN 'SAE'
                WHEN OBJECT_CODE = 'WS' THEN 'SAE'
                WHEN OBJECT_CODE = 'PR' THEN 'SAE'
                WHEN OBJECT_CODE = 'BD' AND OBJECT_CODE_EXT = 'SA' THEN 'SAE'
                WHEN OBJECT_CODE = 'DR' AND OBJECT_CODE_EXT = 'SA' THEN 'SAE'
                WHEN OBJECT_CODE = 'GE' AND OBJECT_CODE_EXT = 'SA' THEN 'SAE'
                WHEN OBJECT_CODE = 'AR' AND OBJECT_CODE_EXT = 'PT'
                     AND MATERIAL_NUMBER ILIKE '%SA%' THEN 'SAE'

                WHEN PROFIT_CENTER IN (
                    '59901002', '59004222', '59004206', '59004064', '59004054',
                    '59004044', '59004021', '59004016', '50012193', '40000482',
                    '40000282', '40000182', '6682133', '5582133'
                ) THEN 'IEC'

                WHEN MATERIAL_GROUP_3 = 'PAR' THEN 'SVC'
                WHEN PROFIT_CENTER LIKE '%079' THEN 'SVC'

                WHEN Z01_MKT_MTART IN ('CAT', 'FERT') THEN 'END'

                ELSE 'AUX'
            END AS MG4_EXPECTED
        FROM PROD_MO_MONM.REPORTING."vw_MatAll"
    ) AS sub
    WHERE MATERIAL_GROUP_2 IN ('MTR', 'LFH', 'ERH')
      AND DELIVERING_PLANT = PLANT
      AND SALES_STATUS NOT IN ('13', 'DR')
    """
    if limit is not None:
        sql += f"\nLIMIT {limit}"
    if offset is not None:
        sql += f"\nOFFSET {offset}"

    return run_query(sql)

@register_query("abb_shop_data")
def abb_shop_data(limit: int = None, offset: int = None) -> pd.DataFrame:
    """
    Auto-generated by the Streamlit query builder.
    Update the SQL or docstring as needed before committing.

    Supports chunking via limit/offset parameters.
    """
    sql = """SELECT DISTINCT MATERIAL_NUMBER, SALES_ORGANIZATION, DISTRIBUTION_CHANNEL, WAREHOUSE_NUMBER, STORAGE_LOCATION, STORAGE_TYPE, ABP_ELECTRICALDATA1, ABP_EFFICIENCYLEVEL, ABP_INSULATIONCLASS, ABP_NEMADESIGNCODE, ABP_NUMBEROFPHASES, ABP_NUMBEROFPOLES, ABP_SERVICEFACTOR, ABP_TYPEOFDUTY, ABP_ENCLOSURETYPE, ABP_FRAMESIZE, ABP_MOTORBASETYPE, ABP_MOUNTINGORIENTATION, ABP_BRAKEPRESENT, ABP_EXPLOSIONPROTECTION, ABP_CERTIFICATIONAGENCY, ABP_EXPPROGROCLA, ABP_DSTR_OUTPUT, ABP_DSTR_SSPEED, ABP_DSTR_VOLTAGE, ABP_DSTR_FREQUENCY
FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
WHERE PRODUCT_HIERARCHY LIKE '5%'
    AND OMS_FLAG = 'Y'"""
    if limit is not None:
        sql += f"\nLIMIT {limit}"
    if offset is not None:
        sql += f"\nOFFSET {offset}"

    return run_query(sql)

def get_column_metadata() -> dict:
    """
    Get column metadata from vw_ProductDataAll including column names,
    data types, and distinct values (for low-cardinality columns).

    Returns a dict with:
    - columns: list of column names
    - column_types: dict mapping column name to data type
    - distinct_values: dict mapping column name to list of distinct values (only for columns with <100 distinct values)
    """
    conn = get_connection()
    try:
        # Get column names and types
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM PROD_MO_MONM.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'REPORTING'
            AND TABLE_NAME = 'vw_ProductDataAll'
            ORDER BY ORDINAL_POSITION
        """)

        columns_info = cursor.fetchall()
        columns = [row[0] for row in columns_info]
        column_types = {row[0]: row[1] for row in columns_info}

        # Get distinct values for low-cardinality columns
        # We'll limit this to columns with fewer than 100 distinct values
        distinct_values = {}

        print(f"▶ Fetching distinct values for {len(columns)} columns...")
        print(f"⏱️ This may take several minutes. Processing all columns to completion...")

        for idx, col in enumerate(columns):
            try:
                # Progress indicator
                if idx % 10 == 0:
                    print(f"  📊 Progress: {idx}/{len(columns)} columns processed...")

                # First check the distinct count
                count_query = f"""
                    SELECT COUNT(DISTINCT {col}) as distinct_count
                    FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
                    LIMIT 1
                """

                cursor.execute(count_query)
                result = cursor.fetchone()

                if not result:
                    print(f"  ⚠️ {col}: No result from count query (skipped)")
                    continue

                distinct_count = result[0]

                # Only fetch distinct values if count is reasonable
                if distinct_count < 100:
                    values_query = f"""
                        SELECT DISTINCT {col}
                        FROM PROD_MO_MONM.REPORTING."vw_ProductDataAll"
                        WHERE {col} IS NOT NULL
                        ORDER BY {col}
                        LIMIT 100
                    """

                    cursor.execute(values_query)
                    values = [row[0] for row in cursor.fetchall()]
                    distinct_values[col] = values
                    print(f"  ✅ {col}: {len(values)} distinct values")
                else:
                    print(f"  ⚠️ {col}: {distinct_count} distinct values (skipped)")

            except Exception as e:
                print(f"  ❌ {col}: Error - {str(e)[:100]} (skipped)")
                continue

        cursor.close()

        print(f"✅ Column metadata fetch complete: {len(columns)} columns, {len(distinct_values)} with distinct values")

        return {
            "columns": columns,
            "column_types": column_types,
            "distinct_values": distinct_values
        }
    finally:
        conn.close()

