"""
Column metadata caching for vw_ProductDataAll.

This module provides caching functionality to avoid repeatedly querying
Snowflake for column names and distinct values.

The cache is persistent and does NOT auto-refresh. Users must manually
refresh via the UI button when they need updated column information.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from core.queries import get_column_metadata

# Store cache in validation_results directory which is mounted as a Docker volume
CACHE_FILE = Path(__file__).parent.parent / "validation_results" / "column_metadata_cache.json"


def get_fallback_column_metadata():
    """
    Fallback column metadata when Snowflake is unavailable.
    Returns hardcoded column list from vw_ProductDataAll.
    """
    columns = [
        "MATERIAL_NUMBER", "SALES_ORGANIZATION", "PLANT", "DISTRIBUTION_CHANNEL",
        "WAREHOUSE_NUMBER", "STORAGE_LOCATION", "STORAGE_TYPE", "ABC_INDICATOR",
        "ACCOUNT_ASSIGNMENT_GROUP", "ALTERNATIVE_BOM", "AUTOMATIC_PO", "AVAILABILITY_CHECK",
        "BASE_UNIT_OF_MEASURE", "BATCH_MANAGEMENT", "BOM_USAGE", "CASH_DISCOUNT",
        "CATALOG_501", "CHANGED_ON", "COMMISSION_GROUP", "COUNTRY_OF_ORIGIN",
        "CREATED_ON", "CURRENT_INVENTORY", "DELIVERING_PLANT", "DIVISION",
        "DISTRIBUTION_INDICATOR", "EAN_NUMBER", "GLOBAL_PRODUCT_ID", "GOODS_RECEIPT_TIME",
        "GROSS_WEIGHT", "HTS_CODE", "IN_HOUSE_PRODUCTION_TIME", "INDUSTRY_SECTOR",
        "INDUSTRY_STANDARD_DESC", "ITEM_CATEGORY_GROUP", "LAB_OFFICE", "LAST_SALES_DATE",
        "LOADING_GROUP", "MANUFACTURER_NUMBER", "MANUFACTURER_PART_NUMBER", "MATERIAL_DESCRIPTION",
        "MATERIAL_GROUP", "MATERIAL_GROUP_1", "MATERIAL_GROUP_2", "MATERIAL_GROUP_3",
        "MATERIAL_GROUP_4", "MATERIAL_GROUP_5", "MATERIAL_STATISTICS_GROUP", "MAXIMUM_LOT_SIZE",
        "MINIMUM_LOT_SIZE", "MINIMUM_REM_SHELF_LIFE_DAYS", "MRP_CONTROLLER", "MRP_GROUP",
        "MRP_TYPE", "NET_WEIGHT", "OBJECT_CODE", "OBJECT_CODE_EXT",
        "OLD_MATERIAL_NUMBER", "OMS_FLAG", "PACK_INDICATOR", "PHYSICAL_INVENTORY_INDICATOR",
        "PLANNED_DELIVERY_TIME", "PLANNING_TIME_FENCE", "PRICE_GROUP_CODE", "PRICING_GROUP",
        "PROCUREMENT_INDICATOR", "PROCUREMENT_TYPE", "PRODUCT_GROUP", "PRODUCT_HIERARCHY",
        "PRODUCTION_SCHEDULER", "PROFIT_CENTER", "PROPRIETARY", "PROPRIETARY_TYPE",
        "PURCHASING_GROUP", "PURCHASING_VALUE_GROUP", "SAFETY_STOCK", "SALES_STATUS",
        "SALES_ITEM_CATEGORY_GROUP", "SERIAL_NUMBER_PROFILE", "SIZE_DIMENSIONS", "SPECIAL_PROCUREMENT_TYPE",
        "SPEC", "STANDARD_PRICE", "STORAGE_TEMP_CONDITIONS", "STRATEGY_GROUP",
        "TOTAL_SHELF_LIFE_DAYS", "TOTAL_STOCK", "VALUATION_CATEGORY", "VALUATION_CLASS",
        "VOLUME", "VOLUME_REBATE_GROUP", "VOLUME_UNIT", "WARRANTY",
        "WEIGHT_UNIT", "ENG_CHANGE_NUMBER", "ENG_CATALOG_SPEC", "ENG_ELECTRICAL_SPEC",
        "ENG_AMPS", "ENG_INSULATION_CLASS", "ENG_NEMA_DESIGN_CODE", "ENG_PHASE",
        "ENG_POLES", "ENG_DUTY_TYPE", "ENG_RPM", "ENG_EFFICIENCY_LEVEL_RAW",
        "ENG_EFFICIENCY_LEVEL", "ENG_SERVICE_FACTOR", "ENG_OUTPUT", "ENG_FREQUENCY",
        "ENG_VOLTAGE", "ENG_ENCLOSURE", "ENG_FRAME_SIZE", "ENG_MOUNTING_TYPE",
        "ENG_MOUNTING_ORIENTATION", "ENG_BRAKE_CODE", "ENG_HAZ_DIVISION_IND", "ENG_EXPLOSION_PROOF_IND",
        "ENG_CLASS_GROUP_RAW", "ENG_XP_CLI", "ENG_XP_CLII", "ENG_XP_GPA",
        "ENG_XP_GPB", "ENG_XP_GPC", "ENG_XP_GPD", "ENG_XP_GPE",
        "ENG_XP_GPF", "ENG_XP_GPG", "ENG_AGENCY_LOGOS_RAW", "ENG_AGENCY_LOGOS_READABLE",
        "Z01_MKT_501_MODEL_CODE", "Z01_MKT_501_PREFIX", "Z01_MKT_501_SUFFIX", "Z01_MKT_ABB_CLASS_CID",
        "Z01_MKT_ABB_CLASS_NAME", "Z01_MKT_AGENCY_LOGOS", "Z01_MKT_AMBIENT_TEMPERATURE", "Z01_MKT_AUX_BOX",
        "Z01_MKT_AUX_BOX_LEAD_TERMINATION", "Z01_MKT_BASE_INDICATOR", "Z01_MKT_BEARING_DE", "Z01_MKT_BEARING_GREASE_TYPE",
        "Z01_MKT_BEARING_ODE", "Z01_MKT_BLOWER", "Z01_MKT_BRAKE_INDICATOR", "Z01_MKT_BRG_RTD",
        "Z01_MKT_BRG_RTD_QTY", "Z01_MKT_BRAND", "Z01_MKT_CABLE_LENGTH", "Z01_MKT_CATALOG_NUMBER",
        "Z01_MKT_COMPLEXITY_LEVEL", "Z01_MKT_CONNECTION_DIAGRAM", "Z01_MKT_CURRENT_AT_VOLTAGE", "Z01_MKT_DATE",
        "Z01_MKT_DATA_SOURCE_RULE", "Z01_MKT_DESIGN_CODE", "Z01_MKT_DESCRIPTION", "Z01_MKT_DIMENSION_DRAWINGS",
        "Z01_MKT_DRIP_COVER", "Z01_MKT_DUTY", "Z01_MKT_EFFICIENCY_100_LOAD", "Z01_MKT_EFFICIENCY_COMPLIANCY",
        "Z01_MKT_EFFICIENCY_LEVEL", "Z01_MKT_ELEC_CONFIG", "Z01_MKT_ELEC_ISOLATED_BEARING", "Z01_MKT_ELEC_RATING",
        "Z01_MKT_ELEC_SPEC", "Z01_MKT_ELECT_RTNG_CURRENT", "Z01_MKT_ELECT_RTNG_FREQ", "Z01_MKT_ELECT_RTNG_OUTPUT",
        "Z01_MKT_ELECT_RTNG_RPM", "Z01_MKT_ELECT_RTNG_VOLTAGE", "Z01_MKT_ENCLOSURE", "Z01_MKT_ENCLOSURE_TYPE",
        "Z01_MKT_FEEDBACK_DEVICE", "Z01_MKT_FLA_HIGHVOLTAGE", "Z01_MKT_FRAME", "Z01_MKT_FRAME_FAMILY",
        "Z01_MKT_FRAME_MATERIAL", "Z01_MKT_FRAME_SIZE", "Z01_MKT_FRAME_SUFFIX", "Z01_MKT_FREQUENCY",
        "Z01_MKT_FREQUENCY_STR", "Z01_MKT_FRONT_FACE_CODE", "Z01_MKT_FRONT_SHAFT", "Z01_MKT_HEATER",
        "Z01_MKT_INVERTER_CODE", "Z01_MKT_INSULATION_CLASS", "Z01_MKT_KOBX_MATERIAL", "Z01_MKT_KVA_CODE",
        "Z01_MKT_LAST_UPDATE", "Z01_MKT_LETTER_TYPE", "Z01_MKT_LETTER_TYPE_CODE", "Z01_MKT_LIFTING_LUGS",
        "Z01_MKT_LOCKED_BEARING", "Z01_MKT_MATNR", "Z01_MKT_MECH_SPEC", "Z01_MKT_MODEL",
        "Z01_MKT_MOTOR_LEAD_EXIT", "Z01_MKT_MOTOR_LEAD_TERMINATION", "Z01_MKT_MOTOR_LEADS", "Z01_MKT_MOTOR_TYPE",
        "Z01_MKT_MOUNTING_ARRANGEMENT", "Z01_MKT_MOUNTING_ORIENT", "Z01_MKT_MTART", "Z01_MKT_MULT_SYM",
        "Z01_MKT_MVGR1", "Z01_MKT_MVGR2", "Z01_MKT_MVGR3", "Z01_MKT_MVGR4",
        "Z01_MKT_NAMEPLATE_AMPS", "Z01_MKT_NAMEPLATE_FREQUENCY", "Z01_MKT_NAMEPLATE_OUTPUT", "Z01_MKT_NAMEPLATE_SPEED",
        "Z01_MKT_NAMEPLATE_VOLTAGE", "Z01_MKT_NEMA_PLATFORM", "Z01_MKT_NRCAN_COMPLIANT", "Z01_MKT_NUMBER_POLES",
        "Z01_MKT_NUMBER_POLES_STR", "Z01_MKT_OBJCODE", "Z01_MKT_OUTPUT", "Z01_MKT_OUTPUT_AT_FREQUENCY",
        "Z01_MKT_OUTPUT_AT_SPEED", "Z01_MKT_OVERALL_LENGTH_C_DIM", "Z01_MKT_PACKING_CRATE_IND", "Z01_MKT_PHASE",
        "Z01_MKT_PLANT", "Z01_MKT_PLANT_LOCATION", "Z01_MKT_POWER_FACTOR", "Z01_MKT_POWER_TYPE",
        "Z01_MKT_PROD_FAMILY", "Z01_MKT_PROD_NET_WEIGHT", "Z01_MKT_PRODUCT_CATEGORY", "Z01_MKT_PRODUCT_FAMILY",
        "Z01_MKT_PRODUCT_GROUP_MONM", "Z01_MKT_PRODUCT_LINE", "Z01_MKT_PULLEY_END_BEARING_TYPE", "Z01_MKT_PULLEY_END_FACE_CODE",
        "Z01_MKT_PULLEY_SHAFT", "Z01_MKT_RODENT_SCREEN", "Z01_MKT_SALES_STATUS", "Z01_MKT_SERVICE_FACTOR",
        "Z01_MKT_SHAFT_DIAMETER", "Z01_MKT_SHAFT_EXTENSION_LOCATION", "Z01_MKT_SHAFT_FEEDBACK_IND", "Z01_MKT_SHAFT_GROUND",
        "Z01_MKT_SHAFT_MTL", "Z01_MKT_SHAFT_ROTATION", "Z01_MKT_SHAFT_SLINGER", "Z01_MKT_SPEED",
        "Z01_MKT_SPEED_AT_FREQUENCY", "Z01_MKT_SPEED_CODE", "Z01_MKT_SPECIFICATION_NUMBER", "Z01_MKT_SPECIAL_PAINT",
        "Z01_MKT_STARTING_METHOD", "Z01_MKT_STANDARD", "Z01_MKT_SYNCHRONOUS_SPEED", "Z01_MKT_SYNCHRONOUS_SPEED_AT_FREQ",
        "Z01_MKT_SYNCHRONOUS_SPEED_STR", "Z01_MKT_THERMAL_DEVICE_BEARING", "Z01_MKT_THERMAL_DEVICE_WINDING", "Z01_MKT_VISIBLE_501CATALOG",
        "Z01_MKT_VISIBLE_WEB", "Z01_MKT_VIBRATION_SENSOR", "Z01_MKT_VOLTAGE", "Z01_MKT_VOLTAGE_AT_FREQUENCY",
        "Z01_MKT_VOLTAGE_STR", "Z01_MKT_WDG_RTD_PRESENT", "Z01_MKT_WDG_RTD_QTY", "Z01_MKT_WDG_RTD_TYPE",
        "Z01_MKT_WDG_THRMST", "Z01_MKT_WDG_THRMST_CTCTS", "Z01_MKT_WDG_THRMCPL", "Z01_MKT_WDG_THRMSTR",
        "Z01_MKT_WINDING_THERMAL1", "Z01_MKT_WINDING_THERMAL2", "Z01_MKT_XP_CLASS_GROUP", "Z01_MKT_XP_CLII",
        "Z01_MKT_XP_CLI", "Z01_MKT_XP_DIVISION", "Z01_MKT_XP_GPA", "Z01_MKT_XP_GPB",
        "Z01_MKT_XP_GPC", "Z01_MKT_XP_GPD", "Z01_MKT_XP_GPE", "Z01_MKT_XP_GPF",
        "Z01_MKT_XP_GPG", "Z01_MKT_XP_IND", "ABP_ABBPRODFAM", "ABP_ACTUALRATIO",
        "ABP_AGMASIZE", "ABP_AMBIENTTEMPERATURE", "ABP_ANGULARMISALIGNMENT", "ABP_ATEXCERTIFICATION",
        "ABP_AXIALMISALIGNMENT", "ABP_BACKSTOPINCLUDED", "ABP_BASETOCENTERHEIGHT", "ABP_BASTOOUTCENHEI",
        "ABP_BEARING", "ABP_BEARINGFAMILY", "ABP_BEARINGNDE", "ABP_BEARINGSERIES",
        "ABP_BEARINGTYPE", "ABP_BELREIMAT", "ABP_BELTHEIGHT", "ABP_BELTLENGTH",
        "ABP_BELTTYPE", "ABP_BELTWIDTH", "ABP_BOLTCIRCLE", "ABP_BOLTSIZE",
        "ABP_BOLTTOBOLT", "ABP_BOLTWIDTH", "ABP_BOREDIAMETER", "ABP_BOREFIT",
        "ABP_BOREFITSTANDARD", "ABP_BORELENGTH", "ABP_BRAKEPRESENT", "ABP_BRANDLABEL",
        "ABP_BUSHINGSIZE", "ABP_BUSHINGTYPE", "ABP_CASESIZE", "ABP_CATALOGNUMBER",
        "ABP_CENTERDISTANCE", "ABP_CENTOENDOFSHADIS", "ABP_CERTIFICATIONAGENCY", "ABP_COUMARAVA",
        "ABP_COUMARAVATYP", "ABP_COUPLINGCOMPONENT", "ABP_COUPLINGSIZE", "ABP_COUPLINGSTYLE",
        "ABP_COUPLINGTYPE", "ABP_DIAMETER", "ABP_DIMENSIONDIAGRAM", "ABP_DISBETSHAEND",
        "ABP_DRIVEENDFLANGETYPE", "ABP_DSTR_FREQUENCY", "ABP_DSTR_OUTPUT", "ABP_DSTR_SSPEED",
        "ABP_DSTR_VOLTAGE", "ABP_DUTYTIME", "ABP_DYNAMICLOADCAPACITY", "ABP_EFFICIENCYLEVEL",
        "ABP_ELECTRICALDATA1", "ABP_ELECTRICALDATA2", "ABP_ELECTRICALDATA3", "ABP_ELECTRICALDATA4",
        "ABP_ELECTRICALDATA5", "ABP_ELECTRICALDATA6", "ABP_ELECTRICALDATA7", "ABP_ELECTRICALDATA8",
        "ABP_ELEMENTMATERIAL", "ABP_ENCLOSURETYPE", "ABP_ETIM7", "ABP_EXPANSIONCAPABILITY",
        "ABP_EXPLOSIONPROTECTION", "ABP_EXPPROGROCLA", "ABP_FITCLASS", "ABP_FLANGEMATERIAL",
        "ABP_FRAMEMATERIAL", "ABP_FRAMESIZE", "ABP_FREQUENCY", "ABP_GEARBOXCOMPONENTTYPE",
        "ABP_GREASENAME", "ABP_HIGHTEMPERATUREFLAG", "ABP_HORPER100RAT", "ABP_HORSEPOWERRATING",
        "ABP_HOUDIMSTA", "ABP_HOUSINGCONSTRUCTION", "ABP_HOUSINGMATERIAL", "ABP_HOUSINGTYPE",
        "ABP_HUBMATERIAL", "ABP_IDEGRACATCOD", "ABP_INPUTPOWEROPTION", "ABP_INPUTSTYLE",
        "ABP_INSERTMATERIAL", "ABP_INSERTOUTERDIAMETER", "ABP_INSTALLATIONTORQUE", "ABP_INSULATIONCLASS",
        "ABP_INTEGRALKEY", "ABP_IPCLASS", "ABP_KEYWAYSIZE", "ABP_LUBRICATION",
        "ABP_MAXIMUMSPEED", "ABP_MILLMOTORSIZE", "ABP_MOTORBASETYPE", "ABP_MOTORFRAMEPREFIX",
        "ABP_MOTORFRAMESIZE", "ABP_MOTORFRAMESUFFIX", "ABP_MOUNTINGBOLTPATTERN", "ABP_MOUNTINGORIENTATION",
        "ABP_MOUNTINGPOSITION", "ABP_MOUNTINGTYPE", "ABP_NEMADESIGNCODE", "ABP_NOMINALRATIO",
        "ABP_NUMBEROFBANDS", "ABP_NUMBEROFBOLTS", "ABP_NUMBEROFPHASES", "ABP_NUMBEROFPOLES",
        "ABP_NUMBEROFSPEEDS", "ABP_OFFERING_TREE_LEAF_NODES", "ABP_OILRESISTANCE", "ABP_OMS_PUBLISHER",
        "ABP_OPPDRIENDFLATYP", "ABP_OUSHCETOINMOMOFADI", "ABP_OUTPUT", "ABP_OUTSIDEDIAMETER",
        "ABP_OUTPUTPOWER", "ABP_OVERHUNGLOAD", "ABP_PAINTTYPE", "ABP_PARALLELMISALIGNMENT",
        "ABP_PILOTDEPTH", "ABP_PILOTDIAMETER", "ABP_POLESHIGH", "ABP_POWERSUPPLYINCLUDED",
        "ABP_PRODUCTCOMPATIBILITY", "ABP_PRODUCTIMAGE", "ABP_PRODUCTLINE", "ABP_PRODUCTMATRIX1",
        "ABP_PRODUCTMATRIX2", "ABP_PRODUCTMATRIX3", "ABP_PRODUCTMATRIX4", "ABP_PRODUCTNAME",
        "ABP_PRODUCTNETDEPTH", "ABP_PRODUCTNETHEIGHT", "ABP_PRODUCTNETWIDTH", "ABP_PRODUCTTYPE",
        "ABP_RATEDTORQUESTR", "ABP_REDUCTIONTYPE", "ABP_RELUBRICATABLE", "ABP_SCRCONCOM",
        "ABP_SEALINGTYPE", "ABP_SENSORREADY", "ABP_SERVICEFACTOR", "ABP_SETUPREQUIREMENTS",
        "ABP_SHAFTATTACHMENT", "ABP_SHAFTDIAMETER", "ABP_SHAFTLENGTH", "ABP_SHAFTSPACING",
        "ABP_SHEAVEDIAMETER", "ABP_STANDARDS", "ABP_STANDOFFINCLUDED", "ABP_STARTINGOFMOTOR",
        "ABP_STATICCONDUCTIVE", "ABP_STATICLOADCAPACITY", "ABP_SUIFORHIGTEMAPP", "ABP_SUIFORWASENV",
        "ABP_SYNBELTOOPRO", "ABP_SYNCHRONOUSBELTPITCH", "ABP_SYNCHRONOUSSPEED", "ABP_TAKEUPFRAMESIZE",
        "ABP_TEMPERATURECLASS", "ABP_TEMPERATURERANGE", "ABP_TEMPERATURERATING", "ABP_THEHORRAT",
        "ABP_THREADSIZE", "ABP_THREADSTANDARD", "ABP_TIGHTENINGTORQUE", "ABP_TORSIONALSTIFFNESS",
        "ABP_TYPEOFDUTY", "ABP_UNSPSC", "ABP_VBELTCONSTRUCTION", "ABP_VBELTDESIGN",
        "ABP_VBELTPROFILES", "ABP_VBELTSIDES", "ABP_VOLTAGERATING", "ABP_WRENCHSIZE"
    ]

    return {
        "columns": columns,
        "column_types": {},  # No type information in fallback mode
        "distinct_values": {}  # No distinct values in fallback mode
    }


def get_cached_column_metadata(force_refresh=False):
    """
    Get column metadata with caching and fallback.

    The cache does NOT auto-expire. It persists until manually refreshed.
    This avoids slow page loads when the cache expires.

    Args:
        force_refresh: If True, bypass cache and fetch fresh data from Snowflake

    Returns:
        dict with:
            - columns: list of column names
            - column_types: dict mapping column name to data type
            - distinct_values: dict mapping column name to list of distinct values
    """
    print(f"🔍 Cache file path: {CACHE_FILE}")
    print(f"📁 Cache file exists: {CACHE_FILE.exists()}")
    print(f"🔄 Force refresh requested: {force_refresh}")

    # Use cached data if available (no expiration check)
    if not force_refresh and CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r') as f:
                cache_data = json.load(f)

            cache_timestamp = cache_data.get("timestamp", "Unknown")
            print(f"✅ Using cached column metadata (last updated: {cache_timestamp})")
            return cache_data["metadata"]
        except (json.JSONDecodeError, KeyError) as e:
            print(f"⚠️ Cache file corrupted, will refresh: {e}")

    # No cache or force refresh - try to fetch fresh data
    try:
        print("▶ Fetching fresh column metadata from Snowflake (this may take a few minutes)...")
        metadata = get_column_metadata()

        print(f"📊 Metadata fetched: {len(metadata.get('columns', []))} columns, {len(metadata.get('distinct_values', {}))} with distinct values")

        # Save to cache
        cache_data = {
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata
        }

        # Ensure parent directory exists
        print(f"📂 Ensuring parent directory exists: {CACHE_FILE.parent}")
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        print(f"✅ Parent directory exists: {CACHE_FILE.parent.exists()}")

        print(f"💾 Writing cache to: {CACHE_FILE}")
        try:
            with open(CACHE_FILE, 'w') as f:
                json.dump(cache_data, f, indent=2)
                f.flush()  # Ensure data is written

            # Verify the file was written
            if CACHE_FILE.exists():
                file_size = CACHE_FILE.stat().st_size
                print(f"✅ Column metadata cached to {CACHE_FILE} ({file_size} bytes)")
            else:
                print(f"❌ ERROR: Cache file does not exist after write!")
        except Exception as write_error:
            print(f"❌ ERROR writing cache file: {write_error}")
            import traceback
            traceback.print_exc()

        return metadata

    except Exception as e:
        print(f"⚠️ Could not connect to Snowflake or fetch metadata: {e}")
        import traceback
        traceback.print_exc()
        print("📋 Using fallback column metadata (hardcoded list)")

        # If we're using fallback, still save it to cache to avoid repeated Snowflake attempts
        fallback = get_fallback_column_metadata()

        try:
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "metadata": fallback,
                "is_fallback": True
            }
            print(f"💾 Writing fallback cache to: {CACHE_FILE}")
            CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(CACHE_FILE, 'w') as f:
                json.dump(cache_data, f, indent=2)
                f.flush()

            if CACHE_FILE.exists():
                file_size = CACHE_FILE.stat().st_size
                print(f"💾 Saved fallback metadata to cache at {CACHE_FILE} ({file_size} bytes)")
            else:
                print(f"❌ ERROR: Fallback cache file does not exist after write!")
        except Exception as cache_error:
            print(f"⚠️ Could not save fallback to cache: {cache_error}")
            import traceback
            traceback.print_exc()

        return fallback


def invalidate_cache():
    """Delete the cache file to force a refresh on next request."""
    if CACHE_FILE.exists():
        os.remove(CACHE_FILE)
        print("✅ Column metadata cache invalidated")


def get_cache_info() -> dict:
    """
    Get information about the current cache status.

    Returns:
        dict with:
            - exists: bool - whether cache file exists
            - timestamp: str - when cache was last updated (ISO format)
            - timestamp_display: str - human-readable timestamp
            - column_count: int - number of columns in cache
    """
    if not CACHE_FILE.exists():
        return {
            "exists": False,
            "timestamp": None,
            "timestamp_display": "Never",
            "column_count": 0
        }

    try:
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)

        timestamp = cache_data.get("timestamp", "Unknown")
        try:
            dt = datetime.fromisoformat(timestamp)
            timestamp_display = dt.strftime("%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            timestamp_display = timestamp

        metadata = cache_data.get("metadata", {})
        column_count = len(metadata.get("columns", []))

        return {
            "exists": True,
            "timestamp": timestamp,
            "timestamp_display": timestamp_display,
            "column_count": column_count
        }
    except (json.JSONDecodeError, KeyError):
        return {
            "exists": False,
            "timestamp": None,
            "timestamp_display": "Corrupted",
            "column_count": 0
        }
