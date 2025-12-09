import os
import json
from datetime import datetime
import pandas as pd
import streamlit as st
from core.queries import get_connection
from core.cache_manager import (
    get_cached_monthly_overview,
    save_cached_monthly_overview,
    clear_monthly_overview_cache
)
from pathlib import Path

# =========================================================
# Page setup
# =========================================================
st.set_page_config(page_title="Monthly Overview", layout="wide")
st.title("Material Master Monitoring & Quality Updates")

today = datetime.today()
current_year = today.year
current_month = today.month

# Clear cache button in sidebar
with st.sidebar:
    if st.button("🔄 Refresh Home Page Data", key="clear_monthly_cache"):
        clear_monthly_overview_cache()
        st.rerun()

st.caption(today.strftime("%B %d, %Y"))

# =========================================================
# Unified Logs Loader
# =========================================================
def load_unified_logs(base_dir="Logs/Unified_Logs"):
    """Read and combine all Unified Log CSV files."""
    import os
    import pandas as pd
    from datetime import datetime

    logs_path = Path(base_dir)
    if not logs_path.exists():
        return pd.DataFrame()

    files = sorted(logs_path.glob("Unified_Logs_*.csv"), key=os.path.getmtime, reverse=True)
    if not files:
        return pd.DataFrame()

    combined = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            df["Source File"] = f.name
            combined.append(df)
        except Exception as e:
            st.write(f"Could not read {f.name}: {e}")

    if not combined:
        return pd.DataFrame()

    df_all = pd.concat(combined, ignore_index=True)
    df_all["Timestamp"] = pd.to_datetime(df_all["Timestamp"], errors="coerce")
    df_all = df_all.dropna(subset=["Timestamp"])
    return df_all


def load_archived_summaries(summaries_dir="Logs/summaries"):
    """Load all archived monthly summaries for Unified Logs."""
    summaries_path = Path(summaries_dir)
    if not summaries_path.exists():
        return {}

    summaries = {}
    for f in sorted(summaries_path.glob("*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                month = data.get("month", f.stem)
                summaries[month] = data
        except Exception:
            continue

    return summaries


def load_archived_validation_summaries(summaries_dir="validation_results/summaries"):
    """Load all archived monthly summaries for Validation Results."""
    summaries_path = Path(summaries_dir)
    if not summaries_path.exists():
        return {}

    summaries = {}
    for f in sorted(summaries_path.glob("*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                month = data.get("month", f.stem)
                summaries[month] = data
        except Exception:
            continue

    return summaries


def compute_monthly_overview_data():
    """
    Compute all Monthly Overview data from scratch.
    Returns a dictionary with all computed metrics.
    """
    # Load validation history
    history_df = load_validation_history()
    if history_df.empty:
        return None

    # Aggregate current vs. previous month totals
    today_dt = datetime.today()
    current_month_str = today_dt.strftime("%Y-%m")
    previous_month_str = (today_dt.replace(day=1) - pd.DateOffset(months=1)).strftime("%Y-%m")

    history_df["month"] = history_df["timestamp"].dt.strftime("%Y-%m")

    current_records = history_df[history_df["month"] == current_month_str]
    previous_records = history_df[history_df["month"] == previous_month_str]

    def distinct_materials(records):
        return set(m for row in records["validated_materials"] for m in row)

    current_materials = distinct_materials(current_records)
    previous_materials = distinct_materials(previous_records)

    current_total = len(current_materials)
    previous_total = len(previous_materials)
    delta = current_total - previous_total

    # Get product hierarchy breakdown from Snowflake
    product_hierarchy_breakdown = []
    if current_materials:
        conn = None
        try:
            conn = get_connection()
            query = f"""
                SELECT DISTINCT "Material Number", "Product Hierarchy"
                FROM DEV_MO_MONM.REPORTING.MD_MAT_ALL
                WHERE "Material Number" IN ({','.join(f"'{m}'" for m in current_materials)})
            """
            df_pg = pd.read_sql(query, conn)
            breakdown = (
                df_pg.groupby("Product Hierarchy")["Material Number"]
                .nunique()
                .reset_index()
            )
            breakdown.columns = ["Product Hierarchy", "Count"]
            breakdown = breakdown.sort_values("Count", ascending=False)
            product_hierarchy_breakdown = breakdown.to_dict("records")
        except Exception as e:
            print(f"⚠️ Error fetching product hierarchy: {e}")
        finally:
            if conn:
                conn.close()

    # Load unified logs stats
    logs_df = load_unified_logs()
    logs_stats = {
        "total_updates": 0,
        "success_count": 0,
        "failed_count": 0,
        "field_breakdown": [],
        "level_1_stats": {"total": 0, "success": 0, "failed": 0},
        "level_2_stats": {"total": 0, "success": 0, "failed": 0},
        "level_3_stats": {"total": 0, "success": 0, "failed": 0},
    }

    if not logs_df.empty:
        # Filter to current month
        month_mask = logs_df["Timestamp"].dt.to_period("M") == pd.Timestamp.now().to_period("M")
        current_logs = logs_df.loc[month_mask].copy()

        total_updates = len(current_logs)
        success_count = int((current_logs["Status"].str.upper() == "SUCCESS").sum())
        failed_count = total_updates - success_count

        field_breakdown = (
            current_logs.groupby("Field")["Material Number"]
            .nunique()
            .reset_index()
            .rename(columns={"Material Number": "Count"})
            .sort_values("Count", ascending=False)
        )

        # Build field-to-level mapping
        field_to_level = build_field_to_level_mapping(rulebooks)

        # Map each log entry to a level
        current_logs["Level"] = current_logs["Field"].map(field_to_level)
        # Default to level 3 if field not in mapping
        current_logs["Level"] = current_logs["Level"].fillna(3).astype(int)

        # Compute stats by level
        level_1_logs = current_logs[current_logs["Level"] == 1]
        level_2_logs = current_logs[current_logs["Level"] == 2]
        level_3_logs = current_logs[current_logs["Level"] == 3]

        level_1_stats = {
            "total": len(level_1_logs),
            "success": int((level_1_logs["Status"].str.upper() == "SUCCESS").sum()),
            "failed": len(level_1_logs) - int((level_1_logs["Status"].str.upper() == "SUCCESS").sum()),
        }
        level_2_stats = {
            "total": len(level_2_logs),
            "success": int((level_2_logs["Status"].str.upper() == "SUCCESS").sum()),
            "failed": len(level_2_logs) - int((level_2_logs["Status"].str.upper() == "SUCCESS").sum()),
        }
        level_3_stats = {
            "total": len(level_3_logs),
            "success": int((level_3_logs["Status"].str.upper() == "SUCCESS").sum()),
            "failed": len(level_3_logs) - int((level_3_logs["Status"].str.upper() == "SUCCESS").sum()),
        }

        logs_stats = {
            "total_updates": total_updates,
            "success_count": success_count,
            "failed_count": failed_count,
            "field_breakdown": field_breakdown.to_dict("records"),
            "level_1_stats": level_1_stats,
            "level_2_stats": level_2_stats,
            "level_3_stats": level_3_stats,
        }

    return {
        "current_total": current_total,
        "previous_total": previous_total,
        "delta": delta,
        "current_materials": list(current_materials),
        "product_hierarchy_breakdown": product_hierarchy_breakdown,
        "logs_stats": logs_stats
    }


def get_monthly_overview_data():
    """
    Get Monthly Overview data, using cache if available.
    """
    # Check cache first
    cached_data = get_cached_monthly_overview()
    if cached_data:
        return cached_data

    # Compute fresh data
    data = compute_monthly_overview_data()
    if data:
        # Save to cache
        save_cached_monthly_overview(data)

    return data

# =========================================================
# Load rulebook registry
# =========================================================
base_dir = Path(os.getcwd())  # Streamlit runs from project root
rulebook_path = base_dir / "rulebook_registry.json"

if not rulebook_path.exists():
    # Fallback if run from a nested working directory
    alt_path = Path(__file__).resolve().parents[1] / "rulebook_registry.json"
    rulebook_path = alt_path if alt_path.exists() else rulebook_path

if not rulebook_path.exists():
    st.error(f"rulebook_registry.json not found at: {rulebook_path}")
    st.stop()

with open(rulebook_path, "r") as f:
    try:
        rulebooks = json.load(f)
    except json.JSONDecodeError:
        st.warning("⚠️ Rulebook registry is corrupted. Creating new empty registry...")
        rulebooks = {}
        with open(rulebook_path, "w") as fw:
            json.dump(rulebooks, fw, indent=4)

    # Handle legacy format or corrupted data (list instead of dict)
    if isinstance(rulebooks, list):
        st.warning("⚠️ Rulebook registry is in legacy list format. Converting to dict format...")
        flat = {}
        for item in rulebooks:
            if isinstance(item, dict):
                flat.update(item)
        rulebooks = flat
        # Save the corrected format
        with open(rulebook_path, "w") as fw:
            json.dump(rulebooks, fw, indent=4)
        st.success("✅ Rulebook registry converted to correct format")

    # Ensure it's a dict at this point
    if not isinstance(rulebooks, dict):
        st.error(f"❌ Rulebook registry has unexpected type: {type(rulebooks).__name__}. Expected dict.")
        rulebooks = {}

# =========================================================
# Helper functions
# =========================================================
def count_rules(rulebook_data: dict) -> int:
    """Count total number of expectations in a rulebook."""
    total = 0
    for expectation_list in rulebook_data.values():
        total += len(expectation_list)
    return total


def count_new_rules_this_month(rulebook_data: dict) -> int:
    """Count rules added since the start of the current month."""
    count = 0
    for expectation_list in rulebook_data.values():
        for rule in expectation_list:
            added_on = rule.get("added_on")
            if added_on:
                try:
                    added_date = datetime.strptime(added_on, "%Y-%m-%d")
                    if added_date.year == current_year and added_date.month == current_month:
                        count += 1
                except ValueError:
                    continue
    return count


def aggregate_other_rulebooks(exclude=("Level_1_Validation", "Level_2_Validation")):
    """Aggregate counts for all rulebooks not explicitly excluded."""
    total_rules = 0
    new_rules = 0
    for key, rb_data in rulebooks.items():
        if key not in exclude:
            total_rules += count_rules(rb_data)
            new_rules += count_new_rules_this_month(rb_data)
    return total_rules, new_rules


def build_field_to_level_mapping(rulebooks_data: dict) -> dict:
    """Build a mapping of field names to validation levels."""
    field_to_level = {}

    # Level 1 fields
    if "Level_1_Validation" in rulebooks_data:
        for expectation_list in rulebooks_data["Level_1_Validation"].values():
            for rule in expectation_list:
                if "column" in rule:
                    field_to_level[rule["column"]] = 1
                elif "columns" in rule:  # For pair expectations
                    for col in rule["columns"]:
                        field_to_level[col] = 1

    # Level 2 fields
    if "Level_2_Validation" in rulebooks_data:
        for expectation_list in rulebooks_data["Level_2_Validation"].values():
            for rule in expectation_list:
                if "column" in rule:
                    field_to_level[rule["column"]] = 2
                elif "columns" in rule:
                    for col in rule["columns"]:
                        field_to_level[col] = 2

    # Level 3 fields (all other suites)
    for suite_name, suite_data in rulebooks_data.items():
        if suite_name not in ("Level_1_Validation", "Level_2_Validation"):
            for expectation_list in suite_data.values():
                for rule in expectation_list:
                    if "column" in rule:
                        # Only assign to level 3 if not already in level 1 or 2
                        if rule["column"] not in field_to_level:
                            field_to_level[rule["column"]] = 3
                    elif "columns" in rule:
                        for col in rule["columns"]:
                            if col not in field_to_level:
                                field_to_level[col] = 3

    return field_to_level


# =========================================================
# Compute GX Rule Counts
# =========================================================
level_1_rules = count_rules(rulebooks.get("Level_1_Validation", {}))
level_2_rules = count_rules(rulebooks.get("Level_2_Validation", {}))
level_3_total, level_3_new = aggregate_other_rulebooks()

level_1_new = count_new_rules_this_month(rulebooks.get("Level_1_Validation", {}))
level_2_new = count_new_rules_this_month(rulebooks.get("Level_2_Validation", {}))

gx_counts = {
    "Level 1 - Data Completeness": {"total": level_1_rules, "new": level_1_new},
    "Level 2 - Relational Checks (Cross Column)": {"total": level_2_rules, "new": level_2_new},
    "Level 3 - Product / Application Specific": {"total": level_3_total, "new": level_3_new},
}

# =========================================================
# Load all saved validation JSONs
# =========================================================
def load_validation_history():
    """Collects all suite results and their validated materials."""
    # Go one level up: from app → project root
    project_root = Path(__file__).resolve().parents[1]
    base_dir = project_root / "validation_results"

    if not base_dir.exists():
        st.warning(f"No validation_results directory found at: {base_dir}")
        return pd.DataFrame()

    # Directories to skip (not validation suite directories)
    skip_dirs = {"cache", "summaries"}

    history = []
    for suite_dir in base_dir.glob("*"):
        if not suite_dir.is_dir():
            continue
        if suite_dir.name in skip_dirs:
            continue

        for json_file in suite_dir.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Extract timestamp from filename (safe fallback)
                ts_str = json_file.stem.split("_")[-2] + "_" + json_file.stem.split("_")[-1]
                ts = datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S")

                validated = data.get("validated_materials", [])
                history.append({
                    "suite": suite_dir.name,
                    "timestamp": ts,
                    "validated_materials": validated,
                })
            except Exception as e:
                print(f"Skipping file {json_file}: {e}")
                continue

    return pd.DataFrame(history)


# =========================================================
# Load Monthly Overview Data (with caching)
# =========================================================
overview_data = get_monthly_overview_data()
if overview_data is None:
    st.warning("No validation history found yet.")
    st.stop()

# Extract cached data
current_total = overview_data["current_total"]
previous_total = overview_data["previous_total"]
delta = overview_data["delta"]
current_materials = set(overview_data["current_materials"])
product_hierarchy_breakdown = overview_data["product_hierarchy_breakdown"]
logs_stats = overview_data["logs_stats"]

# =========================================================
# Layout
# =========================================================
col_rules, col_materials, col_quality = st.columns([1, 1.2, 1])

# ------------------------- GX RULES -------------------------
with col_rules:
    st.subheader("GX RULES")
    with st.container(border=True):
        for level, stats in gx_counts.items():
            delta_str = f"+{stats['new']}" if stats["new"] > 0 else f"{stats['new']}"
            st.metric(label=level, value=stats["total"], delta=delta_str)

# ------------------------- MATERIALS -------------------------
with col_materials:
    st.subheader("VALIDATED MATERIALS")
    with st.container(border=True):
        # --- Metric cards (Option A + B)
        c1, c2 = st.columns(2)
        c1.metric("This Month", current_total)
        c2.metric(
            "Month-over-Month Change",
            delta,
            f"{(delta / previous_total * 100):.1f}%" if previous_total else None,
        )

        # --- Product Group Breakdown (from cached data)
        if current_total == 0:
            st.info("No validated materials for this month yet.")
        elif product_hierarchy_breakdown:
            breakdown = pd.DataFrame(product_hierarchy_breakdown)
            st.subheader("Breakdown by Product Hierarchy")
            st.bar_chart(breakdown.set_index("Product Hierarchy"))
        else:
            st.warning("Product hierarchy data unavailable. Check Snowflake connection.")

# =========================================================
# Data Quality Updates Section (from cached data)
# =========================================================
total_updates = logs_stats["total_updates"]
success_count = logs_stats["success_count"]
failed_count = logs_stats["failed_count"]
field_breakdown_data = logs_stats["field_breakdown"]
# Use .get() with defaults for backward compatibility with old cache
level_1_stats = logs_stats.get("level_1_stats", {"total": 0, "success": 0, "failed": 0})
level_2_stats = logs_stats.get("level_2_stats", {"total": 0, "success": 0, "failed": 0})
level_3_stats = logs_stats.get("level_3_stats", {"total": 0, "success": 0, "failed": 0})

if total_updates > 0:
    # UI Layout
    with col_quality:
        st.subheader("DATA QUALITY UPDATES")

        # Total Updates
        with st.container(border=True):
            st.markdown("**Total Updates**")
            st.metric("Total Updates", f"{total_updates:,}", f"{success_count} Successful")
            st.metric("Failures", f"{failed_count:,}")

        # Level 1 Updates
        if level_1_stats["total"] > 0:
            with st.container(border=True):
                st.markdown("**Level 1 - Data Completeness**")
                st.metric("Total", f"{level_1_stats['total']:,}", f"{level_1_stats['success']} Successful")
                st.metric("Failures", f"{level_1_stats['failed']:,}")

        # Level 2 Updates
        if level_2_stats["total"] > 0:
            with st.container(border=True):
                st.markdown("**Level 2 - Relational Checks**")
                st.metric("Total", f"{level_2_stats['total']:,}", f"{level_2_stats['success']} Successful")
                st.metric("Failures", f"{level_2_stats['failed']:,}")

        # Level 3 Updates
        if level_3_stats["total"] > 0:
            with st.container(border=True):
                st.markdown("**Level 3 - Product / Application Specific**")
                st.metric("Total", f"{level_3_stats['total']:,}", f"{level_3_stats['success']} Successful")
                st.metric("Failures", f"{level_3_stats['failed']:,}")

        if field_breakdown_data:
            field_breakdown = pd.DataFrame(field_breakdown_data)
            st.divider()
            st.markdown("**Top Updated Fields**")
            st.bar_chart(field_breakdown.set_index("Field"))

else:
    with col_quality:
        st.subheader("DATA QUALITY UPDATES")
        st.info("No unified logs found for this month yet.")

# =========================================================
# Archived History (from summaries)
# =========================================================
archived_summaries = load_archived_summaries()

if archived_summaries:
    with col_quality:
        with st.expander(f"View Archived History ({len(archived_summaries)} months)"):
            for month in sorted(archived_summaries.keys(), reverse=True):
                summary = archived_summaries[month]
                st.markdown(f"**{month}**")
                cols = st.columns(3)
                cols[0].metric("Updates", summary.get("total_updates", 0))
                cols[1].metric("Success", summary.get("success_count", 0))
                cols[2].metric("Failed", summary.get("failed_count", 0))
                st.divider()
