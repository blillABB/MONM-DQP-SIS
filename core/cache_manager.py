# core/cache_manager.py
"""
Cache manager for validation results.

Provides daily caching of validation results to avoid re-running validations
multiple times per day. Data is refreshed at 6 AM daily, so cache is valid
for the same calendar day.
"""

import os
import glob
import json
from datetime import datetime, date
from typing import Optional

# Cache directory
CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "validation_results",
    "cache"
)

VALIDATION_RESULTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "validation_results",
)


def _ensure_cache_dir():
    """Ensure the cache directory exists."""
    os.makedirs(CACHE_DIR, exist_ok=True)


def _safe_suite_name(suite_name: str) -> str:
    """Normalize suite name to a filesystem-safe representation."""
    return suite_name.lower().replace(" ", "_").replace("-", "_")


def _get_cache_path(suite_name: str) -> str:
    """Get the cache file path for a validation suite."""
    return os.path.join(CACHE_DIR, f"{_safe_suite_name(suite_name)}_cache.json")


def _get_suite_results_dir(suite_name: str) -> str:
    """Get or create the persistent validation_results directory for a suite."""
    suite_dir = os.path.join(VALIDATION_RESULTS_DIR, _safe_suite_name(suite_name))
    os.makedirs(suite_dir, exist_ok=True)
    return suite_dir


def _get_failures_csv_path(suite_name: str, date_str: str) -> str:
    """Get the cache file path for a suite's raw validation results for a given date."""
    return os.path.join(CACHE_DIR, f"{_safe_suite_name(suite_name)}_failures_{date_str}.csv")


def _get_today_date_str() -> str:
    """Get today's date as a string (YYYY-MM-DD)."""
    return date.today().isoformat()


def _remove_stale_failures_csv(suite_name: str, keep_date: str) -> None:
    """Remove cached raw results CSV files for a suite except the given date."""
    safe_name = _safe_suite_name(suite_name)
    pattern = os.path.join(CACHE_DIR, f"{safe_name}_failures_*.csv")
    for path in glob.glob(pattern):
        if not path.endswith(f"_{keep_date}.csv"):
            os.remove(path)


def _daily_suite_artifacts_exist(suite_name: str, date_str: str) -> bool:
    """Check if daily JSON/CSV artifacts already exist for the suite for the given date."""
    safe_name = _safe_suite_name(suite_name)
    suite_dir = _get_suite_results_dir(suite_name)
    json_pattern = os.path.join(suite_dir, f"{safe_name}_{date_str}*.json")
    csv_pattern = os.path.join(suite_dir, f"{safe_name}_{date_str}*.csv")
    return bool(glob.glob(json_pattern) or glob.glob(csv_pattern))


def get_cached_results(suite_name: str) -> Optional[dict]:
    """
    Get cached validation results if they are fresh (from today).

    Parameters
    ----------
    suite_name : str
        Name of the validation suite (e.g., "aurora", "level1")

    Returns
    -------
    dict or None
        Cached results if fresh, None otherwise.
        Structure: {"results": [...]}
    """
    cache_path = _get_cache_path(suite_name)

    if not os.path.exists(cache_path):
        print(f"📦 No cache found for {suite_name}")
        return None

    try:
        with open(cache_path, "r") as f:
            cache_data = json.load(f)

        # Check if cache is from today
        data_date = cache_data.get("data_date", "")
        today = _get_today_date_str()

        if data_date != today:
            print(f"📦 Cache for {suite_name} is stale (from {data_date}, today is {today})")
            return None

        print(f"✅ Using cached results for {suite_name} (cached at {cache_data.get('cached_at', 'unknown')})")
        return {
            "results": cache_data.get("results", []),
            "validated_materials": cache_data.get("validated_materials", [])
        }

    except (json.JSONDecodeError, KeyError) as e:
        print(f"⚠️ Error reading cache for {suite_name}: {e}")
        return None


def get_cached_failures_csv(suite_name: str) -> Optional[str]:
    """
    Get cached raw Snowflake results CSV for today, if present.

    Parameters
    ----------
    suite_name : str
        Name of the validation suite (e.g., "aurora", "level1")

    Returns
    -------
    str or None
        CSV contents of the raw results if present for today, None otherwise.
    """
    _ensure_cache_dir()
    today = _get_today_date_str()
    csv_path = _get_failures_csv_path(suite_name, today)

    if not os.path.exists(csv_path):
        return None

    with open(csv_path, "r") as f:
        return f.read()


def save_cached_results(suite_name: str, results: list, validated_materials: list = None) -> None:
    """
    Save validation results to cache.

    Parameters
    ----------
    suite_name : str
        Name of the validation suite
    results : list
        Validation results from run_validation_suite()
    validated_materials : list, optional
        List of all material numbers that were validated
    """
    _ensure_cache_dir()
    cache_path = _get_cache_path(suite_name)

    cache_data = {
        "cached_at": datetime.now().isoformat(),
        "data_date": _get_today_date_str(),
        "results": results,
        "validated_materials": validated_materials or []
    }

    with open(cache_path, "w") as f:
        json.dump(cache_data, f, indent=2, default=str)

    print(f"📦 Cached validation results for {suite_name}")


def save_cached_failures_csv(suite_name: str, df) -> None:
    """Save raw Snowflake validation results CSV to cache for today and prune stale copies."""
    _ensure_cache_dir()
    today = _get_today_date_str()
    csv_path = _get_failures_csv_path(suite_name, today)

    # Remove older CSVs for this suite so only today's remains
    _remove_stale_failures_csv(suite_name, today)

    df.to_csv(csv_path, index=False)
    print(f"📦 Cached raw results CSV for {suite_name} at {csv_path}")


def save_daily_suite_artifacts(
    suite_name: str,
    results: list,
    validated_materials: list,
    raw_results_df,
    data_date: Optional[str] = None,
) -> None:
    """Persist the day's JSON and raw CSV results to the suite's validation_results folder once per day."""

    target_date = data_date or _get_today_date_str()
    suite_dir = _get_suite_results_dir(suite_name)
    safe_name = _safe_suite_name(suite_name)

    if _daily_suite_artifacts_exist(suite_name, target_date):
        print(
            f"📦 Daily artifacts already exist for {suite_name} ({target_date}); skipping write",
            flush=True,
        )
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    base_filename = f"{safe_name}_{timestamp}"
    json_path = os.path.join(suite_dir, f"{base_filename}.json")
    csv_path = os.path.join(suite_dir, f"{base_filename}.csv")

    payload = {
        "cached_at": datetime.now().isoformat(),
        "data_date": target_date,
        "results": results or [],
        "validated_materials": validated_materials or [],
    }

    with open(json_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    if raw_results_df is None:
        print(f"⚠️ No raw results DataFrame available to persist for {suite_name}", flush=True)
        return

    try:
        raw_results_df.to_csv(csv_path, index=False)
    except Exception as e:
        print(f"⚠️ Could not write raw results CSV for {suite_name}: {e}", flush=True)
    else:
        print(
            f"📦 Persisted daily artifacts for {suite_name} to {json_path} and {csv_path}",
            flush=True,
        )


def clear_cache(suite_name: str = None) -> None:
    """
    Clear cached results.

    Parameters
    ----------
    suite_name : str, optional
        If provided, clear only this suite's cache.
        If None, clear all caches.
    """
    _ensure_cache_dir()

    if suite_name:
        cache_path = _get_cache_path(suite_name)
        if os.path.exists(cache_path):
            os.remove(cache_path)
            print(f"🗑️ Cleared cache for {suite_name}")

        safe_name = _safe_suite_name(suite_name)
        # Clear both legacy failure CSVs and the raw results CSVs used today
        patterns = [
            os.path.join(CACHE_DIR, f"{safe_name}_failures_*.csv"),
        ]
        for pattern in patterns:
            for path in glob.glob(pattern):
                os.remove(path)
                print(f"🗑️ Cleared failures CSV cache for {suite_name}")
    else:
        # Clear all cache files
        for filename in os.listdir(CACHE_DIR):
            if filename.endswith("_cache.json") or filename.endswith(".csv"):
                os.remove(os.path.join(CACHE_DIR, filename))
        print("🗑️ Cleared all validation caches")


# =========================================================
# Monthly Overview Cache Functions
# =========================================================

MONTHLY_OVERVIEW_CACHE_NAME = "monthly_overview"


def get_cached_monthly_overview() -> Optional[dict]:
    """
    Get cached Monthly Overview data if fresh (from today).

    Returns
    -------
    dict or None
        Cached data if fresh, None otherwise.
        Structure: {
            "current_total": int,
            "previous_total": int,
            "delta": int,
            "current_materials": list,
            "product_hierarchy_breakdown": list of dicts,
            "logs_stats": {
                "total_updates": int,
                "success_count": int,
                "failed_count": int,
                "field_breakdown": list of dicts
            }
        }
    """
    cache_path = _get_cache_path(MONTHLY_OVERVIEW_CACHE_NAME)

    if not os.path.exists(cache_path):
        print(f"📦 No cache found for Monthly Overview")
        return None

    try:
        with open(cache_path, "r") as f:
            cache_data = json.load(f)

        # Check if cache is from today
        data_date = cache_data.get("data_date", "")
        today = _get_today_date_str()

        if data_date != today:
            print(f"📦 Monthly Overview cache is stale (from {data_date}, today is {today})")
            return None

        print(f"✅ Using cached Monthly Overview data (cached at {cache_data.get('cached_at', 'unknown')})")
        return cache_data.get("data", {})

    except (json.JSONDecodeError, KeyError) as e:
        print(f"⚠️ Error reading Monthly Overview cache: {e}")
        return None


def save_cached_monthly_overview(data: dict) -> None:
    """
    Save Monthly Overview data to cache.

    Parameters
    ----------
    data : dict
        Monthly Overview computed data including:
        - current_total, previous_total, delta
        - current_materials
        - product_hierarchy_breakdown
        - logs_stats
    """
    _ensure_cache_dir()
    cache_path = _get_cache_path(MONTHLY_OVERVIEW_CACHE_NAME)

    cache_data = {
        "cached_at": datetime.now().isoformat(),
        "data_date": _get_today_date_str(),
        "data": data
    }

    with open(cache_path, "w") as f:
        json.dump(cache_data, f, indent=2, default=str)

    print(f"📦 Cached Monthly Overview data")


def clear_monthly_overview_cache() -> None:
    """Clear the Monthly Overview cache."""
    _ensure_cache_dir()
    cache_path = _get_cache_path(MONTHLY_OVERVIEW_CACHE_NAME)

    if os.path.exists(cache_path):
        os.remove(cache_path)
        print(f"🗑️ Cleared Monthly Overview cache")
    else:
        print(f"📦 No Monthly Overview cache to clear")
