#!/usr/bin/env python3
"""
Archive Unified Logs and Validation Results for a completed month.

This script:
1. Archives Unified Logs:
   - Calculates summary metrics → Logs/summaries/YYYY-MM.json (tracked in git)
   - Compresses raw files → Logs/archives/YYYY-MM.tar.gz (gitignored)
   - Removes original CSV files

2. Archives Validation Results:
   - Calculates summary metrics → validation_results/summaries/YYYY-MM.json (tracked)
   - Compresses raw files → validation_results/archives/YYYY-MM.tar.gz (gitignored)
   - Removes original JSON files

Summaries are small and tracked in git for historical reporting.
Archives are compressed and stay local (gitignored) for reference if needed.

Usage:
    python scripts/archive_month.py                    # Archive previous month
    python scripts/archive_month.py --month 2025-10   # Archive specific month
    python scripts/archive_month.py --dry-run         # Preview without changes
    python scripts/archive_month.py --logs-only       # Only archive logs
    python scripts/archive_month.py --results-only    # Only archive validation results
"""

import argparse
import json
import os
import tarfile
from datetime import datetime
from pathlib import Path

import pandas as pd


def get_project_root():
    """Get project root directory."""
    return Path(__file__).resolve().parents[1]


def get_previous_month():
    """Get YYYY-MM string for the previous month."""
    today = datetime.today()
    first_of_month = today.replace(day=1)
    last_month = first_of_month - pd.DateOffset(months=1)
    return last_month.strftime("%Y-%m")


# =========================================================
# Unified Logs Archival
# =========================================================

def load_logs_for_month(logs_dir: Path, month: str) -> pd.DataFrame:
    """Load all Unified Log CSVs and filter to specified month."""
    if not logs_dir.exists():
        return pd.DataFrame()

    files = list(logs_dir.glob("Unified_Logs_*.csv"))
    if not files:
        return pd.DataFrame()

    combined = []
    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            df["_source_file"] = f.name
            combined.append(df)
        except Exception as e:
            print(f"  Warning: Could not read {f.name}: {e}")

    if not combined:
        return pd.DataFrame()

    df_all = pd.concat(combined, ignore_index=True)
    df_all["Timestamp"] = pd.to_datetime(df_all["Timestamp"], errors="coerce")
    df_all = df_all.dropna(subset=["Timestamp"])

    # Filter to target month
    df_all["_month"] = df_all["Timestamp"].dt.strftime("%Y-%m")
    month_df = df_all[df_all["_month"] == month].copy()

    return month_df


def calculate_logs_summary(df: pd.DataFrame, month: str) -> dict:
    """Calculate summary metrics for Unified Logs."""
    if df.empty:
        return {
            "month": month,
            "total_updates": 0,
            "success_count": 0,
            "failed_count": 0,
            "unique_materials": 0,
            "field_breakdown": {},
            "archived_at": datetime.now().isoformat(),
        }

    total_updates = len(df)
    success_count = int((df["Status"].str.upper() == "SUCCESS").sum())
    failed_count = total_updates - success_count
    unique_materials = df["Material Number"].nunique()

    field_breakdown = (
        df.groupby("Field")["Material Number"]
        .nunique()
        .to_dict()
    )

    return {
        "month": month,
        "total_updates": total_updates,
        "success_count": success_count,
        "failed_count": failed_count,
        "unique_materials": unique_materials,
        "field_breakdown": field_breakdown,
        "archived_at": datetime.now().isoformat(),
    }


def get_log_files_for_month(logs_dir: Path, month: str) -> list:
    """Get list of log files that contain data for the target month."""
    files = list(logs_dir.glob("Unified_Logs_*.csv"))
    matching_files = []

    for f in files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
            df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
            df["_month"] = df["Timestamp"].dt.strftime("%Y-%m")
            if month in df["_month"].values:
                matching_files.append(f)
        except Exception:
            continue

    return matching_files


def archive_unified_logs(month: str, dry_run: bool = False) -> bool:
    """Archive Unified Logs for the specified month."""
    project_root = get_project_root()
    logs_dir = project_root / "Logs" / "Unified_Logs"
    summaries_dir = project_root / "Logs" / "summaries"
    archives_dir = project_root / "Logs" / "archives"

    print("\n--- Unified Logs ---")

    # Load and calculate summary
    df = load_logs_for_month(logs_dir, month)

    if df.empty:
        print(f"  No logs found for {month}. Skipping.")
        return False

    summary = calculate_logs_summary(df, month)
    print(f"  Total updates: {summary['total_updates']}")
    print(f"  Success: {summary['success_count']}, Failed: {summary['failed_count']}")
    print(f"  Unique materials: {summary['unique_materials']}")

    # Save summary
    summary_path = summaries_dir / f"{month}.json"
    if not dry_run:
        summaries_dir.mkdir(parents=True, exist_ok=True)
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"  Summary saved to: {summary_path}")
    else:
        print(f"  Would save summary to: {summary_path}")

    # Compress source files
    source_files = get_log_files_for_month(logs_dir, month)
    archive_path = archives_dir / f"{month}.tar.gz"

    if not dry_run:
        archives_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path, "w:gz") as tar:
            for f in source_files:
                tar.add(f, arcname=f.name)
        print(f"  Compressed {len(source_files)} files to: {archive_path}")
    else:
        print(f"  Would compress {len(source_files)} files to: {archive_path}")

    # Remove original files
    if not dry_run:
        for f in source_files:
            f.unlink()
        print(f"  Removed {len(source_files)} original files")
    else:
        print(f"  Would remove {len(source_files)} files")

    return True


# =========================================================
# Validation Results Archival
# =========================================================

def load_validation_results_for_month(results_dir: Path, month: str) -> list:
    """Load all validation result JSONs for the specified month."""
    if not results_dir.exists():
        return []

    results = []
    for suite_dir in results_dir.glob("*"):
        if not suite_dir.is_dir() or suite_dir.name in ("summaries", "archives"):
            continue

        for json_file in suite_dir.glob("*.json"):
            try:
                # Extract timestamp from filename
                ts_str = json_file.stem.split("_")[-2] + "_" + json_file.stem.split("_")[-1]
                ts = datetime.strptime(ts_str, "%Y-%m-%d_%H-%M-%S")
                file_month = ts.strftime("%Y-%m")

                if file_month == month:
                    with open(json_file, "r", encoding="utf-8") as f:
                        data = json.load(f)

                    results.append({
                        "suite": suite_dir.name,
                        "timestamp": ts,
                        "file_path": json_file,
                        "data": data,
                    })
            except Exception as e:
                print(f"  Warning: Could not read {json_file}: {e}")
                continue

    return results


def calculate_validation_summary(results: list, month: str) -> dict:
    """Calculate summary metrics for validation results."""
    if not results:
        return {
            "month": month,
            "total_runs": 0,
            "suites": {},
            "archived_at": datetime.now().isoformat(),
        }

    suites = {}
    all_materials = set()

    for r in results:
        suite_name = r["suite"]
        data = r["data"]

        if suite_name not in suites:
            suites[suite_name] = {
                "runs": 0,
                "total_expectations": 0,
                "total_failures": 0,
                "materials_validated": set(),
            }

        suites[suite_name]["runs"] += 1

        # Count expectations and failures
        results_list = data.get("results", [])
        if isinstance(results_list, list):
            suites[suite_name]["total_expectations"] += len(results_list)
            for exp in results_list:
                if isinstance(exp, dict) and not exp.get("success", True):
                    suites[suite_name]["total_failures"] += 1

        # Track validated materials
        validated = data.get("validated_materials", [])
        if isinstance(validated, list):
            suites[suite_name]["materials_validated"].update(validated)
            all_materials.update(validated)

    # Convert sets to counts for JSON serialization
    for suite_name in suites:
        suites[suite_name]["unique_materials"] = len(suites[suite_name]["materials_validated"])
        del suites[suite_name]["materials_validated"]

    return {
        "month": month,
        "total_runs": len(results),
        "total_unique_materials": len(all_materials),
        "suites": suites,
        "archived_at": datetime.now().isoformat(),
    }


def archive_validation_results(month: str, dry_run: bool = False) -> bool:
    """Archive validation results for the specified month."""
    project_root = get_project_root()
    results_dir = project_root / "validation_results"
    summaries_dir = results_dir / "summaries"
    archives_dir = results_dir / "archives"

    print("\n--- Validation Results ---")

    # Load results for month
    results = load_validation_results_for_month(results_dir, month)

    if not results:
        print(f"  No validation results found for {month}. Skipping.")
        return False

    summary = calculate_validation_summary(results, month)
    print(f"  Total runs: {summary['total_runs']}")
    print(f"  Unique materials validated: {summary['total_unique_materials']}")
    for suite_name, stats in summary["suites"].items():
        print(f"    {suite_name}: {stats['runs']} runs, {stats['unique_materials']} materials")

    # Save summary
    summary_path = summaries_dir / f"{month}.json"
    if not dry_run:
        summaries_dir.mkdir(parents=True, exist_ok=True)
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"  Summary saved to: {summary_path}")
    else:
        print(f"  Would save summary to: {summary_path}")

    # Compress result files
    source_files = [r["file_path"] for r in results]
    archive_path = archives_dir / f"{month}.tar.gz"

    if not dry_run:
        archives_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path, "w:gz") as tar:
            for f in source_files:
                # Preserve suite directory structure in archive
                arcname = f"{f.parent.name}/{f.name}"
                tar.add(f, arcname=arcname)
        print(f"  Compressed {len(source_files)} files to: {archive_path}")
    else:
        print(f"  Would compress {len(source_files)} files to: {archive_path}")

    # Remove original files
    if not dry_run:
        for f in source_files:
            f.unlink()
        print(f"  Removed {len(source_files)} original files")
    else:
        print(f"  Would remove {len(source_files)} files")

    return True


# =========================================================
# Main
# =========================================================

def archive_month(month: str, dry_run: bool = False, logs_only: bool = False, results_only: bool = False):
    """Archive both Unified Logs and Validation Results for the specified month."""
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Archiving data for {month}")
    print("=" * 50)

    logs_archived = False
    results_archived = False

    if not results_only:
        logs_archived = archive_unified_logs(month, dry_run)

    if not logs_only:
        results_archived = archive_validation_results(month, dry_run)

    print("\n" + "=" * 50)
    print(f"{'[DRY RUN] ' if dry_run else ''}Archive complete for {month}")
    print("Archives stored locally (gitignored). Summaries tracked in repo.")

    return logs_archived or results_archived


def main():
    parser = argparse.ArgumentParser(
        description="Archive Unified Logs and Validation Results for a completed month"
    )
    parser.add_argument(
        "--month",
        type=str,
        default=None,
        help="Month to archive (YYYY-MM format). Defaults to previous month.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview actions without making changes",
    )
    parser.add_argument(
        "--logs-only",
        action="store_true",
        help="Only archive Unified Logs",
    )
    parser.add_argument(
        "--results-only",
        action="store_true",
        help="Only archive Validation Results",
    )

    args = parser.parse_args()

    month = args.month or get_previous_month()

    # Validate month format
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        print(f"Error: Invalid month format '{month}'. Use YYYY-MM.")
        return 1

    # Don't archive current month
    current_month = datetime.today().strftime("%Y-%m")
    if month == current_month:
        print(f"Error: Cannot archive current month ({month}).")
        print("Wait until the month is complete or specify a previous month.")
        return 1

    success = archive_month(
        month,
        dry_run=args.dry_run,
        logs_only=args.logs_only,
        results_only=args.results_only
    )
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
