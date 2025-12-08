import json
import os
from datetime import date
from typing import Any, Dict, List

RULEBOOK_PATH = os.path.join(os.path.dirname(__file__), "..", "rulebook_registry.json")


def load_rulebook() -> Dict[str, Any]:
    if not os.path.exists(RULEBOOK_PATH):
        return {}
    with open(RULEBOOK_PATH, "r", encoding="utf-8") as f:
        content = f.read().strip()
        return json.loads(content) if content else {}


def save_rulebook(data: Dict[str, Any]) -> None:
    with open(RULEBOOK_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def _get_kwargs(exp: Any) -> Dict[str, Any]:
    """Safely extract expectation kwargs from GX object (handles several versions)."""
    for attr_path in ["kwargs", "configuration.kwargs", "_configuration.kwargs"]:
        try:
            target = exp
            for part in attr_path.split("."):
                target = getattr(target, part)
            if isinstance(target, dict) and target:
                return target
        except AttributeError:
            continue
    return {}


def _friendly_entry(exp: Any) -> Dict[str, Any]:
    """Simplify an expectation into a business-friendly dictionary."""
    kwargs = _get_kwargs(exp)
    entry: Dict[str, Any] = {"added_on": str(date.today())}

    # --- Identify column(s) ---
    if "column" in kwargs:
        entry["column"] = kwargs["column"]
    elif "column_A" in kwargs and "column_B" in kwargs:
        entry["columns"] = [kwargs["column_A"], kwargs["column_B"]]
    elif "column_list" in kwargs:
        entry["columns"] = kwargs["column_list"]

    # --- Friendly extras ---
    if "value_set" in kwargs:
        entry["allowed_values"] = kwargs["value_set"]
    if "regex" in kwargs:
        entry["pattern"] = kwargs["regex"]
    if "or_equal" in kwargs:
        entry["or_equal"] = kwargs["or_equal"]

    return entry


def register_suite_rules(suite_name, expectations):
    """Update the rulebook JSON with new expectations for a suite (deduplicating identical ones)."""
    print(f"📘 register_suite_rules called with {len(expectations) if expectations else 0} expectations", flush=True)

    if not expectations:
        print("📘 No expectations provided, returning early", flush=True)
        return

    today = str(date.today())
    print(f"📘 Processing {len(expectations)} expectations for suite '{suite_name}'", flush=True)

    # --- Load existing rulebook safely ---
    if os.path.exists(RULEBOOK_PATH):
        with open(RULEBOOK_PATH, "r", encoding="utf-8") as f:
            try:
                rulebook = json.load(f)
                if isinstance(rulebook, list):  # legacy repair
                    flat = {}
                    for item in rulebook:
                        if isinstance(item, dict):
                            flat.update(item)
                    rulebook = flat
            except json.JSONDecodeError:
                print("⚠️ Corrupted rulebook detected — resetting file.")
                rulebook = {}
    else:
        rulebook = {}

    suite_rules = rulebook.setdefault(suite_name, {})

    for exp in expectations:
        exp_type = getattr(exp, "expectation_type", None)
        if not exp_type:
            continue

        # --- Extract kwargs safely across all GX versions ---
        kwargs = {}
        if hasattr(exp, "configuration") and getattr(exp.configuration, "kwargs", None):
            kwargs = exp.configuration.kwargs
        elif hasattr(exp, "kwargs"):
            kwargs = exp.kwargs or {}

        entry = {"added_on": today}

        # --- Try all possible places for column info ---
        column = (
            kwargs.get("column")
            or getattr(exp, "column", None)
        )
        column_A = (
            kwargs.get("column_A")
            or getattr(exp, "column_A", None)
        )
        column_B = (
            kwargs.get("column_B")
            or getattr(exp, "column_B", None)
        )

        if column:
            entry["column"] = column
        elif column_A and column_B:
            entry["columns"] = [column_A, column_B]

        # --- Capture optional flags ---
        for key in ["or_equal", "value_set", "regex"]:
            val = kwargs.get(key) or getattr(exp, key, None)
            if val is not None:
                entry[key] = val

        # --- Deduplicate identical entries ---
        existing = suite_rules.setdefault(exp_type, [])

        def is_same_rule(a, b):
            """Return True if two rules match (ignoring added_on)."""
            keys_to_compare = set(a.keys()).union(b.keys()) - {"added_on"}
            return all(a.get(k) == b.get(k) for k in keys_to_compare)

        if not any(is_same_rule(entry, e) for e in existing):
            existing.append(entry)

    # --- Write updated rulebook back to file ---
    print(f"📘 Writing rulebook to {RULEBOOK_PATH}", flush=True)
    print(f"📘 Suite '{suite_name}' has {len(suite_rules)} expectation types", flush=True)
    for exp_type, rules in suite_rules.items():
        print(f"   - {exp_type}: {len(rules)} rule(s)", flush=True)

    with open(RULEBOOK_PATH, "w", encoding="utf-8") as f:
        json.dump(rulebook, f, indent=4)

    print(f"📘 Rulebook updated for suite '{suite_name}' ({len(expectations)} expectations).", flush=True)
