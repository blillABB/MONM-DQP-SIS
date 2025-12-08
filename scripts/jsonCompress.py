import os
import re
import gzip
import json
from datetime import datetime, timedelta

def extract_date_from_filename(filename: str):
    """
    Find a date in the filename in the form YYYY-MM-DD.
    Returns a datetime.date or None if no date is found.
    """
    match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
    if not match:
        return None

    date_str = match.group(0)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None


def is_previous_month(file_date):
    """
    Check if file_date (a date) is in the previous calendar month.
    """
    today = datetime.today().date()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)

    return (
        file_date.year == last_day_prev_month.year and
        file_date.month == last_day_prev_month.month
    )


def compress_json_files_from_previous_month(
    input_dir: str = ".",
    output_dir: str = r"Z:\Compressed Aurora JSON"
):
    os.makedirs(output_dir, exist_ok=True)
    compressed_count = 0

    for entry in os.listdir(input_dir):
        if not entry.lower().endswith(".json"):
            continue

        full_path = os.path.join(input_dir, entry)
        if not os.path.isfile(full_path):
            continue

        file_date = extract_date_from_filename(entry)
        if not file_date or not is_previous_month(file_date):
            continue

        try:
            with open(full_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            compressed_filename = os.path.join(output_dir, entry + ".gz")

            with gzip.open(compressed_filename, "wb") as gz:
                gz.write(json.dumps(data, separators=(",", ":")).encode("utf-8"))

            print(f"✅ Successfully compressed: {entry} -> {compressed_filename}")
            compressed_count += 1

        except Exception as e:
            print(f"❌ Error processing {entry}: {e}")

    # Summary message
    if compressed_count > 0:
        print(f"\n🎉 Completed! {compressed_count} file(s) successfully compressed to '{output_dir}'.")
    else:
        print("\n⚠️ No JSON files from the previous month were found or compressed.")


if __name__ == "__main__":
    compress_json_files_from_previous_month()
