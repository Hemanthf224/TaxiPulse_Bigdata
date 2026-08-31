#!/usr/bin/env python3
"""
scripts/04_stage_downloaded_data.py
Scans C:\TaxiPulse_Data\ for Yellow, Green, and FHVHV Parquet files
and organizes them into data/raw/<type>/year=YYYY/month=MM/ layout.
"""

import os
import re
import shutil

SOURCE_DIR = r"C:\TaxiPulse_Data"
TARGET_DIR = r"c:\TaxiPulse_Bigdata\data\raw"

FILENAME_REGEX = re.compile(r"^(yellow|green|fhvhv)_tripdata_(\d{4})-(\d{2})\.parquet$")

def main():
    print("=" * 60)
    print("TaxiPulse — Staging Local Downloaded Datasets")
    print(f"Source: {SOURCE_DIR}")
    print(f"Target: {TARGET_DIR}")
    print("=" * 60)

    total_files = 0
    total_size_bytes = 0

    for root, dirs, files in os.walk(SOURCE_DIR):
        for f in files:
            match = FILENAME_REGEX.match(f)
            if match:
                trip_type, year, month = match.groups()
                src_path = os.path.join(root, f)
                dest_dir = os.path.join(TARGET_DIR, trip_type, f"year={year}", f"month={month}")
                os.makedirs(dest_dir, exist_ok=True)
                dest_path = os.path.join(dest_dir, f)

                if not os.path.exists(dest_path) or os.path.getsize(dest_path) != os.path.getsize(src_path):
                    print(f"[COPYING] {f} -> {dest_path}")
                    shutil.copy2(src_path, dest_path)
                else:
                    print(f"[EXISTS] {f}")

                total_files += 1
                total_size_bytes += os.path.getsize(src_path)

    size_gb = total_size_bytes / (1024 ** 3)
    print("=" * 60)
    print(f"Staged {total_files} Parquet files totaling {size_gb:.2f} GB!")
    print("=" * 60)

if __name__ == "__main__":
    main()
