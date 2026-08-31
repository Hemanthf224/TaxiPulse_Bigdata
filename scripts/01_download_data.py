#!/usr/bin/env python3
"""
scripts/01_download_data.py
Download official NYC TLC Trip Record datasets (Yellow, Green, FHVHV)
and Taxi Zone Lookup CSV from NYC TLC CloudFront.
"""

import os
import sys
import argparse
import requests
import pandas as pd

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download_file(url, target_path):
    """Download file with stream and resume support."""
    if os.path.exists(target_path) and os.path.getsize(target_path) > 1000:
        print(f"[SKIP] File already exists: {target_path} ({os.path.getsize(target_path)} bytes)")
        return True

    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    print(f"[DOWNLOADING] {url} -> {target_path}")
    
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with open(target_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        print(f"[SUCCESS] Saved: {target_path} ({os.path.getsize(target_path)} bytes)")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        if os.path.exists(target_path):
            os.remove(target_path)
        return False

def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC Taxi Datasets")
    parser.add_argument("--year", type=int, default=2024, help="Year to download (e.g., 2024)")
    parser.add_argument("--months", type=str, default="01", help="Comma-separated months (e.g., 01 or 01,02)")
    parser.add_argument("--types", type=str, default="yellow,green,fhvhv", help="Comma-separated trip types (yellow,green,fhvhv)")
    parser.add_argument("--output-dir", type=str, default="data/raw", help="Directory for raw trip files")
    parser.add_argument("--ref-dir", type=str, default="data/reference", help="Directory for reference files")
    args = parser.parse_args()

    months = [m.strip().zfill(2) for m in args.months.split(",")]
    trip_types = [t.strip().lower() for t in args.types.split(",")]

    print("=" * 60)
    print("TaxiPulse — Data Ingestion & Download Utility")
    print(f"Year: {args.year} | Months: {months} | Types: {trip_types}")
    print("=" * 60)

    # 1. Download Taxi Zone Lookup
    os.makedirs(args.ref_dir, exist_ok=True)
    zone_path = os.path.join(args.ref_dir, "taxi_zone_lookup.csv")
    download_file(ZONE_LOOKUP_URL, zone_path)

    # 2. Download Trip Data Files
    manifest = []
    for t in trip_types:
        for m in months:
            filename = f"{t}_tripdata_{args.year}-{m}.parquet"
            url = f"{BASE_URL}/{filename}"
            type_dir = os.path.join(args.output_dir, t, f"year={args.year}", f"month={m}")
            target_path = os.path.join(type_dir, filename)
            
            success = download_file(url, target_path)
            if success:
                size_mb = os.path.getsize(target_path) / (1024 * 1024)
                manifest.append({"type": t, "year": args.year, "month": m, "file": filename, "size_mb": round(size_mb, 2)})

    manifest_df = pd.DataFrame(manifest)
    manifest_path = os.path.join(args.ref_dir, "download_manifest.csv")
    manifest_df.to_csv(manifest_path, index=False)
    print("\nDownload Manifest Summary:")
    print(manifest_df)

if __name__ == "__main__":
    main()
