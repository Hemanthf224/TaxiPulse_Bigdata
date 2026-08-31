#!/usr/bin/env python3
"""
scripts/02_download_weather.py
Download historical hourly NYC weather data from Open-Meteo API.
Saves raw nested JSON to data/reference/weather.json for semi-structured HDFS ingestion.
"""

import os
import sys
import argparse
import requests
import json

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

def fetch_weather(start_date, end_date, output_path):
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation,rain,snowfall,wind_speed_10m,relative_humidity_2m,weather_code",
        "timezone": "America/New_York"
    }

    print(f"[FETCHING WEATHER] Open-Meteo NYC ({start_date} to {end_date})...")
    response = requests.get(OPEN_METEO_URL, params=params, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    total_hours = len(data.get("hourly", {}).get("time", []))
    print(f"[SUCCESS] Saved weather JSON to {output_path} ({total_hours} hourly data points)")

def main():
    parser = argparse.ArgumentParser(description="Download NYC Weather Data from Open-Meteo")
    parser.add_argument("--start-date", type=str, default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2024-01-31", help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default="data/reference/weather.json", help="Output file path")
    args = parser.parse_args()

    fetch_weather(args.start_date, args.end_date, args.output)

if __name__ == "__main__":
    main()
