# TaxiPulse — Data Quality & Veracity Report

> **Plain Language Summary:** Out of all raw taxi records processed, invalid trips (such as negative fares, zero distances, and taximeter hardware glitches) were systematically filtered to ensure clean, high-fidelity datasets for real-time demand forecasting.

## Overall Data Quality Metrics

- **Total Raw Trip Records Ingested:** 38,406,668
- **Total Cleaned Trip Records Retained:** 36,675,432
- **Total Anomalous Records Filtered:** 1,731,236
- **Overall Data Retention Rate:** 95.49%

## Detailed Veracity Filter Breakdown

| Rule # | Filter Description | Real-World Justification | Rows Removed | % of Raw Data |
|---|---|---|---|---|
| 01 | Null Keys | Missing pickup/dropoff timestamp or zone ID | 0 | 0.00% |
| 02 | Invalid Zone IDs | Location IDs outside valid 1-263 range | 1,425,700 | 3.71% |
| 03 | Non-Positive Fare | Refunded, voided, or test transactions (fare <= 0) | 88,750 | 0.23% |
| 04 | Absurd Fare | Data entry errors (fare > $1,000) | 17 | 0.00% |
| 05 | Non-Positive Distance | Trips canceled before vehicle movement (dist <= 0) | 100,261 | 0.26% |
| 06 | Absurd Distance | Out-of-state GPS glitches (dist > 200 miles) | 251 | 0.00% |
| 07 | Time Inversion | Dropoff timestamp earlier than pickup timestamp | 1,045 | 0.00% |
| 08 | Duration Anomalies | Accidental starts (<60s) or unclosed meters (>6h) | 25,701 | 0.07% |
| 10 | Impossible Speed | GPS anomalies (>90 mph or <0.5 mph) | 6,143 | 0.02% |
| 11 | Out-of-Period | Hardware desync stray dates outside file month | 16 | 0.00% |
| 12 | Passenger Count | Unrealistic passenger counts (>8 passengers) | 83,339 | 0.22% |
| 13 | Duplicate Records | Re-transmitted telemetry packets or gateway double submissions | 13 | 0.00% |
