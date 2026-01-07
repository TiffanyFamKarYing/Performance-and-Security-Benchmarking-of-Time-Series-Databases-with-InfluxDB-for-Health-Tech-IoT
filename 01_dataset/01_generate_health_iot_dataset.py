#!/usr/bin/env python3
"""
Health IoT Dataset Generator
Generates synthetic patient vital signs and medical device telemetry
for database benchmarking.
"""

import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


def generate_health_iot_data(num_records=50000, output_dir="."):
    """
    Generate synthetic Health IoT dataset with realistic patterns

    Args:
        num_records: Number of records to generate
        output_dir: Output directory for CSV file

    Returns:
        DataFrame containing generated data
    """

    print("=" * 60)
    print("HEALTH IOT DATASET GENERATION")
    print("=" * 60)

    # Configuration
    patients = [f"PATIENT_{str(i).zfill(5)}" for i in range(1, 101)]
    devices = [f"DEVICE_{str(i).zfill(3)}" for i in range(1, 21)]
    departments = ["ICU", "WARD", "OUTPATIENT", "EMERGENCY", "CARDIOLOGY"]
    vital_types = [
        "heart_rate_bpm",
        "blood_pressure_sys_mmhg",
        "blood_pressure_dia_mmhg",
        "spo2_percent",
        "temperature_c",
        "respiratory_rate_bpm",
        "blood_glucose_mgdl",
    ]

    # Vital sign value ranges (min, max, typical)
    vital_ranges = {
        "heart_rate_bpm": (40, 180, 72),
        "blood_pressure_sys_mmhg": (80, 200, 120),
        "blood_pressure_dia_mmhg": (50, 120, 80),
        "spo2_percent": (70, 100, 98),
        "temperature_c": (35.0, 41.0, 36.8),
        "respiratory_rate_bpm": (8, 40, 16),
        "blood_glucose_mgdl": (70, 400, 100),
    }

    # Generate timestamps (every 2 seconds for 50000 records ≈ 27.8 hours)
    base_time = datetime(2025, 1, 1, 0, 0, 0)
    timestamps = [base_time + timedelta(seconds=i * 2) for i in range(num_records)]

    print(f"Generating {num_records} Health IoT records...")
    print(f"Time range: {timestamps[0]} to {timestamps[-1]}")

    records = []

    for i in range(num_records):
        patient = random.choice(patients)
        device = random.choice(devices)
        department = random.choice(departments)
        vital_type = random.choice(vital_types)

        # Get value range for this vital type
        min_val, max_val, typical = vital_ranges[vital_type]

        # Generate realistic value with some randomness
        if vital_type == "heart_rate_bpm":
            # Heart rate has circadian rhythm
            hour = timestamps[i].hour
            if 2 <= hour <= 6:  # Sleep hours
                base_value = typical - 10 + random.uniform(-5, 5)
            elif 8 <= hour <= 18:  # Active hours
                base_value = typical + 5 + random.uniform(-10, 10)
            else:
                base_value = typical + random.uniform(-8, 8)
        else:
            base_value = typical + random.uniform(-0.2 * typical, 0.2 * typical)

        # Ensure within valid range
        value = max(min_val, min(max_val, base_value))

        # Generate alert flag (5% chance of alert)
        if vital_type == "heart_rate_bpm":
            alert_flag = 1 if value > 120 or value < 50 else 0
        elif vital_type == "spo2_percent":
            alert_flag = 1 if value < 92 else 0
        elif vital_type == "blood_pressure_sys_mmhg":
            alert_flag = 1 if value > 160 or value < 90 else 0
        else:
            alert_flag = 1 if random.random() < 0.05 else 0

        record = {
            "timestamp": timestamps[i],
            "patient_id": patient,
            "device_id": device,
            "vital_type": vital_type,
            "value": round(value, 2),
            "alert_flag": alert_flag,
            "department": department,
            "data_sensitivity": "PHI",  # Protected Health Information
            "ingestion_batch": f"BATCH_{(i // 1000) + 1:03d}",
        }

        records.append(record)

        # Progress indicator
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i + 1:,} records...")

    # Create DataFrame
    df = pd.DataFrame(records)

    # Save to CSV
    output_path = Path(output_dir) / "health_iot_dataset.csv"
    df.to_csv(output_path, index=False)

    # Save schema information
    schema_info = {
        "dataset_name": "Health IoT Vital Signs",
        "records_count": len(df),
        "time_range": {
            "start": df["timestamp"].min().isoformat(),
            "end": df["timestamp"].max().isoformat(),
        },
        "unique_counts": {
            "patients": df["patient_id"].nunique(),
            "devices": df["device_id"].nunique(),
            "vital_types": df["vital_type"].nunique(),
            "departments": df["department"].nunique(),
        },
        "columns": [
            {
                "name": "timestamp",
                "type": "datetime",
                "description": "Measurement timestamp",
            },
            {
                "name": "patient_id",
                "type": "string",
                "description": "Patient identifier",
            },
            {
                "name": "device_id",
                "type": "string",
                "description": "Medical device identifier",
            },
            {
                "name": "vital_type",
                "type": "string",
                "description": "Type of vital sign",
            },
            {
                "name": "value",
                "type": "float",
                "description": "Vital sign measurement value",
            },
            {
                "name": "alert_flag",
                "type": "integer",
                "description": "1 if alert condition, 0 otherwise",
            },
            {
                "name": "department",
                "type": "string",
                "description": "Hospital department",
            },
            {
                "name": "data_sensitivity",
                "type": "string",
                "description": "Data classification (PHI)",
            },
            {
                "name": "ingestion_batch",
                "type": "string",
                "description": "Batch identifier for ingestion",
            },
        ],
        "statistics": {
            "alert_percentage": (df["alert_flag"].sum() / len(df) * 100),
            "avg_value_by_vital": df.groupby("vital_type")["value"]
            .mean()
            .round(2)
            .to_dict(),
        },
    }

    schema_path = Path(output_dir) / "dataset_schema.json"
    with open(schema_path, "w") as f:
        json.dump(schema_info, f, indent=2)

    print("\n" + "=" * 60)
    print("DATASET GENERATION COMPLETE")
    print("=" * 60)
    print(f"Total records: {len(df):,}")
    print(f"Time span: {df['timestamp'].min()} to {df['timestamp'].max()}")
    print(f"Unique patients: {df['patient_id'].nunique()}")
    print(f"Unique devices: {df['device_id'].nunique()}")
    print(f"Alert rate: {(df['alert_flag'].sum() / len(df) * 100):.2f}%")
    print(f"Output file: {output_path}")
    print(f"Schema file: {schema_path}")

    # Display sample data
    print("\nSAMPLE DATA (first 5 records):")
    print(df.head().to_string())

    return df, schema_info


def validate_dataset(df):
    """Validate the generated dataset for quality assurance"""
    print("\n" + "=" * 60)
    print("DATASET VALIDATION")
    print("=" * 60)

    validation_checks = []

    # Check 1: No null values in critical columns
    critical_cols = ["timestamp", "patient_id", "vital_type", "value"]
    null_check = df[critical_cols].isnull().sum().sum() == 0
    validation_checks.append(("No null values in critical columns", null_check))

    # Check 2: Timestamps are in chronological order
    time_order_check = df["timestamp"].is_monotonic_increasing
    validation_checks.append(("Timestamps are chronological", time_order_check))

    # Check 3: Value ranges are reasonable
    value_ranges_valid = True
    for vital_type in df["vital_type"].unique():
        vital_data = df[df["vital_type"] == vital_type]["value"]
        if vital_data.min() < 0 and vital_type not in ["temperature_c"]:
            value_ranges_valid = False
            print(f"  Warning: {vital_type} has negative values")
    validation_checks.append(("Value ranges are reasonable", value_ranges_valid))

    # Check 4: Alert flags are binary
    alert_binary_check = df["alert_flag"].isin([0, 1]).all()
    validation_checks.append(("Alert flags are binary (0 or 1)", alert_binary_check))

    # Check 5: Patient IDs follow pattern
    patient_pattern_check = df["patient_id"].str.match(r"^PATIENT_\d{5}$").all()
    validation_checks.append(("Patient IDs follow pattern", patient_pattern_check))

    # Display validation results
    print("\nValidation Results:")
    for check_name, check_passed in validation_checks:
        status = "✓ PASS" if check_passed else "✗ FAIL"
        print(f"  {status}: {check_name}")

    all_passed = all(check for _, check in validation_checks)
    if all_passed:
        print("\n✓ All validation checks passed!")
    else:
        print("\n✗ Some validation checks failed. Please review the dataset.")

    return all_passed


def analyze_dataset(df):
    """Analyze and report dataset characteristics"""
    print("\n" + "=" * 60)
    print("DATASET ANALYSIS")
    print("=" * 60)

    # Basic statistics
    print("\nBasic Statistics:")
    print(f"  Total records: {len(df):,}")
    print(f"  Size in memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    # Time analysis
    time_span = df["timestamp"].max() - df["timestamp"].min()
    print("\nTime Analysis:")
    print(f"  Time span: {time_span}")
    print(f"  Start: {df['timestamp'].min()}")
    print(f"  End: {df['timestamp'].max()}")
    print(f"  Records per hour: {len(df) / (time_span.total_seconds() / 3600):.1f}")

    # Patient analysis
    print("\nPatient Analysis:")
    patient_counts = df["patient_id"].value_counts()
    print(f"  Unique patients: {len(patient_counts)}")
    print(f"  Avg records per patient: {patient_counts.mean():.1f}")
    print(f"  Min records per patient: {patient_counts.min()}")
    print(f"  Max records per patient: {patient_counts.max()}")

    # Vital type distribution
    print("\nVital Type Distribution:")
    vital_counts = df["vital_type"].value_counts()
    for vital, count in vital_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {vital}: {count:,} records ({percentage:.1f}%)")

    # Alert analysis
    print("\nAlert Analysis:")
    total_alerts = df["alert_flag"].sum()
    alert_rate = (total_alerts / len(df)) * 100
    print(f"  Total alerts: {total_alerts:,}")
    print(f"  Alert rate: {alert_rate:.2f}%")

    # Department distribution
    print("\nDepartment Distribution:")
    dept_counts = df["department"].value_counts()
    for dept, count in dept_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {dept}: {count:,} records ({percentage:.1f}%)")

    # Value statistics by vital type
    print("\nValue Statistics by Vital Type:")
    for vital_type in df["vital_type"].unique():
        vital_data = df[df["vital_type"] == vital_type]["value"]
        print(f"  {vital_type}:")
        print(f"    Min: {vital_data.min():.2f}")
        print(f"    Max: {vital_data.max():.2f}")
        print(f"    Mean: {vital_data.mean():.2f}")
        print(f"    Std Dev: {vital_data.std():.2f}")


def main():
    """Main function to generate, validate, and analyze dataset"""
    import argparse

    parser = argparse.ArgumentParser(description="Generate Health IoT Dataset")
    parser.add_argument(
        "--records",
        type=int,
        default=50000,
        help="Number of records to generate (default: 50000)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Output directory (default: current directory)",
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Only validate existing dataset"
    )
    parser.add_argument(
        "--analyze-only", action="store_true", help="Only analyze existing dataset"
    )

    args = parser.parse_args()

    output_path = Path(args.output_dir) / "health_iot_dataset.csv"

    if args.validate_only or args.analyze_only:
        # Load existing dataset
        if not output_path.exists():
            print(f"Error: Dataset file not found at {output_path}")
            return

        df = pd.read_csv(output_path, parse_dates=["timestamp"])

        if args.validate_only:
            validate_dataset(df)
        elif args.analyze_only:
            analyze_dataset(df)
    else:
        # Generate new dataset
        df, schema_info = generate_health_iot_data(args.records, args.output_dir)

        # Validate and analyze
        validate_dataset(df)
        analyze_dataset(df)

        # Print generation summary
        print("\n" + "=" * 60)
        print("GENERATION SUMMARY")
        print("=" * 60)
        print("Dataset successfully generated with the following characteristics:")
        print(
            f"• {len(df):,} records spanning {df['timestamp'].min()} to {df['timestamp'].max()}"
        )
        print(f"• {df['patient_id'].nunique()} unique patients")
        print(f"• {df['vital_type'].nunique()} different vital sign types")
        print(
            f"• {df['alert_flag'].sum():,} alert records ({df['alert_flag'].mean()*100:.1f}%)"
        )
        print(
            f"• Data classified as {df['data_sensitivity'].iloc[0]} (Protected Health Information)"
        )
        print("\nFiles created:")
        print(f"  • {output_path}")
        print(f"  • {Path(args.output_dir) / 'dataset_schema.json'}")


if __name__ == "__main__":
    main()
