#!/usr/bin/env python3
"""
Imports health IoT data import script from CSV into InfluxDB
"""

import argparse
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS


class HealthIoTDataImporter:
    """Health IoT data importer for InfluxDB"""

    def __init__(self, url: str, token: str, org: str, bucket: str):
        """Initialize InfluxDB client"""
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.org = org
        self.bucket = bucket

    def parse_csv_file(self, file_path: str) -> List[Dict]:
        """Parse CSV file and convert to dictionary format"""
        data_points = []

        with open(file_path, "r") as f:
            reader = csv.DictReader(f)

            for row_num, row in enumerate(reader, 1):
                try:
                    # helper: get first available field value from candidates
                    def _get_field(r, names):
                        for n in names:
                            if n in r and r[n] not in (None, ""):
                                return r[n]
                        return None

                    # Parse timestamp (accept several column names/formats)
                    ts_val = _get_field(
                        row,
                        [
                            "measurement_time",
                            "timestamp",
                            "time",
                            "measurement_time_utc",
                        ],
                    )
                    if ts_val:
                        try:
                            timestamp = datetime.fromisoformat(
                                ts_val.replace("Z", "+00:00")
                            )
                        except Exception:
                            try:
                                timestamp = datetime.strptime(
                                    ts_val, "%Y-%m-%d %H:%M:%S"
                                )
                            except Exception:
                                timestamp = datetime.utcnow()
                    else:
                        timestamp = datetime.utcnow()

                    # Create data point structure
                    data_point = {
                        "measurement": "patient_vitals",
                        "time": timestamp,
                        "tags": {},
                        "fields": {},
                    }

                    # Extract tags (categorical data) - accept variants
                    tag_field_map = {
                        "patient_id": ["patient_id", "patient"],
                        "vital_type": ["vital_type", "vital"],
                        "patient_department": ["patient_department", "department"],
                        "device_id": ["device_id", "device"],
                        "data_classification": [
                            "data_classification",
                            "data_sensitivity",
                        ],
                    }

                    for tag_key, candidates in tag_field_map.items():
                        val = _get_field(row, candidates)
                        if val is not None:
                            data_point["tags"][tag_key] = val

                    # Extract fields (numeric/boolean) - accept alternate names
                    val_val = _get_field(row, ["vital_value", "value", "vital"])
                    if val_val is not None:
                        try:
                            data_point["fields"]["vital_value"] = float(val_val)
                        except (ValueError, TypeError):
                            # skip this row if value invalid
                            continue

                    is_alert_val = _get_field(row, ["is_alert", "alert_flag", "alert"])
                    if is_alert_val is not None:
                        if isinstance(is_alert_val, str):
                            data_point["fields"]["is_alert"] = is_alert_val.lower() in (
                                "true",
                                "1",
                                "yes",
                            )
                        else:
                            data_point["fields"]["is_alert"] = bool(is_alert_val)

                    conf_val = _get_field(row, ["confidence", "confidence_score"])
                    if conf_val is not None:
                        try:
                            data_point["fields"]["confidence"] = float(conf_val)
                        except (ValueError, TypeError):
                            pass

                    data_points.append(data_point)

                except Exception as e:
                    print(f"Error parsing row {row_num}: {e}")
                    continue

        return data_points

    def convert_to_influx_points(self, data_points: List[Dict]) -> List[Point]:
        """Convert data points to InfluxDB Point objects"""
        points = []

        for dp in data_points:
            point = Point(dp["measurement"])

            # Add tags
            for tag_key, tag_value in dp["tags"].items():
                point = point.tag(tag_key, tag_value)

            # Add fields
            for field_key, field_value in dp["fields"].items():
                point = point.field(field_key, field_value)

            # Add timestamp
            point = point.time(dp["time"])

            points.append(point)

        return points

    def import_data(
        self,
        file_path: str,
        batch_size: int = 1000,
        write_mode: str = "sync",
        max_retries: int = 3,
    ) -> Dict:
        """Import data from CSV file to InfluxDB"""

        print(f"Starting import from {file_path}")
        print(f"Batch size: {batch_size}, Write mode: {write_mode}")

        # Parse CSV file
        print("Parsing CSV file...")
        data_points = self.parse_csv_file(file_path)

        if not data_points:
            print("No data points found in file")
            return {}

        print(f"Parsed {len(data_points)} data points")

        # Convert to InfluxDB points
        print("Converting to InfluxDB format...")
        points = self.convert_to_influx_points(data_points)

        # Choose write API
        if write_mode == "async":
            write_api = self.client.write_api(write_options=ASYNCHRONOUS)
        else:
            write_api = self.client.write_api(write_options=SYNCHRONOUS)

        # Import in batches
        total_points = len(points)
        imported_points = 0
        failed_batches = 0
        start_time = time.time()

        print(f"Starting import of {total_points} points...")

        for i in range(0, total_points, batch_size):
            batch = points[i : i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_points + batch_size - 1) // batch_size

            for attempt in range(max_retries):
                try:
                    write_api.write(bucket=self.bucket, record=batch)
                    imported_points += len(batch)

                    # Progress update
                    elapsed = time.time() - start_time
                    rate = imported_points / elapsed if elapsed > 0 else 0

                    print(
                        f"  Batch {batch_num}/{total_batches}: "
                        f"{imported_points}/{total_points} points "
                        f"({rate:.1f} points/sec)"
                    )

                    break  # Success, move to next batch

                except InfluxDBError as e:
                    if attempt < max_retries - 1:
                        print(f"    Retry {attempt + 1}/{max_retries} after error: {e}")
                        time.sleep(2**attempt)  # Exponential backoff
                    else:
                        print(
                            f"    Failed to write batch {batch_num} after {max_retries} attempts: {e}"
                        )
                        failed_batches += 1

            # Small delay between batches to avoid overwhelming
            time.sleep(0.1)

        # Wait for async writes to complete
        if write_mode == "async":
            print("Waiting for async writes to complete...")
            time.sleep(5)

        # Calculate statistics
        end_time = time.time()
        total_time = end_time - start_time
        points_per_second = imported_points / total_time if total_time > 0 else 0

        # Close write API
        write_api.close()

        # Print summary
        print("\n" + "=" * 60)
        print("IMPORT SUMMARY")
        print("=" * 60)
        print(f"Total points in file: {total_points}")
        print(f"Successfully imported: {imported_points}")
        print(f"Failed batches: {failed_batches}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Import rate: {points_per_second:.1f} points/second")

        # Verify import
        if imported_points > 0:
            self.verify_import(imported_points)

        return {
            "total_points": total_points,
            "imported_points": imported_points,
            "failed_batches": failed_batches,
            "total_time_seconds": total_time,
            "points_per_second": points_per_second,
        }

    def verify_import(self, expected_count: int):
        """Verify that data was imported correctly"""
        print("\nVerifying import...")

        try:
            # Query to count imported points
            query = f"""
                from(bucket: "{self.bucket}")
                  |> range(start: -1h)
                  |> filter(fn: (r) => r._measurement == "patient_vitals")
                  |> count()
            """

            tables = self.client.query_api().query(query, org=self.org)

            actual_count = 0
            for table in tables:
                for record in table.records:
                    actual_count = record.get_value()
                    break

            print(f"Points in database: {actual_count}")
            print(f"Expected points: {expected_count}")

            if actual_count >= expected_count * 0.95:  # Allow 5% margin
                print("✓ Import verification passed")
            else:
                print(
                    f"⚠ Import verification warning: "
                    f"Only {actual_count/expected_count*100:.1f}% of points found"
                )

        except Exception as e:
            print(f"Error during verification: {e}")

    def generate_test_data(self, num_points: int, output_file: Optional[str] = None):
        """Generate test data and optionally write to CSV"""

        print(f"Generating {num_points} test data points...")

        # Generate data points
        test_points = []
        base_time = datetime.utcnow()

        vital_types = [
            "heart_rate_bpm",
            "blood_pressure_systolic",
            "blood_pressure_diastolic",
            "temperature_celsius",
            "oxygen_saturation",
            "respiratory_rate",
        ]

        departments = ["ICU", "WARD", "OUTPATIENT", "EMERGENCY", "RECOVERY"]
        devices = [f"DEVICE_{i:03d}" for i in range(1, 101)]
        patients = [f"PATIENT_{i:05d}" for i in range(1, 1001)]

        for i in range(num_points):
            # Generate timestamp with some spread
            timestamp = base_time.replace(microsecond=i % 1000000) - timedelta(
                seconds=i % 86400
            )

            point = {
                "measurement": "patient_vitals",
                "time": timestamp,
                "tags": {
                    "patient_id": patients[i % len(patients)],
                    "vital_type": vital_types[i % len(vital_types)],
                    "patient_department": departments[i % len(departments)],
                    "device_id": devices[i % len(devices)],
                    "data_classification": "INTERNAL",
                },
                "fields": {
                    "vital_value": 50 + (i % 100) + (i % 10) * 0.1,
                    "is_alert": (i % 100) == 0,  # 1% alert rate
                    "confidence": 0.8 + (i % 10) * 0.02,
                },
            }

            test_points.append(point)

        # Write to CSV if output file specified
        if output_file:
            self.write_points_to_csv(test_points, output_file)

        return test_points

    def write_points_to_csv(self, points: List[Dict], output_file: str):
        """Write data points to CSV file"""

        fieldnames = [
            "measurement_time",
            "patient_id",
            "vital_type",
            "vital_value",
            "is_alert",
            "patient_department",
            "device_id",
            "data_classification",
            "confidence",
        ]

        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for point in points:
                row = {
                    "measurement_time": point["time"].isoformat() + "Z",
                    "patient_id": point["tags"].get("patient_id", ""),
                    "vital_type": point["tags"].get("vital_type", ""),
                    "vital_value": point["fields"].get("vital_value", ""),
                    "is_alert": point["fields"].get("is_alert", False),
                    "patient_department": point["tags"].get("patient_department", ""),
                    "device_id": point["tags"].get("device_id", ""),
                    "data_classification": point["tags"].get("data_classification", ""),
                    "confidence": point["fields"].get("confidence", ""),
                }
                writer.writerow(row)

        print(f"Generated CSV file: {output_file}")

    def test_performance(
        self,
        num_points: int = 10000,
        batch_sizes: List[int] = [100, 500, 1000, 5000],
        write_modes: List[str] = ["sync", "async"],
    ):
        """Run performance tests with different configurations"""

        print("=" * 60)
        print("PERFORMANCE TESTING")
        print("=" * 60)

        results = []

        # Generate test data
        test_data = self.generate_test_data(num_points)
        points = self.convert_to_influx_points(test_data)

        for write_mode in write_modes:
            for batch_size in batch_sizes:
                print(f"\nTesting: mode={write_mode}, batch_size={batch_size}")

                # Choose write API
                if write_mode == "async":
                    write_api = self.client.write_api(write_options=ASYNCHRONOUS)
                else:
                    write_api = self.client.write_api(write_options=SYNCHRONOUS)

                # Run test
                start_time = time.time()
                imported = 0

                for i in range(0, len(points), batch_size):
                    batch = points[i : i + batch_size]

                    try:
                        write_api.write(bucket=self.bucket, record=batch)
                        imported += len(batch)
                    except Exception as e:
                        print(f"  Error writing batch: {e}")
                        break

                # Wait for async writes
                if write_mode == "async":
                    time.sleep(2)

                write_api.close()

                # Calculate metrics
                end_time = time.time()
                total_time = end_time - start_time
                points_per_second = imported / total_time if total_time > 0 else 0

                # Store result
                result = {
                    "write_mode": write_mode,
                    "batch_size": batch_size,
                    "points_imported": imported,
                    "total_time_seconds": total_time,
                    "points_per_second": points_per_second,
                }

                results.append(result)

                print(
                    f"  Result: {points_per_second:.1f} points/sec, "
                    f"{total_time:.2f} seconds"
                )

        # Print summary
        print("\n" + "=" * 60)
        print("PERFORMANCE SUMMARY")
        print("=" * 60)

        for result in results:
            print(
                f"Mode: {result['write_mode']:5s} | "
                f"Batch: {result['batch_size']:5d} | "
                f"Rate: {result['points_per_second']:7.1f} pts/sec | "
                f"Time: {result['total_time_seconds']:6.2f}s"
            )

        return results

    def cleanup_test_data(self):
        """Clean up test data from database"""

        print("Cleaning up test data...")

        try:
            # Delete all data in the bucket
            delete_api = self.client.delete_api()

            # Delete everything from last 7 days
            start = "1970-01-01T00:00:00Z"
            stop = datetime.utcnow().isoformat() + "Z"

            delete_api.delete(
                start,
                stop,
                predicate='_measurement="patient_vitals"',
                bucket=self.bucket,
                org=self.org,
            )

            print("Test data cleaned up")

        except Exception as e:
            print(f"Error during cleanup: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Health IoT Data Importer for InfluxDB"
    )

    # Connection parameters
    parser.add_argument("--url", default="http://localhost:8086", help="InfluxDB URL")
    parser.add_argument("--token", required=True, help="InfluxDB token")
    parser.add_argument("--org", default="HealthIoT", help="Organization name")
    parser.add_argument("--bucket", default="health_iot_metrics", help="Bucket name")

    # Operation mode
    parser.add_argument(
        "--mode",
        choices=["import", "generate", "test", "cleanup"],
        required=True,
        help="Operation mode",
    )

    # Import parameters
    parser.add_argument("--input-file", help="Input CSV file for import")
    parser.add_argument("--output-file", help="Output CSV file for generation")
    parser.add_argument("--points", type=int, default=10000, help="Number of points")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size")
    parser.add_argument(
        "--write-mode", choices=["sync", "async"], default="sync", help="Write mode"
    )

    args = parser.parse_args()

    # Initialize importer
    importer = HealthIoTDataImporter(
        url=args.url, token=args.token, org=args.org, bucket=args.bucket
    )

    try:
        if args.mode == "import":
            if not args.input_file:
                print("Error: --input-file required for import mode")
                return

            if not Path(args.input_file).exists():
                print(f"Error: Input file '{args.input_file}' not found")
                return

            importer.import_data(
                file_path=args.input_file,
                batch_size=args.batch_size,
                write_mode=args.write_mode,
            )

        elif args.mode == "generate":
            points = importer.generate_test_data(
                num_points=args.points, output_file=args.output_file
            )

            print(f"Generated {len(points)} data points")
            if args.output_file:
                print(f"Saved to: {args.output_file}")

        elif args.mode == "test":
            importer.test_performance(
                num_points=args.points,
                batch_sizes=[100, 500, 1000, 5000],
                write_modes=["sync", "async"],
            )

        elif args.mode == "cleanup":
            confirm = input("Are you sure you want to delete all test data? (yes/no): ")
            if confirm.lower() == "yes":
                importer.cleanup_test_data()
            else:
                print("Cleanup cancelled")

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

