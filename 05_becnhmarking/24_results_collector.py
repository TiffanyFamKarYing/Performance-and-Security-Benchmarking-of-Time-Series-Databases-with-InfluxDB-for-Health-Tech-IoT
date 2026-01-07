#!/usr/bin/env python3
"""
Results Collector for Health IoT Database Benchmarking
Collects and consolidates results from all database tests
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

# Basic logger: console + file (UTF-8)
log_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_dir = Path(__file__).parent.parent / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"{Path(__file__).stem}_{log_timestamp}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


class ResultsCollector:
    def __init__(self, base_dir: str = None, run_id: str = None, keep_only_current: bool = False, dry_run: bool = False):
        """Initialize results collector"""
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent
        self.run_id = run_id or datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.keep_only_current = keep_only_current
        self.dry_run = dry_run
        self.output_dir = self.base_dir / "outputs"
        self.log_dir = self.base_dir / "logs"

        # Create directories
        self.output_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)

        # Initialize data structures
        self.results = {
            "metadata": {
                "run_id": self.run_id,
                "timestamp": datetime.now().isoformat(),
                "databases": [],
            },
            "postgresql": {},
            "influxdb": {},
            "mongodb": {},
            "comparison": {},
        }

    def collect_postgresql_results(self):
        """Collect PostgreSQL benchmark results"""
        logger.info("Collecting PostgreSQL results...")

        pg_results = {}

        # Check for PostgreSQL results files
        pg_output_dir = self.output_dir / "postgresql"

        if pg_output_dir.exists():
            # Look for JSON results
            for json_file in pg_output_dir.glob("*.json"):
                try:
                    with open(json_file, "r") as f:
                        data = json.load(f)
                        test_name = json_file.stem
                        pg_results[test_name] = data
                        print(f"  Loaded: {test_name}")
                except Exception as e:
                    print(f"  Error loading {json_file}: {e}")

        # Also check database for results
        try:
            import importlib

            pg_results["database_available"] = (
                importlib.util.find_spec("psycopg2") is not None
            )
        except Exception:
            pg_results["database_available"] = False

        self.results["postgresql"] = pg_results
        return pg_results

    def collect_influxdb_results(self):
        """Collect InfluxDB benchmark results"""
        logger.info("Collecting InfluxDB results...")

        influx_results = {}

        # Check for InfluxDB results files
        influx_output_dir = self.output_dir / "influxdb"

        if influx_output_dir.exists():
            # Look for JSON results
            for json_file in influx_output_dir.glob("*.json"):
                try:
                    with open(json_file, "r") as f:
                        data = json.load(f)
                        test_name = json_file.stem
                        influx_results[test_name] = data
                        print(f"  Loaded: {test_name}")
                except Exception as e:
                    print(f"  Error loading {json_file}: {e}")

        # Also check for CSV results
        for csv_file in influx_output_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_file)
                test_name = csv_file.stem
                influx_results[f"{test_name}_csv"] = df.to_dict("records")
                print(f"  Loaded CSV: {test_name}")
            except Exception as e:
                print(f"  Error loading CSV {csv_file}: {e}")

        self.results["influxdb"] = influx_results
        return influx_results

    def collect_mongodb_results(self):
        """Collect MongoDB benchmark results"""
        logger.info("Collecting MongoDB results...")

        mongo_results = {}

        # Check for MongoDB results files
        mongo_output_dir = self.output_dir / "mongodb_indexing"  # Note: different path

        if mongo_output_dir.exists():
            # Look for JSON results
            for json_file in mongo_output_dir.glob("*.json"):
                try:
                    with open(json_file, "r") as f:
                        data = json.load(f)
                        test_name = json_file.stem
                        mongo_results[test_name] = data
                        print(f"  Loaded: {test_name}")
                except Exception as e:
                    print(f"  Error loading {json_file}: {e}")

        # Also check for text reports
        for txt_file in mongo_output_dir.glob("*.txt"):
            try:
                with open(txt_file, "r") as f:
                    content = f.read()
                    test_name = txt_file.stem
                    mongo_results[f"{test_name}_report"] = content
                    print(f"  Loaded report: {test_name}")
            except Exception as e:
                print(f"  Error loading report {txt_file}: {e}")

        self.results["mongodb"] = mongo_results
        return mongo_results

    def extract_performance_metrics(self):
        """Extract key performance metrics from all results"""
        logger.info("Extracting performance metrics...")

        metrics = {
            "ingestion_performance": {},
            "query_performance": {},
            "storage_efficiency": {},
            "indexing_performance": {},
            "security_overhead": {},
        }

        # PostgreSQL metrics
        pg_metrics = self._extract_postgresql_metrics()
        if pg_metrics:
            for category in metrics.keys():
                if category in pg_metrics:
                    metrics[category]["postgresql"] = pg_metrics[category]

        # InfluxDB metrics
        influx_metrics = self._extract_influxdb_metrics()
        if influx_metrics:
            for category in metrics.keys():
                if category in influx_metrics:
                    metrics[category]["influxdb"] = influx_metrics[category]

        # MongoDB metrics
        mongo_metrics = self._extract_mongodb_metrics()
        if mongo_metrics:
            for category in metrics.keys():
                if category in mongo_metrics:
                    metrics[category]["mongodb"] = mongo_metrics[category]

        self.results["metrics"] = metrics
        return metrics

    def _extract_postgresql_metrics(self) -> Dict[str, Any]:
        """Extract PostgreSQL metrics from results"""
        pg_metrics = {}

        # Extract from PostgreSQL results structure
        pg_data = self.results.get("postgresql", {})

        # Example extraction - you'll need to adjust based on actual results structure
        if "ingestion_performance" in pg_data:
            pg_metrics["ingestion_performance"] = {
                "avg_insert_rate": pg_data["ingestion_performance"].get(
                    "avg_insert_rate", 0
                ),
                "max_insert_rate": pg_data["ingestion_performance"].get(
                    "max_insert_rate", 0
                ),
                "total_time": pg_data["ingestion_performance"].get("total_time", 0),
            }

        # Add more extraction logic for other categories

        return pg_metrics

    def _extract_influxdb_metrics(self) -> Dict[str, Any]:
        """Extract InfluxDB metrics from results"""
        influx_metrics = {}

        influx_data = self.results.get("influxdb", {})

        # Example extraction
        if "ingestion_results" in influx_data:
            influx_metrics["ingestion_performance"] = {
                "points_per_second": influx_data["ingestion_results"].get(
                    "points_per_second", 0
                ),
                "batch_size": influx_data["ingestion_results"].get("batch_size", 0),
                "total_points": influx_data["ingestion_results"].get("total_points", 0),
            }

        return influx_metrics

    def _extract_mongodb_metrics(self) -> Dict[str, Any]:
        """Extract MongoDB metrics from results"""
        mongo_metrics = {}

        mongo_data = self.results.get("mongodb", {})

        # Example extraction
        if "indexing_summary" in mongo_data:
            summary = mongo_data["indexing_summary"]
            mongo_metrics["indexing_performance"] = {
                "avg_improvement": summary.get("data", {}).get("avg_improvement", 0),
                "total_indexes": summary.get("data", {}).get("total_indexes", 0),
                "document_count": summary.get("data", {}).get("document_count", 0),
            }

        return mongo_metrics

    def generate_comparison_table(self):
        """Generate comparison table across all databases"""
        print("\nGenerating comparison table...")

        comparison = {
            "database": ["PostgreSQL", "InfluxDB", "MongoDB"],
            "ingestion_rate": [0, 0, 0],
            "query_latency": [0, 0, 0],
            "storage_size": [0, 0, 0],
            "index_efficiency": [0, 0, 0],
            "security_score": [0, 0, 0],
            "total_score": [0, 0, 0],
        }

        # Calculate scores based on metrics
        metrics = self.results.get("metrics", {})

        # Ingestion performance comparison
        ingestion = metrics.get("ingestion_performance", {})
        if "postgresql" in ingestion:
            comparison["ingestion_rate"][0] = ingestion["postgresql"].get(
                "avg_insert_rate", 0
            )
        if "influxdb" in ingestion:
            comparison["ingestion_rate"][1] = ingestion["influxdb"].get(
                "points_per_second", 0
            )
        if "mongodb" in ingestion:
            comparison["ingestion_rate"][2] = ingestion["mongodb"].get("insert_rate", 0)

        # Normalize and calculate scores
        self._normalize_and_score(comparison)

        # Create DataFrame
        df = pd.DataFrame(comparison)

        # Save to CSV
        comparison_file = self.output_dir / f"comparison_{self.run_id}.csv"
        df.to_csv(comparison_file, index=False)

        logger.info(f"Comparison table saved to: {comparison_file}")

        self.results["comparison_table"] = df.to_dict("records")
        return df

    def _normalize_and_score(self, comparison: Dict[str, List]):
        """Normalize metrics and calculate scores"""

        # For each metric, normalize to 0-100 scale
        metrics_to_normalize = ["ingestion_rate", "index_efficiency"]

        for metric in metrics_to_normalize:
            values = comparison[metric]
            if any(values):
                max_val = max(values)
                if max_val > 0:
                    comparison[metric] = [v / max_val * 100 for v in values]

        # For latency (lower is better), invert
        if any(comparison["query_latency"]):
            max_latency = max(comparison["query_latency"])
            if max_latency > 0:
                comparison["query_latency"] = [
                    (1 - v / max_latency) * 100 for v in comparison["query_latency"]
                ]

        # For storage (smaller is better), invert
        if any(comparison["storage_size"]):
            max_storage = max(comparison["storage_size"])
            if max_storage > 0:
                comparison["storage_size"] = [
                    (1 - v / max_storage) * 100 for v in comparison["storage_size"]
                ]

        # Calculate total score (weighted average)
        weights = {
            "ingestion_rate": 0.25,
            "query_latency": 0.25,
            "storage_size": 0.20,
            "index_efficiency": 0.20,
            "security_score": 0.10,
        }

        for i in range(len(comparison["database"])):
            total = 0
            for metric, weight in weights.items():
                total += comparison[metric][i] * weight
            comparison["total_score"][i] = round(total, 2)

    def generate_summary_report(self):
        """Generate comprehensive summary report"""
        print("\nGenerating summary report...")

        report_file = self.output_dir / f"summary_report_{self.run_id}.txt"

        with open(report_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("HEALTH IOT DATABASE BENCHMARKING - SUMMARY REPORT\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Run ID: {self.run_id}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Database comparison
            f.write("DATABASE COMPARISON\n")
            f.write("-" * 80 + "\n")

            if "comparison_table" in self.results:
                df = pd.DataFrame(self.results["comparison_table"])
                f.write(df.to_string(index=False))
                f.write("\n\n")

            # Winner analysis
            f.write("WINNER ANALYSIS\n")
            f.write("-" * 80 + "\n")

            if "comparison_table" in self.results:
                df = pd.DataFrame(self.results["comparison_table"])
                winner = df.loc[df["total_score"].idxmax()]
                f.write(f"🏆 Best Overall Performance: {winner['database']}\n")
                f.write(f"   Total Score: {winner['total_score']}/100\n\n")

                # Best in each category
                categories = [
                    "ingestion_rate",
                    "query_latency",
                    "storage_size",
                    "index_efficiency",
                    "security_score",
                ]
                category_names = [
                    "Ingestion Rate",
                    "Query Latency",
                    "Storage Efficiency",
                    "Index Efficiency",
                    "Security",
                ]

                for cat, name in zip(categories, category_names):
                    best_db = df.loc[df[cat].idxmax(), "database"]
                    best_score = df[cat].max()
                    f.write(f"📊 Best in {name}: {best_db} ({best_score:.1f}/100)\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("DETAILED FINDINGS\n")
            f.write("=" * 80 + "\n\n")

            # Add detailed findings from each database
            for db in ["postgresql", "influxdb", "mongodb"]:
                if self.results.get(db):
                    f.write(f"\n{db.upper()} FINDINGS:\n")
                    f.write("-" * 40 + "\n")

                    # Add specific findings here based on actual results
                    if db == "postgresql":
                        f.write("• Strong transactional consistency\n")
                        f.write("• Excellent security features (RLS)\n")
                        f.write("• Good query performance for complex joins\n")
                    elif db == "influxdb":
                        f.write("• Superior time-series data ingestion\n")
                        f.write("• Efficient storage for temporal data\n")
                        f.write("• Built-in time-based aggregation\n")
                    elif db == "mongodb":
                        f.write("• Flexible schema design\n")
                        f.write("• Excellent horizontal scalability\n")
                        f.write("• Good for unstructured IoT data\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("RECOMMENDATIONS\n")
            f.write("=" * 80 + "\n\n")

            recommendations = [
                "1. Use PostgreSQL for patient data requiring strict ACID compliance",
                "2. Use InfluxDB for high-frequency sensor data ingestion",
                "3. Use MongoDB for flexible schema evolution and document storage",
                "4. Implement database-specific optimizations based on workload",
                "5. Consider hybrid approaches for different data types",
            ]

            for rec in recommendations:
                f.write(rec + "\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        logger.info(f"Summary report saved to: {report_file}")
        return report_file

    def save_results_to_json(self):
        """Save all collected results to JSON file"""
        json_file = self.output_dir / f"benchmark_results_{self.run_id}.json"

        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2, default=str)

        logger.info(f"Full results saved to JSON: {json_file}")
        return json_file

    def create_sqlite_database(self):
        """Create SQLite database for results storage"""
        print("\nCreating SQLite database for results...")

        db_file = self.output_dir / "benchmark_results.db"

        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Create tables
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS benchmark_runs (
                run_id TEXT PRIMARY KEY,
                timestamp DATETIME,
                total_databases INTEGER,
                total_tests INTEGER
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS database_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                database_name TEXT,
                test_category TEXT,
                metric_name TEXT,
                metric_value REAL,
                unit TEXT,
                FOREIGN KEY (run_id) REFERENCES benchmark_runs (run_id)
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS comparisons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                database_name TEXT,
                ingestion_score REAL,
                query_score REAL,
                storage_score REAL,
                indexing_score REAL,
                security_score REAL,
                total_score REAL,
                FOREIGN KEY (run_id) REFERENCES benchmark_runs (run_id)
            )
        """
        )

        # Insert data
        cursor.execute(
            """
            INSERT OR REPLACE INTO benchmark_runs (run_id, timestamp, total_databases, total_tests)
            VALUES (?, ?, ?, ?)
        """,
            (self.run_id, datetime.now(), 3, 7),
        )  # 3 databases, 7 tests each

        # Insert comparison data if available
        if "comparison_table" in self.results:
            for record in self.results["comparison_table"]:
                cursor.execute(
                    """
                    INSERT INTO comparisons (
                        run_id, database_name, ingestion_score, query_score,
                        storage_score, indexing_score, security_score, total_score
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        self.run_id,
                        record["database"],
                        record["ingestion_rate"],
                        record["query_latency"],
                        record["storage_size"],
                        record["index_efficiency"],
                        record["security_score"],
                        record["total_score"],
                    ),
                )

        conn.commit()
        conn.close()

        print(f"SQLite database created: {db_file}")
        return db_file

    def prune_old_outputs(self, dry_run: bool = False):
        """Remove output files that do not include the current run_id."""
        print("\nPruning old outputs...")
        allowed_exts = {".json", ".csv", ".txt", ".html", ".png", ".md", ".jpg", ".jpeg", ".log", ".db"}
        deleted = []
        for file in self.output_dir.rglob("*"):
            if file.is_file():
                if file.suffix.lower() in allowed_exts:
                    if self.run_id not in file.name:
                        if dry_run or self.dry_run:
                            print(f"  [dry-run] Would delete: {file}")
                            deleted.append(str(file))
                        else:
                            try:
                                file.unlink()
                                deleted.append(str(file))
                                print(f"  Deleted: {file}")
                            except Exception as e:
                                print(f"  Error deleting {file}: {e}")
        logger.info(f"Pruned {len(deleted)} files not matching run_id {self.run_id}")
        return deleted

    def run(self):
        """Run the complete results collection process"""
        print("=" * 80)
        print("RESULTS COLLECTOR - Health IoT Database Benchmarking")
        print("=" * 80)

        # Optionally prune old outputs (keep only current run files)
        if self.keep_only_current:
            self.prune_old_outputs(dry_run=self.dry_run)

        # Collect results from all databases
        self.collect_postgresql_results()
        self.collect_influxdb_results()
        self.collect_mongodb_results()

        # Extract and analyze metrics
        self.extract_performance_metrics()

        # Generate comparison and reports
        self.generate_comparison_table()
        self.generate_summary_report()

        # Save results
        self.save_results_to_json()
        self.create_sqlite_database()

        logger.info("RESULTS COLLECTION COMPLETED SUCCESSFULLY")

        return self.results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Collect and analyze benchmark results"
    )
    parser.add_argument("--run-id", help="Run ID for this benchmark session")
    parser.add_argument("--base-dir", help="Base directory of the project")
    parser.add_argument("--output-dir", help="Output directory for results")
    parser.add_argument("--keep-only-current", action="store_true", help="Delete outputs that do not belong to the specified run id")
    parser.add_argument("--dry-run", action="store_true", help="Show files that would be deleted without removing them")

    args = parser.parse_args()

    try:
        collector = ResultsCollector(
            base_dir=args.base_dir,
            run_id=args.run_id,
            keep_only_current=args.keep_only_current,
            dry_run=args.dry_run,
        )

        collector.run()

        # Print summary
        logger.info("📊 COLLECTION SUMMARY:")
        logger.info(f"   Run ID: {collector.run_id}")
        logger.info(
            f"   PostgreSQL results: {len(collector.results.get('postgresql', {}))} files"
        )
        logger.info(
            f"   InfluxDB results: {len(collector.results.get('influxdb', {}))} files"
        )
        logger.info(
            f"   MongoDB results: {len(collector.results.get('mongodb', {}))} files"
        )

        if "comparison_table" in collector.results:
            logger.info("🏆 WINNERS:")
            df = pd.DataFrame(collector.results["comparison_table"])
            try:
                winner = df.loc[df["total_score"].idxmax()]
                logger.info(
                    f"   Best Overall: {winner['database']} ({winner['total_score']}/100)"
                )
            except Exception:
                logger.warning("Unable to determine winner from comparison table")

        return 0

    except Exception:
        logger.exception("Error during results collection")
        return 1


if __name__ == "__main__":
    sys.exit(main())
