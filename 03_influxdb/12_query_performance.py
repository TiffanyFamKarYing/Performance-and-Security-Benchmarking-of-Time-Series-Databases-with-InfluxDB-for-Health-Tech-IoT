#!/usr/bin/env python3
"""
InfluxDB Query Performance Testing
Comprehensive query performance analysis for Health IoT data
"""

import argparse
import json
import os
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
from influxdb_client import InfluxDBClient


@dataclass
class QueryResult:
    """Container for query performance results"""

    query_name: str
    query_type: str
    execution_times: List[float]
    result_counts: List[int]
    avg_time: float
    min_time: float
    max_time: float
    std_dev: float
    throughput: float  # results per second
    success_rate: float
    query_text: str


class QueryPerformanceTester:
    """Comprehensive query performance tester for InfluxDB"""

    def __init__(self, url: str, token: str, org: str, bucket: str):
        """Initialize InfluxDB client"""
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.org = org
        self.bucket = bucket
        self.results: List[QueryResult] = []

    def define_test_queries(self) -> List[Dict]:
        """Define comprehensive test queries for performance testing"""

        return [
            {
                "name": "simple_recent_data",
                "type": "simple_select",
                "description": "Select recent data with limit",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> limit(n: 1000)
                """,
            },
            {
                "name": "time_range_aggregation",
                "type": "aggregation",
                "description": "Hourly aggregation over 24 hours",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -24h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
                """,
            },
            {
                "name": "group_by_patient",
                "type": "group_by",
                "description": "Group by patient and calculate statistics",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -24h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> group(columns: ["patient_id", "vital_type"])
                      |> mean()
                      |> sort(columns: ["_value"], desc: true)
                """,
            },
            {
                "name": "complex_aggregation",
                "type": "complex",
                "description": "Multiple aggregations with filtering",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -7d)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> filter(fn: (r) => r.vital_type == "heart_rate_bpm")
                      |> group(columns: ["patient_department"])
                      |> aggregateWindow(
                          every: 1h,
                          fn: (column, tables=<-) => tables
                            |> mean(column: column)
                            |> map(fn: (r) => ({{r with _value: round(x: r._value, precision: 1)}}))
                        )
                      |> mean()
                """,
            },
            {
                "name": "alert_detection",
                "type": "filtering",
                "description": "Filter alerts with conditions",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "is_alert")
                      |> filter(fn: (r) => r._value == true)
                      |> count()
                """,
            },
            {
                "name": "join_simulation",
                "type": "complex",
                "description": "Simulate join-like behavior",
                "query": f"""
                    alerts = from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "is_alert")
                      |> filter(fn: (r) => r._value == true)
                    
                    vitals = from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                    
                    join(tables: {{alerts: alerts, vitals: vitals}}, on: ["patient_id", "_time"])
                      |> yield(name: "joined_data")
                """,
            },
            {
                "name": "large_time_range",
                "type": "large_scale",
                "description": "Query large time range (30 days)",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -30d)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> aggregateWindow(every: 6h, fn: mean, createEmpty: false)
                      |> limit(n: 1000)
                """,
            },
            {
                "name": "high_cardinality",
                "type": "high_cardinality",
                "description": "Query with high cardinality groups",
                "query": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> group(columns: ["patient_id", "vital_type", "device_id"])
                      |> count()
                """,
            },
        ]

    def execute_query(
        self, query: str, max_retries: int = 3
    ) -> Tuple[float, int, Optional[str]]:
        """Execute a query and measure execution time"""

        for attempt in range(max_retries):
            try:
                start_time = time.perf_counter()

                # Execute query
                tables = self.client.query_api().query(query, org=self.org)

                end_time = time.perf_counter()
                execution_time = end_time - start_time

                # Count results
                result_count = 0
                for table in tables:
                    result_count += len(table.records)

                return execution_time, result_count, None

            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"  Query failed, retry {attempt + 1}/{max_retries}: {e}")
                    time.sleep(1)
                else:
                    return 0, 0, str(e)

        return 0, 0, "Max retries exceeded"

    def run_performance_test(
        self,
        queries: List[Dict],
        iterations: int = 5,
        warmup_iterations: int = 2,
        delay_between_queries: float = 1.0,
    ) -> List[QueryResult]:
        """Run comprehensive performance tests"""

        print(f"Running performance tests with {iterations} iterations")
        print(f"Warmup iterations: {warmup_iterations}")
        print("=" * 80)

        all_results = []

        for query_def in queries:
            print(f"\nTesting: {query_def['name']}")
            print(f"Description: {query_def['description']}")
            print("-" * 40)

            execution_times = []
            result_counts = []
            errors = []

            # Warmup runs (not counted)
            for i in range(warmup_iterations):
                print(f"  Warmup {i+1}/{warmup_iterations}", end="\r")
                self.execute_query(query_def["query"])
                time.sleep(0.5)

            print("  " + " " * 30)  # Clear line

            # Actual test runs
            for i in range(iterations):
                exec_time, result_count, error = self.execute_query(query_def["query"])

                if error:
                    errors.append(error)
                    print(f"  Iteration {i+1}: ERROR - {error}")
                else:
                    execution_times.append(exec_time)
                    result_counts.append(result_count)
                    print(
                        f"  Iteration {i+1}: {exec_time:.3f}s, {result_count} results"
                    )

                # Delay between iterations
                if i < iterations - 1:
                    time.sleep(delay_between_queries)

            # Calculate statistics
            if execution_times:
                avg_time = statistics.mean(execution_times)
                min_time = min(execution_times)
                max_time = max(execution_times)
                std_dev = (
                    statistics.stdev(execution_times) if len(execution_times) > 1 else 0
                )

                # Calculate throughput (results per second)
                avg_results = statistics.mean(result_counts)
                throughput = avg_results / avg_time if avg_time > 0 else 0

                # Calculate success rate
                success_rate = len(execution_times) / iterations

                # Create result object
                result = QueryResult(
                    query_name=query_def["name"],
                    query_type=query_def["type"],
                    execution_times=execution_times,
                    result_counts=result_counts,
                    avg_time=avg_time,
                    min_time=min_time,
                    max_time=max_time,
                    std_dev=std_dev,
                    throughput=throughput,
                    success_rate=success_rate,
                    query_text=query_def["query"],
                )

                all_results.append(result)

                # Print summary
                print("\n  Summary:")
                print(f"    Avg time: {avg_time:.3f}s")
                print(f"    Range: {min_time:.3f}s - {max_time:.3f}s")
                print(f"    Std Dev: {std_dev:.3f}s")
                print(f"    Throughput: {throughput:.1f} results/sec")
                print(f"    Success rate: {success_rate:.1%}")

            else:
                print("  No successful executions for this query")

        self.results = all_results
        return all_results

    def analyze_results(self, results: List[QueryResult]) -> Dict:
        """Analyze and summarize performance results"""

        summary = {
            "total_queries_tested": len(results),
            "successful_queries": sum(1 for r in results if r.success_rate > 0),
            "total_iterations": sum(len(r.execution_times) for r in results),
            "query_categories": {},
            "performance_by_type": {},
            "recommendations": [],
        }

        # Analyze by query type
        query_types = set(r.query_type for r in results)

        for query_type in query_types:
            type_results = [r for r in results if r.query_type == query_type]

            if type_results:
                avg_times = [r.avg_time for r in type_results]
                throughputs = [r.throughput for r in type_results]

                summary["query_categories"][query_type] = {
                    "count": len(type_results),
                    "avg_execution_time": statistics.mean(avg_times),
                    "min_execution_time": min(avg_times),
                    "max_execution_time": max(avg_times),
                    "avg_throughput": statistics.mean(throughputs),
                    "queries": [r.query_name for r in type_results],
                }

        # Find fastest and slowest queries
        if results:
            fastest = min(results, key=lambda x: x.avg_time)
            slowest = max(results, key=lambda x: x.avg_time)
            highest_throughput = max(results, key=lambda x: x.throughput)

            summary["performance_extremes"] = {
                "fastest_query": {
                    "name": fastest.query_name,
                    "type": fastest.query_type,
                    "avg_time": fastest.avg_time,
                    "throughput": fastest.throughput,
                },
                "slowest_query": {
                    "name": slowest.query_name,
                    "type": slowest.query_type,
                    "avg_time": slowest.avg_time,
                    "throughput": slowest.throughput,
                },
                "highest_throughput": {
                    "name": highest_throughput.query_name,
                    "type": highest_throughput.query_type,
                    "avg_time": highest_throughput.avg_time,
                    "throughput": highest_throughput.throughput,
                },
            }

        # Generate recommendations
        recommendations = []

        # Check for slow queries
        slow_threshold = 2.0  # seconds
        slow_queries = [r for r in results if r.avg_time > slow_threshold]

        if slow_queries:
            recommendations.append(
                {
                    "type": "performance",
                    "severity": "high",
                    "message": f"Found {len(slow_queries)} queries with avg time > {slow_threshold}s",
                    "queries": [r.query_name for r in slow_queries],
                    "suggestion": "Consider optimizing these queries or adding indexes",
                }
            )

        # Check for high variance
        variance_threshold = 0.5  # seconds
        high_variance_queries = [r for r in results if r.std_dev > variance_threshold]

        if high_variance_queries:
            recommendations.append(
                {
                    "type": "consistency",
                    "severity": "medium",
                    "message": f"Found {len(high_variance_queries)} queries with high execution time variance",
                    "queries": [r.query_name for r in high_variance_queries],
                    "suggestion": "Investigate caching behavior or resource contention",
                }
            )

        # Check for low throughput
        throughput_threshold = 100  # results/second
        low_throughput_queries = [
            r
            for r in results
            if r.throughput < throughput_threshold and r.throughput > 0
        ]

        if low_throughput_queries:
            recommendations.append(
                {
                    "type": "throughput",
                    "severity": "medium",
                    "message": f"Found {len(low_throughput_queries)} queries with throughput < {throughput_threshold}/s",
                    "queries": [r.query_name for r in low_throughput_queries],
                    "suggestion": "Consider query optimization or data partitioning",
                }
            )

        summary["recommendations"] = recommendations

        return summary

    def generate_report(self, results: List[QueryResult], summary: Dict) -> str:
        """Generate comprehensive performance report"""

        report = []
        report.append("=" * 80)
        report.append("INFLUXDB QUERY PERFORMANCE REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().isoformat()}")
        report.append(f"Bucket: {self.bucket}")
        report.append(f"Organization: {self.org}")
        report.append("")

        # Summary section
        report.append("SUMMARY")
        report.append("-" * 40)
        report.append(f"Total queries tested: {summary['total_queries_tested']}")
        report.append(f"Successful queries: {summary['successful_queries']}")
        report.append(f"Total iterations: {summary['total_iterations']}")
        report.append("")

        # Performance by type
        report.append("PERFORMANCE BY QUERY TYPE")
        report.append("-" * 40)

        for query_type, stats in summary.get("query_categories", {}).items():
            report.append(f"\n{query_type.upper()}:")
            report.append(f"  Number of queries: {stats['count']}")
            report.append(f"  Avg execution time: {stats['avg_execution_time']:.3f}s")
            report.append(
                f"  Range: {stats['min_execution_time']:.3f}s - {stats['max_execution_time']:.3f}s"
            )
            report.append(
                f"  Avg throughput: {stats['avg_throughput']:.1f} results/sec"
            )
            report.append(f"  Queries: {', '.join(stats['queries'])}")

        # Extremes
        if "performance_extremes" in summary:
            extremes = summary["performance_extremes"]
            report.append("\nPERFORMANCE EXTREMES")
            report.append("-" * 40)

            report.append("\nFastest Query:")
            report.append(f"  Name: {extremes['fastest_query']['name']}")
            report.append(f"  Type: {extremes['fastest_query']['type']}")
            report.append(f"  Avg Time: {extremes['fastest_query']['avg_time']:.3f}s")
            report.append(
                f"  Throughput: {extremes['fastest_query']['throughput']:.1f}/s"
            )

            report.append("\nSlowest Query:")
            report.append(f"  Name: {extremes['slowest_query']['name']}")
            report.append(f"  Type: {extremes['slowest_query']['type']}")
            report.append(f"  Avg Time: {extremes['slowest_query']['avg_time']:.3f}s")
            report.append(
                f"  Throughput: {extremes['slowest_query']['throughput']:.1f}/s"
            )

            report.append("\nHighest Throughput:")
            report.append(f"  Name: {extremes['highest_throughput']['name']}")
            report.append(f"  Type: {extremes['highest_throughput']['type']}")
            report.append(
                f"  Avg Time: {extremes['highest_throughput']['avg_time']:.3f}s"
            )
            report.append(
                f"  Throughput: {extremes['highest_throughput']['throughput']:.1f}/s"
            )

        # Detailed results
        report.append("\n" + "=" * 80)
        report.append("DETAILED QUERY RESULTS")
        report.append("=" * 80)

        for result in results:
            report.append(f"\nQuery: {result.query_name}")
            report.append(f"Type: {result.query_type}")
            report.append(f"Avg Time: {result.avg_time:.3f}s")
            report.append(f"Min/Max: {result.min_time:.3f}s / {result.max_time:.3f}s")
            report.append(f"Std Dev: {result.std_dev:.3f}s")
            report.append(f"Throughput: {result.throughput:.1f} results/sec")
            report.append(f"Success Rate: {result.success_rate:.1%}")
            report.append(f"Result Counts: {result.result_counts}")
            report.append(
                f"Execution Times: {[f'{t:.3f}' for t in result.execution_times]}"
            )

        # Recommendations
        if summary.get("recommendations"):
            report.append("\n" + "=" * 80)
            report.append("RECOMMENDATIONS")
            report.append("=" * 80)

            for i, rec in enumerate(summary["recommendations"], 1):
                report.append(
                    f"\n{i}. {rec['type'].upper()} - {rec['severity'].upper()}:"
                )
                report.append(f"   {rec['message']}")
                report.append(f"   Affected queries: {', '.join(rec['queries'])}")
                report.append(f"   Suggestion: {rec['suggestion']}")

        # Query texts (appendix)
        report.append("\n" + "=" * 80)
        report.append("QUERY DEFINITIONS")
        report.append("=" * 80)

        for result in results:
            report.append(f"\n{result.query_name}:")
            report.append("-" * 40)
            report.append(result.query_text.strip())

        return "\n".join(report)

    def save_results(
        self, results: List[QueryResult], summary: Dict, output_dir: str = "./results"
    ):
        """Save results to files"""

        import os

        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save JSON data
        json_data = {
            "metadata": {
                "timestamp": timestamp,
                "bucket": self.bucket,
                "org": self.org,
                "test_type": "query_performance",
            },
            "summary": summary,
            "detailed_results": [
                {
                    "query_name": r.query_name,
                    "query_type": r.query_type,
                    "avg_time": r.avg_time,
                    "min_time": r.min_time,
                    "max_time": r.max_time,
                    "std_dev": r.std_dev,
                    "throughput": r.throughput,
                    "success_rate": r.success_rate,
                    "execution_times": r.execution_times,
                    "result_counts": r.result_counts,
                    "query_text": r.query_text,
                }
                for r in results
            ],
        }

        json_file = os.path.join(output_dir, f"query_performance_{timestamp}.json")
        with open(json_file, "w") as f:
            json.dump(json_data, f, indent=2, default=str)

        # Save text report
        report = self.generate_report(results, summary)
        report_file = os.path.join(output_dir, f"query_performance_{timestamp}.txt")
        with open(report_file, "w") as f:
            f.write(report)

        # Generate visualization
        self.create_visualization(results, output_dir, timestamp)

        print("\nResults saved to:")
        print(f"  JSON: {json_file}")
        print(f"  Report: {report_file}")

        return json_file, report_file

    def create_visualization(
        self, results: List[QueryResult], output_dir: str, timestamp: str
    ):
        """Create visualization charts"""

        try:
            import matplotlib

            matplotlib.use("Agg")  # Use non-interactive backend

            # Create figure with subplots
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle(
                "InfluxDB Query Performance Analysis", fontsize=16, fontweight="bold"
            )

            # Plot 1: Execution times by query
            ax1 = axes[0, 0]
            query_names = [r.query_name for r in results]
            avg_times = [r.avg_time for r in results]

            x_pos = np.arange(len(query_names))
            ax1.bar(
                x_pos,
                avg_times,
                yerr=[avg_times, avg_times],
                capsize=5,
                alpha=0.7,
                color="steelblue",
            )
            ax1.set_xlabel("Query Name")
            ax1.set_ylabel("Execution Time (seconds)")
            ax1.set_title("Average Execution Time by Query")
            ax1.set_xticks(x_pos)
            ax1.set_xticklabels(query_names, rotation=45, ha="right")
            ax1.grid(True, alpha=0.3)

            # Plot 2: Throughput by query
            ax2 = axes[0, 1]
            throughputs = [r.throughput for r in results]
            ax2.bar(x_pos, throughputs, alpha=0.7, color="forestgreen")
            ax2.set_xlabel("Query Name")
            ax2.set_ylabel("Throughput (results/second)")
            ax2.set_title("Query Throughput")
            ax2.set_xticks(x_pos)
            ax2.set_xticklabels(query_names, rotation=45, ha="right")
            ax2.grid(True, alpha=0.3)

            # Plot 3: Execution time distribution (box plot)
            ax3 = axes[1, 0]
            time_data = [r.execution_times for r in results]
            ax3.boxplot(time_data, labels=query_names)
            ax3.set_xlabel("Query Name")
            ax3.set_ylabel("Execution Time (seconds)")
            ax3.set_title("Execution Time Distribution")
            ax3.tick_params(axis="x", rotation=45)
            ax3.grid(True, alpha=0.3)

            # Plot 4: Query type comparison
            ax4 = axes[1, 1]
            query_types = {}
            for result in results:
                if result.query_type not in query_types:
                    query_types[result.query_type] = []
                query_types[result.query_type].append(result.avg_time)

            type_avg_times = [statistics.mean(times) for times in query_types.values()]
            type_names = list(query_types.keys())

            x_pos_types = np.arange(len(type_names))
            ax4.bar(x_pos_types, type_avg_times, alpha=0.7, color="coral")
            ax4.set_xlabel("Query Type")
            ax4.set_ylabel("Average Execution Time (seconds)")
            ax4.set_title("Performance by Query Type")
            ax4.set_xticks(x_pos_types)
            ax4.set_xticklabels(type_names, rotation=45, ha="right")
            ax4.grid(True, alpha=0.3)

            # Adjust layout
            plt.tight_layout()

            # Save figure
            plot_file = os.path.join(output_dir, f"query_performance_{timestamp}.png")
            plt.savefig(plot_file, dpi=300, bbox_inches="tight")
            plt.close()

            print(f"  Visualization: {plot_file}")

        except ImportError:
            print("  Visualization skipped: matplotlib not installed")
        except Exception as e:
            print(f"  Error creating visualization: {e}")

    def run_comprehensive_test(self, output_dir: str = "./results"):
        """Run comprehensive performance test suite"""

        print("=" * 80)
        print("INFLUXDB COMPREHENSIVE QUERY PERFORMANCE TEST")
        print("=" * 80)

        # Define test queries
        test_queries = self.define_test_queries()

        print(f"\nDefined {len(test_queries)} test queries:")
        for q in test_queries:
            print(f"  • {q['name']}: {q['description']}")

        # Run performance tests
        print("\n" + "=" * 80)
        print("RUNNING PERFORMANCE TESTS")
        print("=" * 80)

        results = self.run_performance_test(
            queries=test_queries,
            iterations=5,
            warmup_iterations=2,
            delay_between_queries=1.0,
        )

        # Analyze results
        print("\n" + "=" * 80)
        print("ANALYZING RESULTS")
        print("=" * 80)

        summary = self.analyze_results(results)

        # Print summary
        print("\nTEST SUMMARY:")
        print(f"  Queries tested: {summary['total_queries_tested']}")
        print(f"  Successful queries: {summary['successful_queries']}")

        if "performance_extremes" in summary:
            extremes = summary["performance_extremes"]
            print(
                f"\n  Fastest query: {extremes['fastest_query']['name']} "
                f"({extremes['fastest_query']['avg_time']:.3f}s)"
            )
            print(
                f"  Slowest query: {extremes['slowest_query']['name']} "
                f"({extremes['slowest_query']['avg_time']:.3f}s)"
            )
            print(
                f"  Highest throughput: {extremes['highest_throughput']['name']} "
                f"({extremes['highest_throughput']['throughput']:.1f} results/sec)"
            )

        # Generate and save report
        print("\n" + "=" * 80)
        print("GENERATING REPORT")
        print("=" * 80)

        json_file, report_file = self.save_results(results, summary, output_dir)

        # Print recommendations
        if summary.get("recommendations"):
            print("\nRECOMMENDATIONS:")
            for rec in summary["recommendations"]:
                print(f"  • {rec['message']}")

        print("\n" + "=" * 80)
        print("TEST COMPLETE")
        print("=" * 80)

        return results, summary


def main():
    parser = argparse.ArgumentParser(
        description="InfluxDB Query Performance Testing Tool"
    )

    # Connection parameters
    parser.add_argument("--url", default="http://localhost:8086", help="InfluxDB URL")
    parser.add_argument(
        "--token",
        default=os.getenv("INFLUXDB_TOKEN"),
        help="InfluxDB authentication token (or set INFLUXDB_TOKEN env var)",
    )
    parser.add_argument("--org", default="HealthIoT", help="Organization name")
    parser.add_argument("--bucket", default="health_iot_metrics", help="Bucket name")

    # Test parameters
    parser.add_argument(
        "--iterations", type=int, default=5, help="Number of iterations per query"
    )
    parser.add_argument(
        "--output-dir", default="./results", help="Output directory for results"
    )
    parser.add_argument(
        "--single-query", help="Test a single query (provide Flux query)"
    )
    parser.add_argument("--query-file", help="Test queries from a file (JSON format)")

    args = parser.parse_args()

    # Ensure token is provided either via CLI or INFLUXDB_TOKEN env var
    if not args.token:
        parser.error(
            "InfluxDB token is required; provide --token or set INFLUXDB_TOKEN environment variable"
        )

    # Initialize tester
    tester = QueryPerformanceTester(
        url=args.url, token=args.token, org=args.org, bucket=args.bucket
    )

    try:
        if args.single_query:
            # Test single query
            print("Testing single query...")
            exec_time, result_count, error = tester.execute_query(args.single_query)

            if error:
                print(f"Error: {error}")
            else:
                print(f"Execution time: {exec_time:.3f} seconds")
                print(f"Results returned: {result_count}")
                print(f"Throughput: {result_count/exec_time:.1f} results/second")

        elif args.query_file:
            # Test queries from file
            import json

            with open(args.query_file, "r") as f:
                test_queries = json.load(f)

            results = tester.run_performance_test(
                queries=test_queries, iterations=args.iterations
            )

            summary = tester.analyze_results(results)
            tester.save_results(results, summary, args.output_dir)

        else:
            # Run comprehensive test
            tester.run_comprehensive_test(output_dir=args.output_dir)

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\nError during testing: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
