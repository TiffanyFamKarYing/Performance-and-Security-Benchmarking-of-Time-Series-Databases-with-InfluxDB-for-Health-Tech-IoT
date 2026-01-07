#!/usr/bin/env python3
"""
InfluxDB Indexing and Performance Analysis
Analyzes indexing effectiveness and query performance
"""

import argparse
import json
import statistics
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

from influxdb_client import InfluxDBClient


@dataclass
class IndexAnalysisResult:
    """Container for index analysis results"""

    query_name: str
    query_type: str
    execution_time_with_index: float
    execution_time_without_index: float
    performance_improvement: float
    result_count: int
    scan_type: str
    series_scanned: int
    bytes_scanned: int
    recommendation: str
    query_text: str


class InfluxDBIndexAnalyzer:
    """Analyzer for InfluxDB indexing and query performance"""

    def __init__(self, url: str, token: str, org: str, bucket: str):
        """Initialize InfluxDB client"""
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.org = org
        self.bucket = bucket

    def analyze_query_patterns(self, days: int = 7) -> Dict:
        """Analyze query patterns to identify indexing opportunities"""

        print(f"Analyzing query patterns from last {days} days...")

        # Note: InfluxDB doesn't have traditional indexes like SQL databases.
        # Instead, we analyze data organization and query patterns.

        analysis_results = {
            "time_range_analysis": self.analyze_time_ranges(days),
            "tag_usage_analysis": self.analyze_tag_usage(days),
            "field_usage_analysis": self.analyze_field_usage(days),
            "common_filters": self.analyze_common_filters(days),
            "aggregation_patterns": self.analyze_aggregation_patterns(days),
        }

        return analysis_results

    def analyze_time_ranges(self, days: int) -> Dict:
        """Analyze common time ranges in queries"""

        # In InfluxDB, time is automatically indexed
        # We analyze common time ranges to optimize data retention

        query = f"""
            from(bucket: "{self.bucket}")
              |> range(start: -{days}d)
              |> filter(fn: (r) => r._measurement == "patient_vitals")
              |> group(columns: ["_measurement"])
              |> aggregateWindow(every: 1d, fn: count, createEmpty: false)
              |> sort(columns: ["_time"], desc: true)
        """

        try:
            tables = self.client.query_api().query(query, org=self.org)

            daily_counts = []
            for table in tables:
                for record in table.records:
                    daily_counts.append(
                        {"date": record.get_time(), "count": record.get_value()}
                    )

            if daily_counts:
                avg_daily = statistics.mean([d["count"] for d in daily_counts])
                max_daily = max([d["count"] for d in daily_counts])
                min_daily = min([d["count"] for d in daily_counts])

                return {
                    "avg_daily_points": avg_daily,
                    "max_daily_points": max_daily,
                    "min_daily_points": min_daily,
                    "total_points": sum([d["count"] for d in daily_counts]),
                    "trend": (
                        "stable"
                        if (max_daily - min_daily) / avg_daily < 0.5
                        else "variable"
                    ),
                }

        except Exception as e:
            print(f"Error analyzing time ranges: {e}")

        return {}

    def analyze_tag_usage(self, days: int) -> Dict:
        """Analyze tag usage patterns"""

        # In InfluxDB, tags are indexed automatically
        # We analyze tag cardinality and usage frequency

        tags_to_analyze = [
            "patient_id",
            "vital_type",
            "patient_department",
            "device_id",
        ]

        results = {}

        for tag in tags_to_analyze:
            query = f"""
                from(bucket: "{self.bucket}")
                  |> range(start: -{days}d)
                  |> filter(fn: (r) => r._measurement == "patient_vitals")
                  |> group(columns: ["{tag}"])
                  |> count()
                  |> group()
                  |> sort(columns: ["_value"], desc: true)
                  |> limit(n: 10)
            """

            try:
                tables = self.client.query_api().query(query, org=self.org)

                tag_values = []
                for table in tables:
                    for record in table.records:
                        tag_values.append(
                            {
                                "value": record.values.get(tag),
                                "count": record.get_value(),
                            }
                        )

                # Get cardinality
                cardinality_query = f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -{days}d)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> group(columns: ["{tag}"])
                      |> distinct(column: "{tag}")
                      |> count()
                """

                cardinality_tables = self.client.query_api().query(
                    cardinality_query, org=self.org
                )
                cardinality = 0
                for table in cardinality_tables:
                    for record in table.records:
                        cardinality = record.get_value()

                results[tag] = {
                    "cardinality": cardinality,
                    "top_values": tag_values[:5],
                    "cardinality_category": self._categorize_cardinality(cardinality),
                }

            except Exception as e:
                print(f"Error analyzing tag {tag}: {e}")
                results[tag] = {"error": str(e)}

        return results

    def analyze_field_usage(self, days: int) -> Dict:
        """Analyze field usage patterns"""

        query = f"""
            from(bucket: "{self.bucket}")
              |> range(start: -{days}d)
              |> filter(fn: (r) => r._measurement == "patient_vitals")
              |> group(columns: ["_field"])
              |> count()
              |> sort(columns: ["_value"], desc: true)
        """

        try:
            tables = self.client.query_api().query(query, org=self.org)

            field_usage = []
            for table in tables:
                for record in table.records:
                    field_usage.append(
                        {
                            "field": record.values.get("_field"),
                            "count": record.get_value(),
                        }
                    )

            return {
                "fields": field_usage,
                "most_used_field": field_usage[0]["field"] if field_usage else None,
                "field_count": len(field_usage),
            }

        except Exception as e:
            print(f"Error analyzing field usage: {e}")
            return {}

    def analyze_common_filters(self, days: int) -> Dict:
        """Analyze common filter patterns in queries"""

        # This is a simplified analysis
        # In production, you'd want to parse actual query logs

        common_filters = {
            "time_filters": [
                {"pattern": "range(start: -1h)", "description": "Recent 1 hour"},
                {"pattern": "range(start: -24h)", "description": "Recent 24 hours"},
                {"pattern": "range(start: -7d)", "description": "Recent 7 days"},
            ],
            "measurement_filters": [
                {"pattern": 'r._measurement == "patient_vitals"', "frequency": "high"}
            ],
            "field_filters": [
                {"pattern": 'r._field == "vital_value"', "frequency": "high"},
                {"pattern": 'r._field == "is_alert"', "frequency": "medium"},
            ],
            "tag_filters": [
                {"pattern": 'r.vital_type == "heart_rate_bpm"', "frequency": "high"},
                {"pattern": 'r.patient_department == "ICU"', "frequency": "medium"},
                {"pattern": "r.is_alert == true", "frequency": "low"},
            ],
        }

        return common_filters

    def analyze_aggregation_patterns(self, days: int) -> Dict:
        """Analyze common aggregation patterns"""

        # Analyze what aggregations are commonly used
        aggregation_patterns = {
            "window_aggregations": [
                {"function": "mean", "window": "1h", "frequency": "high"},
                {"function": "count", "window": "1d", "frequency": "medium"},
                {"function": "max", "window": "5m", "frequency": "low"},
                {"function": "min", "window": "5m", "frequency": "low"},
            ],
            "grouping_patterns": [
                {"columns": ["patient_id", "vital_type"], "frequency": "high"},
                {"columns": ["patient_department"], "frequency": "medium"},
                {"columns": ["device_id"], "frequency": "low"},
            ],
            "sorting_patterns": [
                {"columns": ["_time"], "direction": "desc", "frequency": "high"},
                {"columns": ["_value"], "direction": "desc", "frequency": "medium"},
            ],
        }

        return aggregation_patterns

    def _categorize_cardinality(self, cardinality: int) -> str:
        """Categorize tag cardinality"""
        if cardinality < 10:
            return "very_low"
        elif cardinality < 100:
            return "low"
        elif cardinality < 1000:
            return "medium"
        elif cardinality < 10000:
            return "high"
        else:
            return "very_high"

    def test_index_effectiveness(self) -> List[IndexAnalysisResult]:
        """Test effectiveness of different data organization strategies"""

        print("\nTesting data organization effectiveness...")

        test_queries = [
            {
                "name": "time_range_query",
                "type": "time_filter",
                "description": "Query with time filter only",
                "with_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> limit(n: 1000)
                """,
                "without_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -7d)  # Larger time range
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> limit(n: 1000)
                """,
            },
            {
                "name": "tag_filter_query",
                "type": "tag_filter",
                "description": "Query with tag filter",
                "with_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -24h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> filter(fn: (r) => r.vital_type == "heart_rate_bpm")
                      |> limit(n: 1000)
                """,
                "without_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -7d)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> limit(n: 1000)
                """,
            },
            {
                "name": "complex_filter_query",
                "type": "complex_filter",
                "description": "Query with multiple filters",
                "with_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -24h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> filter(fn: (r) => r.vital_type == "heart_rate_bpm")
                      |> filter(fn: (r) => r.patient_department == "ICU")
                      |> limit(n: 1000)
                """,
                "without_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -7d)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> limit(n: 1000)
                """,
            },
            {
                "name": "aggregation_query",
                "type": "aggregation",
                "description": "Query with aggregation",
                "with_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> filter(fn: (r) => r.vital_type == "heart_rate_bpm")
                      |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)
                """,
                "without_index": f"""
                    from(bucket: "{self.bucket}")
                      |> range(start: -24h)
                      |> filter(fn: (r) => r._measurement == "patient_vitals")
                      |> filter(fn: (r) => r._field == "vital_value")
                      |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)
                """,
            },
        ]

        results = []

        for test in test_queries:
            print(f"\nTesting: {test['name']}")
            print(f"Description: {test['description']}")

            # Test with optimized query (narrow filters)
            time_with, count_with, scan_with = self.execute_and_analyze_query(
                test["with_index"]
            )

            # Test without optimization (wide filters)
            time_without, count_without, scan_without = self.execute_and_analyze_query(
                test["without_index"]
            )

            # Calculate improvement
            if time_with > 0 and time_without > 0:
                improvement = time_without / time_with
            else:
                improvement = 1.0

            # Generate recommendation
            recommendation = self._generate_index_recommendation(
                test["type"], improvement, scan_with, scan_without
            )

            result = IndexAnalysisResult(
                query_name=test["name"],
                query_type=test["type"],
                execution_time_with_index=time_with,
                execution_time_without_index=time_without,
                performance_improvement=improvement,
                result_count=count_with,
                scan_type=scan_with.get("scan_type", "unknown"),
                series_scanned=scan_with.get("series_scanned", 0),
                bytes_scanned=scan_with.get("bytes_scanned", 0),
                recommendation=recommendation,
                query_text=test["with_index"],
            )

            results.append(result)

            print(f"  With optimization: {time_with:.3f}s, {count_with} results")
            print(
                f"  Without optimization: {time_without:.3f}s, {count_without} results"
            )
            print(f"  Performance improvement: {improvement:.1f}x")
            print(f"  Recommendation: {recommendation}")

        return results

    def execute_and_analyze_query(
        self, query: str, iterations: int = 3
    ) -> Tuple[float, int, Dict]:
        """Execute query and analyze performance"""

        execution_times = []
        result_count = 0
        scan_stats = {}

        for i in range(iterations):
            try:
                start_time = time.perf_counter()

                # Execute query
                tables = self.client.query_api().query(query, org=self.org)

                end_time = time.perf_counter()
                execution_time = end_time - start_time
                execution_times.append(execution_time)

                # Count results
                count = 0
                for table in tables:
                    count += len(table.records)

                if i == 0:  # Only count once
                    result_count = count

                # Get scan statistics from query explanation
                # Note: InfluxDB doesn't provide detailed execution plans like SQL databases
                # We'll estimate based on time range and filters
                scan_stats = self._estimate_scan_stats(query)

                # Small delay between iterations
                if i < iterations - 1:
                    time.sleep(1)

            except Exception as e:
                print(f"    Error executing query (iteration {i+1}): {e}")
                execution_times.append(0)

        # Calculate average time
        avg_time = statistics.mean(execution_times) if execution_times else 0

        return avg_time, result_count, scan_stats

    def _estimate_scan_stats(self, query: str) -> Dict:
        """Estimate scan statistics based on query"""

        # This is a simplified estimation
        # In production, you'd want to parse the query more thoroughly

        stats = {"scan_type": "series_scan", "series_scanned": 0, "bytes_scanned": 0}

        # Estimate based on query characteristics
        if "range(start: -1h)" in query:
            stats["time_range_hours"] = 1
            stats["series_scanned"] = 100  # Estimated
            stats["bytes_scanned"] = 1024 * 100  # 100KB estimated
        elif "range(start: -24h)" in query:
            stats["time_range_hours"] = 24
            stats["series_scanned"] = 1000  # Estimated
            stats["bytes_scanned"] = 1024 * 1000  # 1MB estimated
        elif "range(start: -7d)" in query:
            stats["time_range_days"] = 7
            stats["series_scanned"] = 10000  # Estimated
            stats["bytes_scanned"] = 1024 * 10000  # 10MB estimated

        # Check for specific filters
        if 'r.vital_type == "heart_rate_bpm"' in query:
            stats["filter_selectivity"] = "high"
            stats["series_scanned"] = int(
                stats.get("series_scanned", 0) * 0.2
            )  # 20% selectivity

        if 'r.patient_department == "ICU"' in query:
            stats["filter_selectivity"] = "medium"
            stats["series_scanned"] = int(
                stats.get("series_scanned", 0) * 0.3
            )  # 30% selectivity

        return stats

    def _generate_index_recommendation(
        self, query_type: str, improvement: float, scan_with: Dict, scan_without: Dict
    ) -> str:
        """Generate indexing recommendation based on analysis"""

        if improvement > 5.0:
            return f"Excellent optimization ({improvement:.1f}x improvement). Keep current data organization."
        elif improvement > 2.0:
            return f"Good optimization ({improvement:.1f}x improvement). Consider adding more specific filters."
        elif improvement > 1.0:
            return f"Minor optimization ({improvement:.1f}x improvement). Review query patterns."
        else:
            return "No significant improvement. Consider restructuring data or queries."

    def generate_optimization_recommendations(
        self, analysis_results: Dict, test_results: List[IndexAnalysisResult]
    ) -> List[Dict]:
        """Generate comprehensive optimization recommendations"""

        recommendations = []

        # 1. Tag-based recommendations
        tag_analysis = analysis_results.get("tag_usage_analysis", {})

        for tag, stats in tag_analysis.items():
            if isinstance(stats, dict) and "cardinality_category" in stats:
                category = stats["cardinality_category"]

                if category == "very_high":
                    recommendations.append(
                        {
                            "type": "tag_optimization",
                            "priority": "high",
                            "title": f"High Cardinality Tag: {tag}",
                            "description": f"Tag '{tag}' has very high cardinality ({stats.get('cardinality', 0)} unique values)",
                            "recommendation": "Consider moving to fields if not used for filtering, or implement tag value limits.",
                            "impact": "High memory usage, slower queries",
                        }
                    )
                elif category == "very_low":
                    recommendations.append(
                        {
                            "type": "tag_optimization",
                            "priority": "low",
                            "title": f"Low Cardinality Tag: {tag}",
                            "description": f"Tag '{tag}' has very low cardinality ({stats.get('cardinality', 0)} unique values)",
                            "recommendation": "Consider if this tag is necessary for querying. Could be removed or combined.",
                            "impact": "Minor optimization opportunity",
                        }
                    )

        # 2. Time-based recommendations
        time_analysis = analysis_results.get("time_range_analysis", {})
        if time_analysis.get("trend") == "variable":
            recommendations.append(
                {
                    "type": "time_optimization",
                    "priority": "medium",
                    "title": "Variable Data Volume",
                    "description": "Data volume varies significantly day-to-day",
                    "recommendation": "Consider implementing data downsampling for older data to improve query performance.",
                    "impact": "Inconsistent query performance",
                }
            )

        # 3. Query performance recommendations
        for test_result in test_results:
            if test_result.performance_improvement < 1.5:
                recommendations.append(
                    {
                        "type": "query_optimization",
                        "priority": "medium",
                        "title": f"Query Optimization Needed: {test_result.query_name}",
                        "description": f"Query shows limited performance improvement ({test_result.performance_improvement:.1f}x)",
                        "recommendation": test_result.recommendation,
                        "impact": "Suboptimal query performance",
                    }
                )

        # 4. Field usage recommendations
        field_analysis = analysis_results.get("field_usage_analysis", {})
        fields = field_analysis.get("fields", [])

        if len(fields) > 10:
            recommendations.append(
                {
                    "type": "schema_optimization",
                    "priority": "medium",
                    "title": "High Number of Fields",
                    "description": f"Measurement has {len(fields)} different fields",
                    "recommendation": "Consider splitting into multiple measurements or using a more structured schema.",
                    "impact": "Complex schema, potentially slower writes",
                }
            )

        # 5. Retention policy recommendations
        total_points = time_analysis.get("total_points", 0)
        if total_points > 1000000:  # 1 million points
            recommendations.append(
                {
                    "type": "retention_optimization",
                    "priority": "high",
                    "title": "Large Data Volume",
                    "description": f"Database contains {total_points:,} data points",
                    "recommendation": "Implement tiered retention policies and data downsampling.",
                    "impact": "Storage costs, backup times, query performance",
                }
            )

        return recommendations

    def create_optimization_plan(self, recommendations: List[Dict]) -> Dict:
        """Create an actionable optimization plan"""

        # Group by priority
        high_priority = [r for r in recommendations if r["priority"] == "high"]
        medium_priority = [r for r in recommendations if r["priority"] == "medium"]
        low_priority = [r for r in recommendations if r["priority"] == "low"]

        # Estimate effort
        effort_estimation = {
            "tag_optimization": {"effort": "medium", "risk": "low"},
            "time_optimization": {"effort": "high", "risk": "medium"},
            "query_optimization": {"effort": "low", "risk": "low"},
            "schema_optimization": {"effort": "high", "risk": "high"},
            "retention_optimization": {"effort": "medium", "risk": "medium"},
        }

        plan = {
            "summary": {
                "total_recommendations": len(recommendations),
                "high_priority": len(high_priority),
                "medium_priority": len(medium_priority),
                "low_priority": len(low_priority),
                "generated_date": datetime.now().isoformat(),
            },
            "high_priority_actions": [],
            "medium_priority_actions": [],
            "low_priority_actions": [],
            "implementation_roadmap": {
                "immediate": [],
                "short_term": [],
                "long_term": [],
            },
        }

        # Categorize actions
        for rec in high_priority:
            effort = effort_estimation.get(
                rec["type"], {"effort": "unknown", "risk": "unknown"}
            )
            plan["high_priority_actions"].append(
                {
                    **rec,
                    "estimated_effort": effort["effort"],
                    "risk": effort["risk"],
                    "timeline": "immediate",
                }
            )
            plan["implementation_roadmap"]["immediate"].append(rec["title"])

        for rec in medium_priority:
            effort = effort_estimation.get(
                rec["type"], {"effort": "unknown", "risk": "unknown"}
            )
            plan["medium_priority_actions"].append(
                {
                    **rec,
                    "estimated_effort": effort["effort"],
                    "risk": effort["risk"],
                    "timeline": "short_term",
                }
            )
            plan["implementation_roadmap"]["short_term"].append(rec["title"])

        for rec in low_priority:
            effort = effort_estimation.get(
                rec["type"], {"effort": "unknown", "risk": "unknown"}
            )
            plan["low_priority_actions"].append(
                {
                    **rec,
                    "estimated_effort": effort["effort"],
                    "risk": effort["risk"],
                    "timeline": "long_term",
                }
            )
            plan["implementation_roadmap"]["long_term"].append(rec["title"])

        return plan

    def save_analysis_report(
        self,
        analysis_results: Dict,
        test_results: List[IndexAnalysisResult],
        recommendations: List[Dict],
        optimization_plan: Dict,
        output_dir: str = "./results",
    ):
        """Save comprehensive analysis report"""

        import os

        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Prepare report data
        report_data = {
            "metadata": {
                "timestamp": timestamp,
                "bucket": self.bucket,
                "org": self.org,
                "analysis_type": "indexing_performance",
            },
            "query_pattern_analysis": analysis_results,
            "performance_test_results": [
                {
                    "query_name": r.query_name,
                    "query_type": r.query_type,
                    "execution_time_with_index": r.execution_time_with_index,
                    "execution_time_without_index": r.execution_time_without_index,
                    "performance_improvement": r.performance_improvement,
                    "result_count": r.result_count,
                    "scan_type": r.scan_type,
                    "series_scanned": r.series_scanned,
                    "bytes_scanned": r.bytes_scanned,
                    "recommendation": r.recommendation,
                }
                for r in test_results
            ],
            "optimization_recommendations": recommendations,
            "optimization_plan": optimization_plan,
        }

        # Save JSON report
        json_file = os.path.join(output_dir, f"indexing_analysis_{timestamp}.json")
        with open(json_file, "w") as f:
            json.dump(report_data, f, indent=2, default=str)

        # Generate text report
        text_report = self.generate_text_report(report_data)
        text_file = os.path.join(output_dir, f"indexing_analysis_{timestamp}.txt")
        with open(text_file, "w") as f:
            f.write(text_report)

        print("\nAnalysis report saved:")
        print(f"  JSON: {json_file}")
        print(f"  Text: {text_file}")

        return json_file, text_file

    def generate_text_report(self, report_data: Dict) -> str:
        """Generate human-readable text report"""

        report = []
        report.append("=" * 80)
        report.append("INFLUXDB INDEXING AND PERFORMANCE ANALYSIS REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {report_data['metadata']['timestamp']}")
        report.append(f"Bucket: {report_data['metadata']['bucket']}")
        report.append(f"Organization: {report_data['metadata']['org']}")
        report.append("")

        # Summary
        report.append("EXECUTIVE SUMMARY")
        report.append("-" * 40)

        test_results = report_data.get("performance_test_results", [])
        if test_results:
            avg_improvement = statistics.mean(
                [r["performance_improvement"] for r in test_results]
            )
            best_improvement = max([r["performance_improvement"] for r in test_results])
            worst_improvement = min(
                [r["performance_improvement"] for r in test_results]
            )

            report.append(f"Average Performance Improvement: {avg_improvement:.1f}x")
            report.append(f"Best Improvement: {best_improvement:.1f}x")
            report.append(f"Worst Improvement: {worst_improvement:.1f}x")
            report.append("")

        # Performance Test Results
        report.append("PERFORMANCE TEST RESULTS")
        report.append("-" * 40)

        for result in test_results:
            report.append(f"\nQuery: {result['query_name']} ({result['query_type']})")
            report.append(
                f"  With optimization: {result['execution_time_with_index']:.3f}s"
            )
            report.append(
                f"  Without optimization: {result['execution_time_without_index']:.3f}s"
            )
            report.append(f"  Improvement: {result['performance_improvement']:.1f}x")
            report.append(f"  Results returned: {result['result_count']}")
            report.append(f"  Recommendation: {result['recommendation']}")

        # Optimization Recommendations
        recommendations = report_data.get("optimization_recommendations", [])
        if recommendations:
            report.append("\n" + "=" * 80)
            report.append("OPTIMIZATION RECOMMENDATIONS")
            report.append("-" * 40)

            high_priority = [r for r in recommendations if r["priority"] == "high"]
            medium_priority = [r for r in recommendations if r["priority"] == "medium"]
            low_priority = [r for r in recommendations if r["priority"] == "low"]

            if high_priority:
                report.append("\nHIGH PRIORITY:")
                for i, rec in enumerate(high_priority, 1):
                    report.append(f"\n{i}. {rec['title']}")
                    report.append(f"   Description: {rec['description']}")
                    report.append(f"   Recommendation: {rec['recommendation']}")
                    report.append(f"   Impact: {rec['impact']}")

            if medium_priority:
                report.append("\nMEDIUM PRIORITY:")
                for i, rec in enumerate(medium_priority, 1):
                    report.append(f"\n{i}. {rec['title']}")
                    report.append(f"   Description: {rec['description']}")
                    report.append(f"   Recommendation: {rec['recommendation']}")
                    report.append(f"   Impact: {rec['impact']}")

            if low_priority:
                report.append("\nLOW PRIORITY:")
                for i, rec in enumerate(low_priority, 1):
                    report.append(f"\n{i}. {rec['title']}")
                    report.append(f"   Description: {rec['description']}")
                    report.append(f"   Recommendation: {rec['recommendation']}")
                    report.append(f"   Impact: {rec['impact']}")

        # Optimization Plan
        optimization_plan = report_data.get("optimization_plan", {})
        if optimization_plan:
            report.append("\n" + "=" * 80)
            report.append("OPTIMIZATION IMPLEMENTATION PLAN")
            report.append("-" * 40)

            summary = optimization_plan.get("summary", {})
            report.append("\nSummary:")
            report.append(
                f"  Total recommendations: {summary.get('total_recommendations', 0)}"
            )
            report.append(f"  High priority: {summary.get('high_priority', 0)}")
            report.append(f"  Medium priority: {summary.get('medium_priority', 0)}")
            report.append(f"  Low priority: {summary.get('low_priority', 0)}")

            roadmap = optimization_plan.get("implementation_roadmap", {})
            if roadmap.get("immediate"):
                report.append("\nImmediate Actions (Next 2 weeks):")
                for action in roadmap["immediate"]:
                    report.append(f"  • {action}")

            if roadmap.get("short_term"):
                report.append("\nShort Term Actions (Next 1-3 months):")
                for action in roadmap["short_term"]:
                    report.append(f"  • {action}")

            if roadmap.get("long_term"):
                report.append("\nLong Term Actions (3+ months):")
                for action in roadmap["long_term"]:
                    report.append(f"  • {action}")

        # Tag Analysis Details
        tag_analysis = report_data.get("query_pattern_analysis", {}).get(
            "tag_usage_analysis", {}
        )
        if tag_analysis:
            report.append("\n" + "=" * 80)
            report.append("TAG ANALYSIS DETAILS")
            report.append("-" * 40)

            for tag, stats in tag_analysis.items():
                if isinstance(stats, dict):
                    report.append(f"\n{tag}:")
                    report.append(f"  Cardinality: {stats.get('cardinality', 'N/A')}")
                    report.append(
                        f"  Category: {stats.get('cardinality_category', 'N/A')}"
                    )

                    top_values = stats.get("top_values", [])[:3]
                    if top_values:
                        report.append("  Top values:")
                        for val in top_values:
                            report.append(
                                f"    • {val.get('value')}: {val.get('count')}"
                            )

        report.append("\n" + "=" * 80)
        report.append("END OF REPORT")
        report.append("=" * 80)

        return "\n".join(report)

    def run_comprehensive_analysis(self, output_dir: str = "./results"):
        """Run comprehensive indexing and performance analysis"""

        print("=" * 80)
        print("INFLUXDB INDEXING AND PERFORMANCE ANALYSIS")
        print("=" * 80)

        # Step 1: Analyze query patterns
        print("\nStep 1: Analyzing query patterns...")
        analysis_results = self.analyze_query_patterns(days=7)

        # Step 2: Test index effectiveness
        print("\nStep 2: Testing data organization effectiveness...")
        test_results = self.test_index_effectiveness()

        # Step 3: Generate recommendations
        print("\nStep 3: Generating optimization recommendations...")
        recommendations = self.generate_optimization_recommendations(
            analysis_results, test_results
        )

        # Step 4: Create optimization plan
        print("\nStep 4: Creating implementation plan...")
        optimization_plan = self.create_optimization_plan(recommendations)

        # Step 5: Save report
        print("\nStep 5: Generating comprehensive report...")
        json_file, text_file = self.save_analysis_report(
            analysis_results,
            test_results,
            recommendations,
            optimization_plan,
            output_dir,
        )

        # Print summary
        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)

        summary = optimization_plan.get("summary", {})
        print("\nSummary:")
        print(f"  Total recommendations: {summary.get('total_recommendations', 0)}")
        print(f"  High priority actions: {summary.get('high_priority', 0)}")
        print(f"  Medium priority actions: {summary.get('medium_priority', 0)}")
        print(f"  Low priority actions: {summary.get('low_priority', 0)}")

        if test_results:
            avg_improvement = statistics.mean(
                [r.performance_improvement for r in test_results]
            )
            print("\nPerformance Analysis:")
            print(f"  Average improvement: {avg_improvement:.1f}x")

        print("\nReports saved to:")
        print(f"  {json_file}")
        print(f"  {text_file}")

        return {
            "analysis_results": analysis_results,
            "test_results": test_results,
            "recommendations": recommendations,
            "optimization_plan": optimization_plan,
        }


def main():
    parser = argparse.ArgumentParser(
        description="InfluxDB Indexing and Performance Analysis Tool"
    )

    # Connection parameters
    parser.add_argument("--url", default="http://localhost:8086", help="InfluxDB URL")
    parser.add_argument("--token", required=True, help="InfluxDB authentication token")
    parser.add_argument("--org", default="HealthIoT", help="Organization name")
    parser.add_argument("--bucket", default="health_iot_metrics", help="Bucket name")

    # Analysis parameters
    parser.add_argument(
        "--analysis-days", type=int, default=7, help="Number of days to analyze"
    )
    parser.add_argument(
        "--output-dir", default="./results", help="Output directory for results"
    )
    parser.add_argument("--single-test", help="Run single query test (provide query)")

    args = parser.parse_args()

    # Initialize analyzer
    analyzer = InfluxDBIndexAnalyzer(
        url=args.url, token=args.token, org=args.org, bucket=args.bucket
    )

    try:
        if args.single_test:
            # Run single query test
            print("Running single query test...")
            time_taken, result_count, scan_stats = analyzer.execute_and_analyze_query(
                args.single_test
            )

            print("\nResults:")
            print(f"  Execution time: {time_taken:.3f} seconds")
            print(f"  Results returned: {result_count}")
            print(f"  Scan type: {scan_stats.get('scan_type', 'unknown')}")
            print(f"  Series scanned: {scan_stats.get('series_scanned', 0)}")
            print(f"  Bytes scanned: {scan_stats.get('bytes_scanned', 0)}")

        else:
            # Run comprehensive analysis
            analyzer.run_comprehensive_analysis(output_dir=args.output_dir)

    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
    except Exception as e:
        print(f"\nError during analysis: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
