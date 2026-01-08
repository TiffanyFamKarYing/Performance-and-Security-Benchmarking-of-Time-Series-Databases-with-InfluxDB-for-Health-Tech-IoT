#!/usr/bin/env python3
"""
Final Tables Generator for Health IoT Database Benchmarking
Generates comprehensive final comparison tables and reports
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import numpy as np
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


class FinalTablesGenerator:
    def __init__(self, base_dir: str = None, run_id: str = None):
        """Initialize final tables generator"""
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent
        self.run_id = run_id or datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.output_dir = self.base_dir / "outputs" / "final_results"

        # Create directories
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Load results from previous steps
        self.results = self._load_results()

        # Initialize tables
        self.tables = {}

    def _load_results(self) -> Dict[str, Any]:
        """Load results from previous analysis steps"""
        print("Loading benchmark results...")

        results = {
            "performance": {},
            "security": {},
            "storage": {},
            "compliance": {},
            "metadata": {
                "run_id": self.run_id,
                "timestamp": datetime.now().isoformat(),
            },
        }

        # Load results from different sources
        result_sources = [
            ("performance", "comparison", "comparison"),
            ("security", "security_analysis", "security_analysis"),
            ("postgresql", "postgresql", "postgresql_results"),
            ("influxdb", "influxdb", "influxdb_results"),
            ("mongodb", "mongodb_indexing", "mongodb_results"),
        ]

        for category, subdir, key in result_sources:
            json_file = self.base_dir / "outputs" / subdir / f"{key}_{self.run_id}.json"
            if json_file.exists():
                try:
                    with open(json_file, "r") as f:
                        results[category] = json.load(f)
                    print(f"  Loaded: {category}")
                except Exception as e:
                    print(f"  Error loading {category}: {e}")

        return results

    def generate_performance_summary_table(self):
        """Generate performance summary table"""
        logger.info("Generating performance summary table...")

        # Extract performance metrics from results
        performance_data = []

        # PostgreSQL performance
        if "postgresql" in self.results and self.results.get("postgresql"):
            pg_perf = self._extract_postgresql_performance()
            performance_data.append({"Database": "PostgreSQL", **pg_perf})

        # InfluxDB performance
        if "influxdb" in self.results and self.results.get("influxdb"):
            influx_perf = self._extract_influxdb_performance()
            performance_data.append({"Database": "InfluxDB", **influx_perf})

        # MongoDB performance
        if "mongodb" in self.results and self.results.get("mongodb"):
            mongo_perf = self._extract_mongodb_performance()
            performance_data.append({"Database": "MongoDB", **mongo_perf})

        # Create DataFrame
        df = pd.DataFrame(performance_data)

        # If no performance data found, create placeholder rows and insert a note
        if df.empty:
            logger.warning("No performance results found; creating placeholder table.")
            df = pd.DataFrame(
                [
                    {"Database": "PostgreSQL", "Performance_Score": "N/A"},
                    {"Database": "InfluxDB", "Performance_Score": "N/A"},
                    {"Database": "MongoDB", "Performance_Score": "N/A"},
                ]
            )
        else:
            # Calculate normalized scores (0-100)
            df = self._normalize_performance_scores(df)

        # Save to CSV
        csv_file = self.output_dir / f"performance_summary_{self.run_id}.csv"
        df.to_csv(csv_file, index=False)

        # Save as markdown table (include a note if placeholders are present)
        md_file = self.output_dir / f"performance_summary_{self.run_id}.md"
        if "Performance_Score" in df.columns and df["Performance_Score"].dtype == object:
            note_df = df.copy()
            note_df.loc[:, "Note"] = "No performance results found for this run."
            self._save_as_markdown(note_df, md_file, "Performance Summary")
        else:
            self._save_as_markdown(df, md_file, "Performance Summary")

        self.tables["performance_summary"] = df
        return df

    def _extract_postgresql_performance(self) -> Dict[str, Any]:
        """Extract PostgreSQL performance metrics"""
        metrics = {
            "Ingestion_Rate": 0,
            "Query_Latency_Avg": 0,
            "Query_Latency_P95": 0,
            "Storage_Efficiency": 0,
            "Index_Improvement": 0,
            "Security_Overhead": 0,
        }

        pg_data = self.results.get("postgresql", {})

        # Extract from actual results structure
        # These would come from your actual benchmark results
        # For now, using placeholder logic

        if "ingestion_performance" in pg_data:
            metrics["Ingestion_Rate"] = pg_data["ingestion_performance"].get(
                "avg_insert_rate", 0
            )

        if "query_performance" in pg_data:
            query_data = pg_data["query_performance"]
            if isinstance(query_data, list) and len(query_data) > 0:
                metrics["Query_Latency_Avg"] = np.mean(
                    [q.get("execution_time_ms", 0) for q in query_data]
                )

        return metrics

    def _extract_influxdb_performance(self) -> Dict[str, Any]:
        """Extract InfluxDB performance metrics"""
        metrics = {
            "Ingestion_Rate": 0,
            "Query_Latency_Avg": 0,
            "Query_Latency_P95": 0,
            "Storage_Efficiency": 0,
            "Index_Improvement": 0,
            "Security_Overhead": 0,
        }

        # Placeholder extraction logic
        # Replace with actual extraction from your results

        return metrics

    def _extract_mongodb_performance(self) -> Dict[str, Any]:
        """Extract MongoDB performance metrics"""
        metrics = {
            "Ingestion_Rate": 0,
            "Query_Latency_Avg": 0,
            "Query_Latency_P95": 0,
            "Storage_Efficiency": 0,
            "Index_Improvement": 0,
            "Security_Overhead": 0,
        }

        # Placeholder extraction logic
        # Replace with actual extraction from your results

        return metrics

    def _normalize_performance_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize performance scores to 0-100 scale"""

        # Columns to normalize (higher is better)
        higher_better = ["Ingestion_Rate", "Storage_Efficiency", "Index_Improvement"]

        for col in higher_better:
            if col in df.columns:
                max_val = df[col].max()
                if max_val > 0:
                    df[f"{col}_Score"] = (df[col] / max_val) * 100
                else:
                    df[f"{col}_Score"] = 0

        # Columns to normalize (lower is better)
        lower_better = ["Query_Latency_Avg", "Query_Latency_P95", "Security_Overhead"]

        for col in lower_better:
            if col in df.columns:
                max_val = df[col].max()
                if max_val > 0:
                    df[f"{col}_Score"] = (1 - (df[col] / max_val)) * 100
                else:
                    df[f"{col}_Score"] = 100

        # Calculate total performance score (weighted)
        weights = {
            "Ingestion_Rate_Score": 0.25,
            "Query_Latency_Avg_Score": 0.20,
            "Query_Latency_P95_Score": 0.15,
            "Storage_Efficiency_Score": 0.20,
            "Index_Improvement_Score": 0.15,
            "Security_Overhead_Score": 0.05,
        }

        df["Performance_Score"] = 0
        for col, weight in weights.items():
            if col in df.columns:
                df["Performance_Score"] += df[col] * weight

        df["Performance_Score"] = df["Performance_Score"].round(2)

        return df

    def generate_security_comparison_table(self):
        """Generate security comparison table"""
        print("\nGenerating security comparison table...")

        # Load security analysis results
        security_data = self.results.get("security", {})

        if not security_data:
            logger.warning("No security analysis data found")
            return None

        security_table = []

        for db, assessment in security_data.get("database_assessments", {}).items():
            db_name = db.capitalize()

            # Extract security features count
            features = assessment.get("security_features", [])
            feature_count = len(features)

            # Extract compliance scores
            compliance = assessment.get("compliance_status", {})
            compliance_scores = []
            for framework, status in compliance.items():
                compliance_scores.append(status.get("score", 0))

            avg_compliance = np.mean(compliance_scores) if compliance_scores else 0

            # Extract vulnerability count
            vulnerabilities = security_data.get("vulnerability_analysis", {}).get(
                db, []
            )
            vuln_count = len(
                [v for v in vulnerabilities if "id" in v]
            )  # Exclude summary entries

            # Get overall security score
            overall_score = (
                security_data.get("compliance_scores", {})
                .get(db, {})
                .get("overall_score", 0)
            )

            security_table.append(
                {
                    "Database": db_name,
                    "Security_Features": feature_count,
                    "Avg_Compliance_Score": round(avg_compliance, 2),
                    "Vulnerabilities_Found": vuln_count,
                    "Overall_Security_Score": overall_score,
                    "Security_Grade": security_data.get("compliance_scores", {})
                    .get(db, {})
                    .get("security_grade", "N/A"),
                }
            )

        df = pd.DataFrame(security_table)

        # Save to CSV
        csv_file = self.output_dir / f"security_comparison_{self.run_id}.csv"
        df.to_csv(csv_file, index=False)

        # Save as markdown table
        md_file = self.output_dir / f"security_comparison_{self.run_id}.md"
        self._save_as_markdown(df, md_file, "Security Comparison")

        self.tables["security_comparison"] = df
        return df

    def generate_storage_efficiency_table(self):
        """Generate storage efficiency comparison table"""
        print("\nGenerating storage efficiency table...")

        storage_data = []

        # Extract storage metrics from each database's results
        databases = ["postgresql", "influxdb", "mongodb"]

        for db in databases:
            if db in self.results:
                storage_metrics = self._extract_storage_metrics(db)
                storage_data.append({"Database": db.capitalize(), **storage_metrics})

        if not storage_data:
            logger.warning("No storage data found")
            return None

        df = pd.DataFrame(storage_data)

        # Calculate efficiency scores
        if "Data_Size_MB" in df.columns and "Index_Size_MB" in df.columns:
            df["Total_Size_MB"] = df["Data_Size_MB"] + df["Index_Size_MB"]
            df["Compression_Ratio"] = (
                df["Raw_Data_Size_MB"] / df["Data_Size_MB"]
                if "Raw_Data_Size_MB" in df.columns
                else 1
            )

        # Save to CSV
        csv_file = self.output_dir / f"storage_efficiency_{self.run_id}.csv"
        df.to_csv(csv_file, index=False)

        # Save as markdown table
        md_file = self.output_dir / f"storage_efficiency_{self.run_id}.md"
        self._save_as_markdown(df, md_file, "Storage Efficiency")

        self.tables["storage_efficiency"] = df
        return df

    def _extract_storage_metrics(self, database: str) -> Dict[str, Any]:
        """Extract storage metrics for a specific database"""
        metrics = {
            "Data_Size_MB": 0,
            "Index_Size_MB": 0,
            "Total_Size_MB": 0,
            "Compression_Ratio": 1.0,
            "Records_Count": 0,
        }

        db_data = self.results.get(database, {})

        # Database-specific extraction logic
        if database == "postgresql":
            # Extract from PostgreSQL storage results
            if "storage_efficiency" in db_data:
                storage_info = db_data["storage_efficiency"]
                metrics["Data_Size_MB"] = storage_info.get("table_size_mb", 0)
                metrics["Index_Size_MB"] = storage_info.get("index_size_mb", 0)
                metrics["Total_Size_MB"] = storage_info.get("total_size_mb", 0)

        elif database == "influxdb":
            # Extract from InfluxDB storage results
            if "storage_metrics" in db_data:
                storage_info = db_data["storage_metrics"]
                metrics["Data_Size_MB"] = storage_info.get("disk_usage_mb", 0)
                metrics["Compression_Ratio"] = storage_info.get(
                    "compression_ratio", 1.0
                )

        elif database == "mongodb":
            # Extract from MongoDB storage results
            if "storage_stats" in db_data:
                storage_info = db_data["storage_stats"]
                metrics["Data_Size_MB"] = storage_info.get("size_mb", 0)
                metrics["Index_Size_MB"] = storage_info.get("index_size_mb", 0)
                metrics["Total_Size_MB"] = storage_info.get("total_size_mb", 0)

        return metrics

    def generate_cost_analysis_table(self):
        """Generate cost analysis table"""
        print("\nGenerating cost analysis table...")

        # Cost assumptions (per hour for cloud instances)
        cost_assumptions = {
            "PostgreSQL": {
                "instance_type": "db.m5.large",
                "hourly_cost": 0.171,
                "storage_cost_per_gb": 0.115,
                "io_cost_per_million": 0.20,
            },
            "InfluxDB": {
                "instance_type": "db.r5.large",
                "hourly_cost": 0.181,
                "storage_cost_per_gb": 0.125,
                "io_cost_per_million": 0.25,
            },
            "MongoDB": {
                "instance_type": "db.m5.large",
                "hourly_cost": 0.191,
                "storage_cost_per_gb": 0.135,
                "io_cost_per_million": 0.22,
            },
        }

        cost_data = []

        for db_name, costs in cost_assumptions.items():
            # Get storage size from previous tables
            storage_size_gb = 0
            if "storage_efficiency" in self.tables:
                storage_df = self.tables["storage_efficiency"]
                db_row = storage_df[storage_df["Database"] == db_name]
                if not db_row.empty:
                    storage_size_gb = db_row.iloc[0].get("Total_Size_MB", 0) / 1024

            # Get performance metrics
            monthly_operations = 10000000  # Example: 10 million operations/month

            # Calculate costs
            compute_cost_monthly = costs["hourly_cost"] * 24 * 30
            storage_cost_monthly = storage_size_gb * costs["storage_cost_per_gb"] * 30
            io_cost_monthly = (monthly_operations / 1000000) * costs[
                "io_cost_per_million"
            ]

            total_monthly_cost = (
                compute_cost_monthly + storage_cost_monthly + io_cost_monthly
            )

            cost_data.append(
                {
                    "Database": db_name,
                    "Instance_Type": costs["instance_type"],
                    "Compute_Cost_Monthly": round(compute_cost_monthly, 2),
                    "Storage_Cost_Monthly": round(storage_cost_monthly, 2),
                    "IO_Cost_Monthly": round(io_cost_monthly, 2),
                    "Total_Cost_Monthly": round(total_monthly_cost, 2),
                    "Cost_Per_Million_Ops": round(
                        (total_monthly_cost / monthly_operations) * 1000000, 2
                    ),
                }
            )

        df = pd.DataFrame(cost_data)

        # Save to CSV
        csv_file = self.output_dir / f"cost_analysis_{self.run_id}.csv"
        df.to_csv(csv_file, index=False)

        # Save as markdown table
        md_file = self.output_dir / f"cost_analysis_{self.run_id}.md"
        self._save_as_markdown(df, md_file, "Cost Analysis")

        self.tables["cost_analysis"] = df
        return df

    def generate_final_ranking_table(self):
        """Generate final ranking table with weighted scores"""
        logger.info("Generating final ranking table...")

        # Collect scores from all tables
        ranking_data = []

        databases = ["PostgreSQL", "InfluxDB", "MongoDB"]

        for db in databases:
            db_data = {"Database": db}

            # Performance Score
            perf_df = self.tables.get("performance_summary")
            if (
                isinstance(perf_df, pd.DataFrame)
                and not perf_df.empty
                and "Database" in perf_df.columns
            ):
                db_row = perf_df[perf_df["Database"] == db]
                if not db_row.empty:
                    db_data["Performance_Score"] = db_row.iloc[0].get(
                        "Performance_Score", 0
                    )
                else:
                    db_data["Performance_Score"] = 0
            else:
                db_data["Performance_Score"] = 0

            # Security Score
            sec_df = self.tables.get("security_comparison")
            if (
                isinstance(sec_df, pd.DataFrame)
                and not sec_df.empty
                and "Database" in sec_df.columns
            ):
                db_row = sec_df[sec_df["Database"] == db]
                if not db_row.empty:
                    db_data["Security_Score"] = db_row.iloc[0].get(
                        "Overall_Security_Score", 0
                    )
                else:
                    db_data["Security_Score"] = 0
            else:
                db_data["Security_Score"] = 0

            # Storage Efficiency (inverse of size)
            storage_df = self.tables.get("storage_efficiency")
            if (
                isinstance(storage_df, pd.DataFrame)
                and not storage_df.empty
                and "Database" in storage_df.columns
                and "Total_Size_MB" in storage_df.columns
            ):
                db_row = storage_df[storage_df["Database"] == db]
                if not db_row.empty:
                    size_mb = db_row.iloc[0].get("Total_Size_MB", 1)
                    max_size = storage_df["Total_Size_MB"].max()
                    db_data["Storage_Score"] = (
                        (1 - (size_mb / max_size)) * 100 if max_size > 0 else 0
                    )
                else:
                    db_data["Storage_Score"] = 0
            else:
                db_data["Storage_Score"] = 0

            # Cost Efficiency (inverse of cost)
            cost_df = self.tables.get("cost_analysis")
            if (
                isinstance(cost_df, pd.DataFrame)
                and not cost_df.empty
                and "Database" in cost_df.columns
                and "Total_Cost_Monthly" in cost_df.columns
            ):
                db_row = cost_df[cost_df["Database"] == db]
                if not db_row.empty:
                    cost = db_row.iloc[0].get("Total_Cost_Monthly", 1)
                    max_cost = cost_df["Total_Cost_Monthly"].max()
                    db_data["Cost_Score"] = (
                        (1 - (cost / max_cost)) * 100 if max_cost > 0 else 0
                    )
                else:
                    db_data["Cost_Score"] = 0
            else:
                db_data["Cost_Score"] = 0

            ranking_data.append(db_data)

        df = pd.DataFrame(ranking_data)

        # Fill missing scores with 0
        score_columns = [
            "Performance_Score",
            "Security_Score",
            "Storage_Score",
            "Cost_Score",
        ]
        for col in score_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)
            else:
                df[col] = 0

        # Calculate weighted total score
        weights = {
            "Performance_Score": 0.35,
            "Security_Score": 0.30,
            "Storage_Score": 0.20,
            "Cost_Score": 0.15,
        }

        df["Total_Score"] = 0
        for col, weight in weights.items():
            if col in df.columns:
                df["Total_Score"] += df[col] * weight

        df["Total_Score"] = df["Total_Score"].round(2)

        # Add ranking
        df["Rank"] = df["Total_Score"].rank(ascending=False).astype(int)

        # Add recommendation
        df["Recommendation"] = df.apply(self._get_recommendation, axis=1)

        # Sort by rank
        df = df.sort_values("Rank")

        # Save to CSV
        csv_file = self.output_dir / f"final_ranking_{self.run_id}.csv"
        df.to_csv(csv_file, index=False)

        # Save as markdown table
        md_file = self.output_dir / f"final_ranking_{self.run_id}.md"
        self._save_as_markdown(df, md_file, "Final Ranking")

        self.tables["final_ranking"] = df
        return df

    def _get_recommendation(self, row):
        """Get recommendation based on scores"""
        total_score = row["Total_Score"]

        if total_score >= 80:
            return "Strongly Recommended"
        elif total_score >= 70:
            return "Recommended"
        elif total_score >= 60:
            return "Consider with Modifications"
        else:
            return "Not Recommended for Production"

    def generate_comprehensive_report(self):
        """Generate comprehensive final report"""
        print("\nGenerating comprehensive report...")

        report_file = self.output_dir / f"comprehensive_report_{self.run_id}.md"

        with open(report_file, "w", encoding="utf-8") as f:
            f.write("# Health IoT Database Benchmarking - Final Report\n\n")

            f.write(f"**Run ID:** {self.run_id}\n")
            f.write(
                f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            )

            f.write("## Executive Summary\n\n")

            # Get winner from final ranking
            if "final_ranking" in self.tables:
                winner = self.tables["final_ranking"].iloc[0]
                f.write(f"**🏆 Recommended Database:** {winner['Database']}\n\n")
                f.write(f"**Overall Score:** {winner['Total_Score']}/100\n")
                f.write(f"**Rank:** #{winner['Rank']}\n")
                f.write(f"**Recommendation:** {winner['Recommendation']}\n\n")

            f.write("## Detailed Analysis\n\n")

            # Performance Summary
            if "performance_summary" in self.tables:
                f.write("### Performance Analysis\n\n")
                df = self.tables["performance_summary"]
                # If no numeric scores available, add a clear note for the reader
                try:
                    if (
                        "Performance_Score" not in df.columns
                        or df["Performance_Score"].replace("N/A", np.nan).isna().all()
                    ):
                        f.write(
                            "**Note:** No performance results were found for this run. "
                            "To populate this section, run the ingestion and query benchmarks and place the "
                            "result JSON files into the corresponding `outputs/<database>` folders.\n\n"
                        )
                except Exception:
                    # If df is not a pandas DataFrame or check fails, skip the note
                    pass
                f.write(df.to_markdown(index=False))
                f.write("\n\n")

            # Security Comparison
            if "security_comparison" in self.tables:
                f.write("### Security Analysis\n\n")
                df = self.tables["security_comparison"]
                f.write(df.to_markdown(index=False))
                f.write("\n\n")

            # Storage Efficiency
            if "storage_efficiency" in self.tables:
                f.write("### Storage Efficiency\n\n")
                df = self.tables["storage_efficiency"]
                f.write(df.to_markdown(index=False))
                f.write("\n\n")

            # Cost Analysis
            if "cost_analysis" in self.tables:
                f.write("### Cost Analysis\n\n")
                df = self.tables["cost_analysis"]
                f.write(df.to_markdown(index=False))
                f.write("\n\n")

            # Final Ranking
            if "final_ranking" in self.tables:
                f.write("### Final Ranking\n\n")
                df = self.tables["final_ranking"]
                f.write(df.to_markdown(index=False))
                f.write("\n\n")

            f.write("## Recommendations by Use Case\n\n")

            recommendations = [
                (
                    "High-Security Healthcare Applications",
                    "PostgreSQL",
                    "Strong RLS and compliance features",
                ),
                (
                    "High-Volume IoT Sensor Data",
                    "InfluxDB",
                    "Optimized for time-series data ingestion",
                ),
                (
                    "Flexible Schema Document Storage",
                    "MongoDB",
                    "Excellent for evolving data structures",
                ),
                (
                    "Mixed Workload Applications",
                    "PostgreSQL",
                    "Balanced performance across all categories",
                ),
                (
                    "Cost-Sensitive Deployments",
                    "Based on specific needs",
                    "Consider cloud pricing and scaling requirements",
                ),
            ]

            f.write("| Use Case | Recommended Database | Reason |\n")
            f.write("|----------|---------------------|--------|\n")
            for use_case, db, reason in recommendations:
                f.write(f"| {use_case} | {db} | {reason} |\n")

            f.write("\n## Conclusion\n\n")
            f.write(
                "Based on comprehensive benchmarking across performance, security, storage efficiency, and cost, "
            )
            f.write(
                "each database has strengths in different areas. The final recommendation depends on specific "
            )
            f.write("application requirements and constraints.\n\n")

            f.write("## Next Steps\n\n")
            f.write("1. Review detailed metrics in the accompanying CSV files\n")
            f.write("2. Consider conducting application-specific benchmarks\n")
            f.write("3. Evaluate operational and maintenance requirements\n")
            f.write("4. Plan for scalability and future growth\n")

        logger.info(f"Comprehensive report saved to: {report_file}")
        return report_file

    def generate_executive_summary(self):
        """Generate executive summary (one-page report)"""
        print("\nGenerating executive summary...")

        summary_file = self.output_dir / f"executive_summary_{self.run_id}.txt"

        with open(summary_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("HEALTH IOT DATABASE BENCHMARKING - EXECUTIVE SUMMARY\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Report Date: {datetime.now().strftime('%Y-%m-%d')}\n")
            f.write(f"Run ID: {self.run_id}\n\n")

            # Overall Winner
            if "final_ranking" in self.tables:
                winner = self.tables["final_ranking"].iloc[0]
                f.write("🏆 OVERALL RECOMMENDATION\n")
                f.write("-" * 40 + "\n")
                f.write(f"Database: {winner['Database']}\n")
                f.write(f"Overall Score: {winner['Total_Score']}/100\n")
                f.write(f"Rank: #{winner['Rank']}\n")
                f.write(f"Recommendation: {winner['Recommendation']}\n\n")

            # Top 3 Databases
            f.write("📊 TOP 3 DATABASES\n")
            f.write("-" * 40 + "\n")

            if "final_ranking" in self.tables:
                top3 = self.tables["final_ranking"].head(3)
                for _, row in top3.iterrows():
                    f.write(
                        f"{row['Rank']}. {row['Database']}: {row['Total_Score']}/100\n"
                    )
            f.write("\n")

            # Best in Each Category
            f.write("⭐ BEST IN CATEGORY\n")
            f.write("-" * 40 + "\n")

            categories = [
                ("Performance", "performance_summary", "Performance_Score", True),
                ("Security", "security_comparison", "Overall_Security_Score", True),
                (
                    "Storage",
                    "storage_efficiency",
                    "Total_Size_MB",
                    False,
                ),  # Lower is better
                (
                    "Cost",
                    "cost_analysis",
                    "Total_Cost_Monthly",
                    False,
                ),  # Lower is better
            ]

            for category, table_key, metric, higher_better in categories:
                if table_key in self.tables:
                    df = self.tables[table_key]
                    # Skip if dataframe is empty or metric missing
                    if df.empty or metric not in df.columns:
                        continue
                    try:
                        if higher_better:
                            idx = df[metric].idxmax()
                        else:
                            idx = df[metric].idxmin()
                        best = df.loc[idx]
                        f.write(
                            f"{category}: {best['Database']} ({best[metric]:.1f})\n"
                        )
                    except Exception:
                        # Unable to compute best (e.g., all NaN); skip
                        continue
            f.write("\n")

            # Key Findings
            f.write("🔍 KEY FINDINGS\n")
            f.write("-" * 40 + "\n")

            findings = [
                "PostgreSQL excels in security and transactional consistency",
                "InfluxDB demonstrates superior time-series data ingestion rates",
                "MongoDB offers excellent flexibility for evolving schemas",
                "Security overhead varies significantly between databases",
                "Storage efficiency depends on data type and access patterns",
            ]

            for finding in findings:
                f.write(f"• {finding}\n")
            f.write("\n")

            # Recommendations
            f.write("💡 RECOMMENDATIONS\n")
            f.write("-" * 40 + "\n")

            recs = [
                "Use PostgreSQL for applications requiring strict compliance",
                "Choose InfluxDB for high-frequency sensor data collection",
                "Consider MongoDB for flexible document storage needs",
                "Implement database-specific optimizations based on workload",
                "Regularly review and update security configurations",
            ]

            for i, rec in enumerate(recs, 1):
                f.write(f"{i}. {rec}\n")
            f.write("\n")

            f.write("=" * 80 + "\n")
            f.write("END OF EXECUTIVE SUMMARY\n")
            f.write("=" * 80 + "\n")

        logger.info(f"Executive summary saved to: {summary_file}")
        return summary_file

    def _save_as_markdown(self, df: pd.DataFrame, filepath: Path, title: str):
        """Save DataFrame as markdown table"""
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n")
            f.write(df.to_markdown(index=False))
            f.write("\n")

    def export_all_tables(self):
        """Export all tables to various formats"""
        print("\nExporting all tables...")

        # Export each table to multiple formats
        for table_name, df in self.tables.items():
            # CSV
            csv_file = self.output_dir / f"{table_name}_{self.run_id}.csv"
            df.to_csv(csv_file, index=False)

            # JSON
            json_file = self.output_dir / f"{table_name}_{self.run_id}.json"
            df.to_json(json_file, orient="records", indent=2)

            # Excel (if pandas supports it)
            try:
                excel_file = self.output_dir / f"{table_name}_{self.run_id}.xlsx"
                df.to_excel(excel_file, index=False, sheet_name=table_name[:31])
            except ImportError:
                logger.warning("Note: Excel export requires openpyxl or xlsxwriter")

        # Create a consolidated Excel file with all tables
        self._create_consolidated_excel()

        logger.info("All tables exported successfully")

    def _create_consolidated_excel(self):
        """Create consolidated Excel file with all tables"""
        try:
            excel_file = self.output_dir / f"all_results_{self.run_id}.xlsx"

            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                for table_name, df in self.tables.items():
                    # Truncate sheet name to 31 characters (Excel limit)
                    sheet_name = table_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Add summary sheet
                summary_data = {
                    "Metric": [
                        "Total Tables Generated",
                        "Databases Compared",
                        "Run ID",
                        "Generation Time",
                    ],
                    "Value": [
                        len(self.tables),
                        3,
                        self.run_id,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    ],
                }
                pd.DataFrame(summary_data).to_excel(
                    writer, sheet_name="Summary", index=False
                )

            print(f"  Consolidated Excel file created: {excel_file}")

        except ImportError:
            print("  Note: Consolidated Excel requires openpyxl")
        except Exception as e:
            print(f"  Error creating consolidated Excel: {e}")

    def run(self):
        """Run complete final tables generation"""
        print("=" * 80)
        print("FINAL TABLES GENERATOR - Health IoT Database Benchmarking")
        print("=" * 80)

        # Generate all tables
        self.generate_performance_summary_table()
        self.generate_security_comparison_table()
        self.generate_storage_efficiency_table()
        self.generate_cost_analysis_table()
        self.generate_final_ranking_table()

        # Generate reports
        self.generate_comprehensive_report()
        self.generate_executive_summary()

        # Export all tables
        self.export_all_tables()

        print("\n" + "=" * 80)
        print("FINAL TABLES GENERATION COMPLETED SUCCESSFULLY")
        print("=" * 80)

        # Print summary
        print("\n📋 GENERATED TABLES:")
        for table_name in self.tables.keys():
            print(f"   • {table_name}")

        print(f"\n📁 Output directory: {self.output_dir}")

        return self.tables


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Generate final comparison tables")
    parser.add_argument("--run-id", help="Run ID for this analysis session")
    parser.add_argument("--base-dir", help="Base directory of the project")

    args = parser.parse_args()

    try:
        generator = FinalTablesGenerator(base_dir=args.base_dir, run_id=args.run_id)

        generator.run()

        return 0

    except Exception:
        logger.exception("Error during final tables generation")
        return 1


if __name__ == "__main__":
    sys.exit(main())
