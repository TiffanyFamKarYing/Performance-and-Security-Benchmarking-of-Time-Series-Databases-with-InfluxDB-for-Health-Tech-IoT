#!/usr/bin/env python3
"""
Visualization Module for Health IoT Database Benchmarking
Generates comprehensive visualizations and dashboards
"""

import argparse
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
from plotly.subplots import make_subplots

# Suppress warnings
warnings.filterwarnings("ignore")


class BenchmarkVisualizer:
    def __init__(self, base_dir: str = None, run_id: str = None):
        """Initialize visualizer"""
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent
        self.run_id = run_id or datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.output_dir = self.base_dir / "outputs" / "visualizations"

        # Create directories
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Set styles
        self._set_styles()

        # Load data
        self.data = self._load_data()

    def _set_styles(self):
        """Set visualization styles"""
        # Matplotlib style
        plt.style.use("seaborn-v0_8-darkgrid")
        matplotlib.rcParams["figure.figsize"] = [12, 8]
        matplotlib.rcParams["figure.dpi"] = 100
        matplotlib.rcParams["savefig.dpi"] = 300

        # Seaborn style
        sns.set_palette("husl")

        # Plotly template
        self.plotly_template = "plotly_white"

    def _load_data(self) -> Dict[str, Any]:
        """Load benchmark data from files"""
        print("Loading benchmark data...")

        data = {
            "performance": None,
            "security": None,
            "storage": None,
            "cost": None,
            "ranking": None,
        }

        # Load from final tables
        final_dir = self.base_dir / "outputs" / "final_results"

        if final_dir.exists():
            for file_type in data.keys():
                # Try common filename patterns, e.g. *_summary_*, *_comparison_*
                candidates = [
                    final_dir / f"{file_type}_summary_{self.run_id}.csv",
                    final_dir / f"{file_type}_comparison_{self.run_id}.csv",
                    final_dir / f"{file_type}_{self.run_id}.csv",
                ]
                loaded = False
                for csv_file in candidates:
                    if csv_file.exists():
                        try:
                            data[file_type] = pd.read_csv(csv_file)
                            print(f"  Loaded: {file_type} from {csv_file.name}")
                            loaded = True
                            break
                        except Exception as e:
                            print(f"  Error loading {csv_file.name}: {e}")
                if not loaded:
                    # Fallback: check for any file starting with file_type_
                    for fpath in final_dir.glob(f"{file_type}_*_{self.run_id}.csv"):
                        try:
                            data[file_type] = pd.read_csv(fpath)
                            print(f"  Loaded: {file_type} from {fpath.name}")
                            loaded = True
                            break
                        except Exception as e:
                            print(f"  Error loading {fpath.name}: {e}")
                    if not loaded:
                        print(f"  No {file_type} data found in final_results")

        # Also try to load from comparison files
        comparison_file = self.base_dir / "outputs" / f"comparison_{self.run_id}.csv"
        if comparison_file.exists():
            try:
                data["comparison"] = pd.read_csv(comparison_file)
                print("  Loaded: comparison")
            except Exception as e:
                print(f"  Error loading comparison: {e}")

        return data

    def create_performance_dashboard(self):
        """Create performance comparison dashboard"""
        print("\nCreating performance dashboard...")

        if self.data["performance"] is None:
            print("  No performance data found")
            return

        df = self.data["performance"]
        # Ensure expected columns exist
        if "Database" not in df.columns:
            print("  Performance data does not contain expected 'Database' column; skipping performance dashboard")
            return

        df = self.data["performance"]

        # Create subplot figure
        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=(
                "Overall Performance Scores",
                "Ingestion Rate Comparison",
                "Query Latency Comparison",
                "Storage Efficiency Scores",
            ),
            specs=[
                [{"type": "bar"}, {"type": "bar"}],
                [{"type": "bar"}, {"type": "bar"}],
            ],
        )

        # 1. Overall Performance Scores
        fig.add_trace(
            go.Bar(
                x=df["Database"],
                y=df["Performance_Score"],
                name="Performance Score",
                marker_color="lightblue",
                text=df["Performance_Score"].round(1),
                textposition="auto",
            ),
            row=1,
            col=1,
        )

        # 2. Ingestion Rate
        if "Ingestion_Rate" in df.columns:
            fig.add_trace(
                go.Bar(
                    x=df["Database"],
                    y=df["Ingestion_Rate"],
                    name="Ingestion Rate",
                    marker_color="lightgreen",
                    text=df["Ingestion_Rate"].round(0),
                    textposition="auto",
                ),
                row=1,
                col=2,
            )

        # 3. Query Latency
        if "Query_Latency_Avg" in df.columns:
            fig.add_trace(
                go.Bar(
                    x=df["Database"],
                    y=df["Query_Latency_Avg"],
                    name="Query Latency (ms)",
                    marker_color="salmon",
                    text=df["Query_Latency_Avg"].round(1),
                    textposition="auto",
                ),
                row=2,
                col=1,
            )

        # 4. Storage Efficiency
        if "Storage_Efficiency_Score" in df.columns:
            fig.add_trace(
                go.Bar(
                    x=df["Database"],
                    y=df["Storage_Efficiency_Score"],
                    name="Storage Efficiency",
                    marker_color="gold",
                    text=df["Storage_Efficiency_Score"].round(1),
                    textposition="auto",
                ),
                row=2,
                col=2,
            )

        # Update layout
        fig.update_layout(
            title_text=f"Performance Dashboard - Run {self.run_id}",
            showlegend=False,
            height=800,
            template=self.plotly_template,
        )

        # Save as HTML
        html_file = self.output_dir / f"performance_dashboard_{self.run_id}.html"
        fig.write_html(str(html_file))

        # Save as PNG
        png_file = self.output_dir / f"performance_dashboard_{self.run_id}.png"
        fig.write_image(str(png_file))

        print(f"  Dashboard saved to: {html_file}")
        return fig

    def create_security_radar_chart(self):
        """Create security radar chart"""
        print("\nCreating security radar chart...")

        if self.data["security"] is None:
            print("  No security data found")
            return

        df = self.data["security"]

        # Prepare data for radar chart
        categories = [
            "Security_Features",
            "Avg_Compliance_Score",
            "Overall_Security_Score",
        ]

        fig = go.Figure()

        for _, row in df.iterrows():
            values = [row[cat] for cat in categories]
            # Close the polygon
            values = values + [values[0]]

            fig.add_trace(
                go.Scatterpolar(
                    r=values,
                    theta=categories + [categories[0]],
                    fill="toself",
                    name=row["Database"],
                )
            )

        fig.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            title=f"Security Comparison Radar Chart - Run {self.run_id}",
            showlegend=True,
            template=self.plotly_template,
            height=600,
        )

        # Save files
        html_file = self.output_dir / f"security_radar_{self.run_id}.html"
        fig.write_html(str(html_file))

        png_file = self.output_dir / f"security_radar_{self.run_id}.png"
        fig.write_image(str(png_file))

        print(f"  Radar chart saved to: {html_file}")
        return fig

    def create_storage_comparison_chart(self):
        """Create storage comparison visualization"""
        print("\nCreating storage comparison chart...")

        if self.data["storage"] is None:
            print("  No storage data found")
            return

        df = self.data["storage"]

        # Create grouped bar chart
        fig = go.Figure()

        if "Data_Size_MB" in df.columns:
            fig.add_trace(
                go.Bar(
                    name="Data Size (MB)",
                    x=df["Database"],
                    y=df["Data_Size_MB"],
                    marker_color="blue",
                )
            )

        if "Index_Size_MB" in df.columns:
            fig.add_trace(
                go.Bar(
                    name="Index Size (MB)",
                    x=df["Database"],
                    y=df["Index_Size_MB"],
                    marker_color="orange",
                )
            )

        if "Total_Size_MB" in df.columns:
            fig.add_trace(
                go.Bar(
                    name="Total Size (MB)",
                    x=df["Database"],
                    y=df["Total_Size_MB"],
                    marker_color="green",
                )
            )

        fig.update_layout(
            barmode="group",
            title=f"Storage Usage Comparison - Run {self.run_id}",
            xaxis_title="Database",
            yaxis_title="Size (MB)",
            template=self.plotly_template,
            height=500,
        )

        # Save files
        html_file = self.output_dir / f"storage_comparison_{self.run_id}.html"
        fig.write_html(str(html_file))

        png_file = self.output_dir / f"storage_comparison_{self.run_id}.png"
        fig.write_image(str(png_file))

        print(f"  Storage chart saved to: {html_file}")
        return fig

    def create_cost_comparison_chart(self):
        """Create cost comparison visualization"""
        print("\nCreating cost comparison chart...")

        if self.data["cost"] is None:
            print("  No cost data found")
            return

        df = self.data["cost"]

        # Create stacked bar chart for cost breakdown
        fig = go.Figure()

        cost_columns = [
            "Compute_Cost_Monthly",
            "Storage_Cost_Monthly",
            "IO_Cost_Monthly",
        ]
        colors = ["#FF6B6B", "#4ECDC4", "#45B7D1"]

        for i, col in enumerate(cost_columns):
            if col in df.columns:
                fig.add_trace(
                    go.Bar(
                        name=col.replace("_", " "),
                        x=df["Database"],
                        y=df[col],
                        marker_color=colors[i % len(colors)],
                    )
                )

        fig.update_layout(
            barmode="stack",
            title=f"Monthly Cost Breakdown - Run {self.run_id}",
            xaxis_title="Database",
            yaxis_title="Cost ($)",
            template=self.plotly_template,
            height=500,
        )

        # Save files
        html_file = self.output_dir / f"cost_comparison_{self.run_id}.html"
        fig.write_html(str(html_file))

        png_file = self.output_dir / f"cost_comparison_{self.run_id}.png"
        fig.write_image(str(png_file))

        print(f"  Cost chart saved to: {html_file}")
        return fig

    def create_final_ranking_chart(self):
        """Create final ranking visualization"""
        print("\nCreating final ranking chart...")

        if self.data["ranking"] is None:
            print("  No ranking data found")
            return

        df = self.data["ranking"]

        # Create horizontal bar chart for ranking
        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=df["Total_Score"],
                y=df["Database"],
                orientation="h",
                marker_color=df["Total_Score"],
                text=df["Total_Score"].round(1),
                textposition="auto",
                hovertemplate="<b>%{y}</b><br>Score: %{x:.1f}<br>Rank: %{customdata}",
                customdata=df["Rank"],
            )
        )

        fig.update_layout(
            title=f"Final Ranking - Run {self.run_id}",
            xaxis_title="Total Score",
            yaxis_title="Database",
            template=self.plotly_template,
            height=400,
            showlegend=False,
        )

        # Save files
        html_file = self.output_dir / f"final_ranking_{self.run_id}.html"
        fig.write_html(str(html_file))

        png_file = self.output_dir / f"final_ranking_{self.run_id}.png"
        fig.write_image(str(png_file))

        print(f"  Ranking chart saved to: {html_file}")
        return fig

    def create_comprehensive_comparison_heatmap(self):
        """Create comprehensive comparison heatmap"""
        print("\nCreating comprehensive comparison heatmap...")

        # Combine all metrics into one DataFrame
        metrics_data = []

        for db in ["PostgreSQL", "InfluxDB", "MongoDB"]:
            db_metrics = {"Database": db}

            # Get performance metrics
            if self.data["performance"] is not None and "Database" in self.data["performance"].columns:
                perf_row = self.data["performance"][
                    self.data["performance"]["Database"] == db
                ]
                if not perf_row.empty:
                    db_metrics["Performance"] = perf_row.iloc[0].get(
                        "Performance_Score", 0
                    )
            else:
                # No performance Database column; leave as missing
                pass

            # Get security metrics
            if self.data["security"] is not None:
                sec_row = self.data["security"][self.data["security"]["Database"] == db]
                if not sec_row.empty:
                    db_metrics["Security"] = sec_row.iloc[0].get(
                        "Overall_Security_Score", 0
                    )

            # Get storage metrics (inverted - smaller is better)
            if self.data["storage"] is not None:
                storage_row = self.data["storage"][
                    self.data["storage"]["Database"] == db
                ]
                if not storage_row.empty and "Total_Size_MB" in storage_row.columns:
                    size = storage_row.iloc[0]["Total_Size_MB"]
                    max_size = self.data["storage"]["Total_Size_MB"].max()
                    db_metrics["Storage"] = (
                        (1 - (size / max_size)) * 100 if max_size > 0 else 0
                    )

            # Get cost metrics (inverted - lower cost is better)
            if self.data["cost"] is not None:
                cost_row = self.data["cost"][self.data["cost"]["Database"] == db]
                if not cost_row.empty and "Total_Cost_Monthly" in cost_row.columns:
                    cost = cost_row.iloc[0]["Total_Cost_Monthly"]
                    max_cost = self.data["cost"]["Total_Cost_Monthly"].max()
                    db_metrics["Cost"] = (
                        (1 - (cost / max_cost)) * 100 if max_cost > 0 else 0
                    )

            metrics_data.append(db_metrics)

        if not metrics_data:
            print("  No data available for heatmap")
            return

        df_metrics = pd.DataFrame(metrics_data)

        # Prepare data for heatmap
        heatmap_data = df_metrics.set_index("Database").T

        # Create heatmap
        fig = go.Figure(
            data=go.Heatmap(
                z=heatmap_data.values,
                x=heatmap_data.columns,
                y=heatmap_data.index,
                colorscale="RdYlGn",
                zmin=0,
                zmax=100,
                text=heatmap_data.values.round(1),
                texttemplate="%{text}",
                textfont={"size": 12},
                hoverongaps=False,
            )
        )

        fig.update_layout(
            title=f"Comprehensive Comparison Heatmap - Run {self.run_id}",
            xaxis_title="Database",
            yaxis_title="Metric",
            template=self.plotly_template,
            height=400,
        )

        # Save files
        html_file = self.output_dir / f"comparison_heatmap_{self.run_id}.html"
        fig.write_html(str(html_file))

        png_file = self.output_dir / f"comparison_heatmap_{self.run_id}.png"
        fig.write_image(str(png_file))

        print(f"  Heatmap saved to: {html_file}")
        return fig

    def create_interactive_dashboard(self):
        """Create interactive dashboard with all visualizations"""
        print("\nCreating interactive dashboard...")

        # Create dashboard HTML
        dashboard_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Health IoT Database Benchmarking Dashboard</title>
            <style>
                body {{ 
                    font-family: Arial, sans-serif; 
                    margin: 0; 
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                .header {{ 
                    text-align: center; 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                }}
                .dashboard-title {{ 
                    font-size: 2.5em; 
                    margin: 0; 
                }}
                .run-info {{ 
                    font-size: 1.2em; 
                    margin-top: 10px;
                    opacity: 0.9;
                }}
                .dashboard-container {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(600px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .chart-container {{
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .chart-title {{
                    text-align: center;
                    margin-bottom: 15px;
                    color: #333;
                    font-size: 1.3em;
                }}
                .summary-section {{
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    margin-bottom: 30px;
                }}
                .summary-title {{
                    color: #333;
                    border-bottom: 2px solid #667eea;
                    padding-bottom: 10px;
                    margin-bottom: 20px;
                }}
                .summary-content {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
                    gap: 20px;
                }}
                .metric-card {{
                    background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 10px;
                    text-align: center;
                }}
                .metric-value {{
                    font-size: 2.5em;
                    font-weight: bold;
                    margin: 10px 0;
                }}
                .metric-label {{
                    font-size: 1.1em;
                    opacity: 0.9;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 40px;
                    color: #666;
                    font-size: 0.9em;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1 class="dashboard-title">Health IoT Database Benchmarking</h1>
                <div class="run-info">Run ID: {self.run_id} | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            </div>
            
            <div class="summary-section">
                <h2 class="summary-title">Performance Summary</h2>
                <div class="summary-content">
        """

        # Add summary metrics
        if self.data["ranking"] is not None:
            df_ranking = self.data["ranking"]
            winner = df_ranking.iloc[0]

            dashboard_html += f"""
                    <div class="metric-card">
                        <div class="metric-label">🏆 Recommended Database</div>
                        <div class="metric-value">{winner['Database']}</div>
                        <div class="metric-label">Score: {winner['Total_Score']}/100</div>
                    </div>
                    
                    <div class="metric-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
                        <div class="metric-label">📊 Performance Leader</div>
                        <div class="metric-value">
            """

            # Find performance leader
            if self.data["performance"] is not None:
                perf_leader = self.data["performance"].loc[
                    self.data["performance"]["Performance_Score"].idxmax()
                ]
                dashboard_html += f"{perf_leader['Database']}</div>"
                dashboard_html += f"<div class='metric-label'>Score: {perf_leader['Performance_Score']}/100</div>"

            dashboard_html += """
                    </div>
                    
                    <div class="metric-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
                        <div class="metric-label">🔒 Security Leader</div>
                        <div class="metric-value">
            """

            # Find security leader
            if self.data["security"] is not None:
                sec_leader = self.data["security"].loc[
                    self.data["security"]["Overall_Security_Score"].idxmax()
                ]
                dashboard_html += f"{sec_leader['Database']}</div>"
                dashboard_html += f"<div class='metric-label'>Score: {sec_leader['Overall_Security_Score']}/100</div>"

            dashboard_html += """
                    </div>
                </div>
            </div>
            
            <div class="dashboard-container">
                <div class="chart-container">
                    <div class="chart-title">Final Ranking</div>
                    <iframe src="final_ranking_{run_id}.html" width="100%" height="400" frameborder="0"></iframe>
                </div>
                
                <div class="chart-container">
                    <div class="chart-title">Performance Dashboard</div>
                    <iframe src="performance_dashboard_{run_id}.html" width="100%" height="400" frameborder="0"></iframe>
                </div>
                
                <div class="chart-container">
                    <div class="chart-title">Security Comparison</div>
                    <iframe src="security_radar_{run_id}.html" width="100%" height="400" frameborder="0"></iframe>
                </div>
                
                <div class="chart-container">
                    <div class="chart-title">Storage Comparison</div>
                    <iframe src="storage_comparison_{run_id}.html" width="100%" height="400" frameborder="0"></iframe>
                </div>
                
                <div class="chart-container">
                    <div class="chart-title">Cost Comparison</div>
                    <iframe src="cost_comparison_{run_id}.html" width="100%" height="400" frameborder="0"></iframe>
                </div>
                
                <div class="chart-container">
                    <div class="chart-title">Comprehensive Heatmap</div>
                    <iframe src="comparison_heatmap_{run_id}.html" width="100%" height="400" frameborder="0"></iframe>
                </div>
            </div>
            
            <div class="footer">
                <p>Health IoT Database Benchmarking Suite | Generated with Python, Plotly, and Matplotlib</p>
                <p>© {datetime_now_year} Database Benchmarking Project</p>
            </div>
        </body>
        </html>
        """.format(
                run_id=self.run_id, datetime_now_year=datetime.now().year
            )

        # Save dashboard
        dashboard_file = self.output_dir / f"interactive_dashboard_{self.run_id}.html"
        with open(dashboard_file, "w") as f:
            f.write(dashboard_html)

        print(f"  Interactive dashboard saved to: {dashboard_file}")
        return dashboard_file

    def create_matplotlib_figures(self):
        """Create static matplotlib figures for reports"""
        print("\nCreating static matplotlib figures...")

        # Set up the figure grid
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()

        # 1. Performance Scores
        if self.data["performance"] is not None and "Database" in self.data["performance"].columns:
            df = self.data["performance"]
            ax = axes[0]
            bars = ax.bar(
                df["Database"],
                df["Performance_Score"],
                color=["#4CAF50", "#2196F3", "#FF9800"],
            )
            ax.set_title("Performance Scores", fontsize=14, fontweight="bold")
            ax.set_ylabel("Score (0-100)")
            ax.set_ylim(0, 105)

            # Add value labels
            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    height + 1,
                    f"{height:.1f}",
                    ha="center",
                    va="bottom",
                )
        else:
            axes[0].text(0.5, 0.5, "Performance data not available", ha="center", va="center")
            axes[0].set_axis_off()

        # 2. Security Scores
        if self.data["security"] is not None:
            df = self.data["security"]
            ax = axes[1]
            bars = ax.bar(
                df["Database"],
                df["Overall_Security_Score"],
                color=["#4CAF50", "#2196F3", "#FF9800"],
            )
            ax.set_title("Security Scores", fontsize=14, fontweight="bold")
            ax.set_ylabel("Score (0-100)")
            ax.set_ylim(0, 105)

            for bar in bars:
                height = bar.get_height()
                ax.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    height + 1,
                    f"{height:.1f}",
                    ha="center",
                    va="bottom",
                )

        # 3. Storage Size
        if self.data["storage"] is not None:
            df = self.data["storage"]
            ax = axes[2]

            if "Total_Size_MB" in df.columns:
                bars = ax.bar(
                    df["Database"],
                    df["Total_Size_MB"],
                    color=["#4CAF50", "#2196F3", "#FF9800"],
                )
                ax.set_title("Storage Size Comparison", fontsize=14, fontweight="bold")
                ax.set_ylabel("Size (MB)")

                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height + 1,
                        f"{height:.0f}",
                        ha="center",
                        va="bottom",
                    )

        # 4. Monthly Cost
        if self.data["cost"] is not None:
            df = self.data["cost"]
            ax = axes[3]

            if "Total_Cost_Monthly" in df.columns:
                bars = ax.bar(
                    df["Database"],
                    df["Total_Cost_Monthly"],
                    color=["#4CAF50", "#2196F3", "#FF9800"],
                )
                ax.set_title("Monthly Cost Comparison", fontsize=14, fontweight="bold")
                ax.set_ylabel("Cost ($)")

                for bar in bars:
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height + 0.1,
                        f"${height:.2f}",
                        ha="center",
                        va="bottom",
                    )

        # 5. Final Ranking
        if self.data["ranking"] is not None:
            df = self.data["ranking"]
            ax = axes[4]

            # Sort by rank
            df_sorted = df.sort_values("Rank")
            colors = ["gold", "silver", "#cd7f32"]  # Gold, Silver, Bronze

            bars = ax.barh(
                df_sorted["Database"],
                df_sorted["Total_Score"],
                color=colors[: len(df_sorted)],
            )
            ax.set_title("Final Ranking", fontsize=14, fontweight="bold")
            ax.set_xlabel("Total Score")
            ax.set_xlim(0, 105)

            # Add rank numbers
            for i, (bar, rank) in enumerate(zip(bars, df_sorted["Rank"])):
                width = bar.get_width()
                ax.text(
                    width + 1,
                    bar.get_y() + bar.get_height() / 2,
                    f"#{rank}",
                    va="center",
                )

        # 6. Metric Comparison (placeholder or additional metric)
        ax = axes[5]
        ax.text(
            0.5,
            0.5,
            "Additional Analysis\n(CPU Usage, Memory, etc.)",
            ha="center",
            va="center",
            transform=ax.transAxes,
            fontsize=12,
        )
        ax.set_title("Additional Metrics", fontsize=14, fontweight="bold")
        ax.axis("off")

        # Adjust layout
        plt.suptitle(
            f"Health IoT Database Benchmarking - Run {self.run_id}",
            fontsize=16,
            fontweight="bold",
            y=1.02,
        )
        plt.tight_layout()

        # Save figure
        static_file = self.output_dir / f"static_summary_{self.run_id}.png"
        plt.savefig(static_file, dpi=300, bbox_inches="tight")
        plt.close()

        print(f"  Static summary figure saved to: {static_file}")

        return static_file

    def generate_all_visualizations(self):
        """Generate all visualizations"""
        print("=" * 80)
        print("VISUALIZATION GENERATOR - Health IoT Database Benchmarking")
        print("=" * 80)

        # Generate individual visualizations
        self.create_performance_dashboard()
        self.create_security_radar_chart()
        self.create_storage_comparison_chart()
        self.create_cost_comparison_chart()
        self.create_final_ranking_chart()
        self.create_comprehensive_comparison_heatmap()

        # Generate combined visualizations
        self.create_interactive_dashboard()
        self.create_matplotlib_figures()

        # Create visualization summary
        self._create_visualization_summary()

        print("\n" + "=" * 80)
        print("VISUALIZATION GENERATION COMPLETED SUCCESSFULLY")
        print("=" * 80)

        # Print summary
        print("\n📊 Generated Visualizations:")
        print("   • Performance Dashboard")
        print("   • Security Radar Chart")
        print("   • Storage Comparison")
        print("   • Cost Comparison")
        print("   • Final Ranking Chart")
        print("   • Comprehensive Heatmap")
        print("   • Interactive Dashboard")
        print("   • Static Summary Figures")

        print(f"\n📁 Output directory: {self.output_dir}")

    def _create_visualization_summary(self):
        """Create visualization summary file"""
        summary_file = self.output_dir / f"visualization_summary_{self.run_id}.txt"

        with open(summary_file, "w", encoding="utf-8") as f:
            f.write("=" * 80 + "\n")
            f.write("VISUALIZATION SUMMARY - Health IoT Database Benchmarking\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Run ID: {self.run_id}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Output Directory: {self.output_dir}\n\n")

            f.write("GENERATED VISUALIZATIONS:\n")
            f.write("-" * 40 + "\n\n")

            visualizations = [
                (
                    "Interactive Dashboard",
                    "interactive_dashboard_{run_id}.html",
                    "Complete dashboard with all charts",
                ),
                (
                    "Performance Dashboard",
                    "performance_dashboard_{run_id}.html",
                    "Performance metrics comparison",
                ),
                (
                    "Security Radar Chart",
                    "security_radar_{run_id}.html",
                    "Security feature comparison",
                ),
                (
                    "Storage Comparison",
                    "storage_comparison_{run_id}.html",
                    "Storage efficiency analysis",
                ),
                (
                    "Cost Comparison",
                    "cost_comparison_{run_id}.html",
                    "Cost breakdown analysis",
                ),
                (
                    "Final Ranking",
                    "final_ranking_{run_id}.html",
                    "Final ranking visualization",
                ),
                (
                    "Comparison Heatmap",
                    "comparison_heatmap_{run_id}.html",
                    "Comprehensive metric heatmap",
                ),
                (
                    "Static Summary",
                    "static_summary_{run_id}.png",
                    "Static figure for reports",
                ),
            ]

            for name, filename, description in visualizations:
                actual_filename = filename.format(run_id=self.run_id)
                file_path = self.output_dir / actual_filename
                if file_path.exists():
                    f.write(f"✓ {name}\n")
                    f.write(f"  File: {actual_filename}\n")
                    f.write(f"  Description: {description}\n")
                    f.write(f"  Size: {file_path.stat().st_size / 1024:.1f} KB\n\n")
                else:
                    f.write(f"✗ {name} (not generated)\n\n")

            f.write("=" * 80 + "\n")
            f.write("END OF SUMMARY\n")
            f.write("=" * 80 + "\n")

        print(f"  Visualization summary saved to: {summary_file}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Generate visualizations for benchmark results"
    )
    parser.add_argument("--run-id", help="Run ID for this visualization session")
    parser.add_argument("--base-dir", help="Base directory of the project")

    args = parser.parse_args()

    try:
        visualizer = BenchmarkVisualizer(base_dir=args.base_dir, run_id=args.run_id)

        visualizer.generate_all_visualizations()

        return 0

    except Exception as e:
        print(f"Error during visualization generation: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
