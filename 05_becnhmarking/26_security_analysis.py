#!/usr/bin/env python3
"""
Security Analysis for Health IoT Database Benchmarking
Analyzes security features, overhead, and compliance across databases
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

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


class SecurityAnalyzer:
    def __init__(self, base_dir: str = None, run_id: str = None):
        """Initialize security analyzer"""
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent
        self.run_id = run_id or datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.output_dir = self.base_dir / "outputs" / "security_analysis"
        self.results_dir = self.base_dir / "outputs"

        # Create directories
        self.output_dir.mkdir(exist_ok=True, parents=True)

        # Security frameworks and standards
        self.security_frameworks = {
            "hipaa": {
                "name": "HIPAA",
                "requirements": [
                    "access_controls",
                    "audit_trails",
                    "data_encryption",
                    "authentication",
                    "authorization",
                ],
            },
            "gdpr": {
                "name": "GDPR",
                "requirements": [
                    "data_protection",
                    "consent_management",
                    "right_to_erasure",
                    "data_portability",
                    "privacy_by_design",
                ],
            },
            "iso_27001": {
                "name": "ISO 27001",
                "requirements": [
                    "risk_assessment",
                    "security_policies",
                    "access_control",
                    "cryptography",
                    "physical_security",
                ],
            },
        }

        # Initialize results
        self.analysis_results = {
            "metadata": {
                "run_id": self.run_id,
                "timestamp": datetime.now().isoformat(),
                "frameworks_analyzed": list(self.security_frameworks.keys()),
            },
            "database_assessments": {},
            "compliance_scores": {},
            "vulnerability_analysis": {},
            "recommendations": {},
        }

    def analyze_postgresql_security(self):
        """Analyze PostgreSQL security features"""
        logger.info("Analyzing PostgreSQL security...")

        assessment = {
            "database": "PostgreSQL",
            "security_features": [],
            "strengths": [],
            "weaknesses": [],
            "compliance_status": {},
            "overhead_metrics": {},
        }

        # Load PostgreSQL security results if available
        pg_results_file = self.results_dir / "postgresql" / "security_overhead.json"

        if pg_results_file.exists():
            try:
                with open(pg_results_file, "r") as f:
                    pg_data = json.load(f)
                    assessment["overhead_metrics"] = pg_data
            except Exception as e:
                print(f"  Error loading PostgreSQL results: {e}")

        # PostgreSQL security features assessment
        pg_features = [
            {
                "feature": "Row Level Security (RLS)",
                "implementation": "Native",
                "strength": "High",
                "description": "Fine-grained access control at row level",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "SSL/TLS Encryption",
                "implementation": "Native",
                "strength": "High",
                "description": "Encrypted connections",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "Column Encryption",
                "implementation": "pgcrypto extension",
                "strength": "Medium",
                "description": "Column-level encryption support",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "Audit Logging",
                "implementation": "pgaudit extension",
                "strength": "High",
                "description": "Comprehensive audit trail",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "Role-Based Access Control",
                "implementation": "Native",
                "strength": "High",
                "description": "Granular permissions system",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
        ]

        assessment["security_features"] = pg_features

        # Strengths
        assessment["strengths"] = [
            "Mature and proven security model",
            "Comprehensive access control (RLS)",
            "Strong encryption support",
            "Extensive audit capabilities",
            "Active security community",
        ]

        # Weaknesses
        assessment["weaknesses"] = [
            "Performance overhead with RLS on large tables",
            "Complex configuration for advanced features",
            "Requires extensions for some security features",
        ]

        # Compliance assessment
        assessment["compliance_status"] = self._assess_compliance(
            pg_features, "PostgreSQL"
        )

        self.analysis_results["database_assessments"]["postgresql"] = assessment
        return assessment

    def analyze_influxdb_security(self):
        """Analyze InfluxDB security features"""
        logger.info("Analyzing InfluxDB security...")

        assessment = {
            "database": "InfluxDB",
            "security_features": [],
            "strengths": [],
            "weaknesses": [],
            "compliance_status": {},
            "overhead_metrics": {},
        }

        # Load InfluxDB security results if available
        influx_results_file = self.results_dir / "influxdb" / "security_tokens.json"

        if influx_results_file.exists():
            try:
                with open(influx_results_file, "r") as f:
                    influx_data = json.load(f)
                    assessment["overhead_metrics"] = influx_data
            except Exception as e:
                print(f"  Error loading InfluxDB results: {e}")

        # InfluxDB security features assessment
        influx_features = [
            {
                "feature": "Token-Based Authentication",
                "implementation": "Native",
                "strength": "High",
                "description": "API tokens for authentication",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "TLS/SSL Encryption",
                "implementation": "Native",
                "strength": "High",
                "description": "Encrypted communications",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "User Management",
                "implementation": "Native",
                "strength": "Medium",
                "description": "Basic user and permission system",
                "hipaa_compliant": True,
                "gdpr_compliant": False,  # Limited granularity
            },
            {
                "feature": "Audit Logs",
                "implementation": "Enterprise Only",
                "strength": "Low",
                "description": "Limited in open source version",
                "hipaa_compliant": False,
                "gdpr_compliant": False,
            },
            {
                "feature": "Data Retention Policies",
                "implementation": "Native",
                "strength": "High",
                "description": "Automatic data expiration",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
        ]

        assessment["security_features"] = influx_features

        # Strengths
        assessment["strengths"] = [
            "Simple token-based authentication",
            "Built-in TLS/SSL support",
            "Automatic data retention",
            "Lightweight security model",
            "Good for IoT sensor data",
        ]

        # Weaknesses
        assessment["weaknesses"] = [
            "Limited audit capabilities in OSS version",
            "Basic permission system",
            "No native row-level security",
            "Limited encryption at rest in OSS",
        ]

        # Compliance assessment
        assessment["compliance_status"] = self._assess_compliance(
            influx_features, "InfluxDB"
        )

        self.analysis_results["database_assessments"]["influxdb"] = assessment
        return assessment

    def analyze_mongodb_security(self):
        """Analyze MongoDB security features"""
        logger.info("Analyzing MongoDB security...")

        assessment = {
            "database": "MongoDB",
            "security_features": [],
            "strengths": [],
            "weaknesses": [],
            "compliance_status": {},
            "overhead_metrics": {},
        }

        # Load MongoDB security results if available
        mongo_results_file = (
            self.results_dir / "mongodb_indexing" / "security_roles.json"
        )

        if mongo_results_file.exists():
            try:
                with open(mongo_results_file, "r") as f:
                    mongo_data = json.load(f)
                    assessment["overhead_metrics"] = mongo_data
            except Exception as e:
                print(f"  Error loading MongoDB results: {e}")

        # MongoDB security features assessment
        mongo_features = [
            {
                "feature": "Role-Based Access Control",
                "implementation": "Native",
                "strength": "High",
                "description": "Fine-grained role permissions",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "Field-Level Encryption",
                "implementation": "Enterprise Only",
                "strength": "High",
                "description": "Client-side field encryption",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "Audit Logging",
                "implementation": "Enterprise Only",
                "strength": "Medium",
                "description": "Comprehensive audit trail",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "TLS/SSL Encryption",
                "implementation": "Native",
                "strength": "High",
                "description": "Encrypted connections",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
            {
                "feature": "LDAP Integration",
                "implementation": "Enterprise Only",
                "strength": "Medium",
                "description": "Enterprise directory integration",
                "hipaa_compliant": True,
                "gdpr_compliant": True,
            },
        ]

        assessment["security_features"] = mongo_features

        # Strengths
        assessment["strengths"] = [
            "Flexible role-based permissions",
            "Strong enterprise security features",
            "Good encryption support",
            "Document-level security",
            "Active directory integration",
        ]

        # Weaknesses
        assessment["weaknesses"] = [
            "Many advanced features require Enterprise edition",
            "Complex configuration for full security",
            "Performance impact with field-level encryption",
        ]

        # Compliance assessment
        assessment["compliance_status"] = self._assess_compliance(
            mongo_features, "MongoDB"
        )

        self.analysis_results["database_assessments"]["mongodb"] = assessment
        return assessment

    # Short answer ✅
    # PostgreSQL shows no points on the radar because the compliance checks use exact token
    # matches like "access_controls" and "audit_trails", but the PostgreSQL feature
    # descriptions use human-readable phrases like "access control" and "audit logging".
    # The current matching logic therefore fails to find matches and yields 0 scores for
    # each framework.
    #
    # Details & fix suggestion 🔧
    # Cause: _assess_compliance() checks `req in feature.get("description", "").lower()`
    # where `req` is snake_case (e.g., access_controls) but descriptions use spaces/synonyms
    # (e.g., "access control", "audit logging"). That mismatch makes the score remain 0.
    # Minimal fix: normalize requirement terms before matching, e.g.:
    #   - compare `req.replace("_", " ")` with feature text, and
    #   - fallback to synonyms (e.g., map audit_trails → "audit log|audit trail|audit logging")
    #
    # Note: The user opted NOT to apply the automatic fix now.
    def _assess_compliance(
        self, features: List[Dict], database: str
    ) -> Dict[str, Dict]:
        """Assess compliance with security frameworks"""
        compliance_scores = {}

        for framework_key, framework in self.security_frameworks.items():
            framework_name = framework["name"]
            requirements = framework["requirements"]

            score = 0
            max_score = len(requirements)
            compliance_details = []

            for req in requirements:
                # Check if any feature supports this requirement
                feature_support = any(
                    req in feature.get("description", "").lower()
                    or req in feature.get("feature", "").lower()
                    for feature in features
                )

                if feature_support:
                    score += 1
                    compliance_details.append(
                        {
                            "requirement": req,
                            "status": "Compliant",
                            "features": [
                                f["feature"]
                                for f in features
                                if req in f.get("description", "").lower()
                                or req in f.get("feature", "").lower()
                            ],
                        }
                    )
                else:
                    compliance_details.append(
                        {
                            "requirement": req,
                            "status": "Non-Compliant",
                            "notes": f"No native {database} feature found",
                        }
                    )

            compliance_score = (score / max_score) * 100 if max_score > 0 else 0

            compliance_scores[framework_key] = {
                "framework": framework_name,
                "score": round(compliance_score, 2),
                "status": self._get_compliance_status(compliance_score),
                "details": compliance_details,
            }

        return compliance_scores

    def _get_compliance_status(self, score: float) -> str:
        """Get compliance status based on score"""
        if score >= 90:
            return "Fully Compliant"
        elif score >= 70:
            return "Mostly Compliant"
        elif score >= 50:
            return "Partially Compliant"
        else:
            return "Non-Compliant"

    def perform_vulnerability_analysis(self):
        """Perform vulnerability analysis across databases"""
        print("\nPerforming vulnerability analysis...")

        vulnerabilities = {
            "postgresql": self._analyze_postgresql_vulnerabilities(),
            "influxdb": self._analyze_influxdb_vulnerabilities(),
            "mongodb": self._analyze_mongodb_vulnerabilities(),
        }

        # Calculate risk scores
        for db, vuln_list in vulnerabilities.items():
            if vuln_list:
                avg_severity = np.mean([v.get("severity_score", 0) for v in vuln_list])
                vulnerabilities[db].append(
                    {
                        "summary": "Risk Assessment",
                        "total_vulnerabilities": len(vuln_list),
                        "average_severity": round(avg_severity, 2),
                        "risk_level": self._get_risk_level(avg_severity),
                    }
                )

        self.analysis_results["vulnerability_analysis"] = vulnerabilities
        return vulnerabilities

    def _analyze_postgresql_vulnerabilities(self) -> List[Dict]:
        """Analyze PostgreSQL-specific vulnerabilities"""
        vulnerabilities = [
            {
                "id": "PG-001",
                "type": "Configuration",
                "severity": "Medium",
                "severity_score": 5,
                "description": "Default postgres user with weak/no password",
                "mitigation": "Change default password, use strong authentication",
                "cve_references": ["CVE-2019-9193"],
            },
            {
                "id": "PG-002",
                "type": "Privilege Escalation",
                "severity": "High",
                "severity_score": 8,
                "description": "Potential privilege escalation through extensions",
                "mitigation": "Restrict extension installation to superusers only",
                "cve_references": ["CVE-2018-1058"],
            },
            {
                "id": "PG-003",
                "type": "Denial of Service",
                "severity": "Medium",
                "severity_score": 6,
                "description": "Resource exhaustion through complex queries",
                "mitigation": "Implement query timeouts and resource limits",
                "cve_references": [],
            },
        ]
        return vulnerabilities

    def _analyze_influxdb_vulnerabilities(self) -> List[Dict]:
        """Analyze InfluxDB-specific vulnerabilities"""
        vulnerabilities = [
            {
                "id": "IF-001",
                "type": "Authentication",
                "severity": "High",
                "severity_score": 9,
                "description": "Default installation without authentication enabled",
                "mitigation": "Always enable authentication in production",
                "cve_references": ["CVE-2019-20933"],
            },
            {
                "id": "IF-002",
                "type": "Data Exposure",
                "severity": "Medium",
                "severity_score": 5,
                "description": "Unencrypted data at rest in OSS version",
                "mitigation": "Use filesystem encryption or Enterprise edition",
                "cve_references": [],
            },
            {
                "id": "IF-003",
                "type": "API Security",
                "severity": "Medium",
                "severity_score": 6,
                "description": "Insecure default API configurations",
                "mitigation": "Use HTTPS, restrict CORS, implement rate limiting",
                "cve_references": ["CVE-2020-13937"],
            },
        ]
        return vulnerabilities

    def _analyze_mongodb_vulnerabilities(self) -> List[Dict]:
        """Analyze MongoDB-specific vulnerabilities"""
        vulnerabilities = [
            {
                "id": "MG-001",
                "type": "Authentication",
                "severity": "Critical",
                "severity_score": 10,
                "description": "Default installation without access control",
                "mitigation": "Enable access control before production deployment",
                "cve_references": [],
            },
            {
                "id": "MG-002",
                "type": "Encryption",
                "severity": "High",
                "severity_score": 8,
                "description": "Unencrypted data at rest in community edition",
                "mitigation": "Use WiredTiger encryption or filesystem encryption",
                "cve_references": [],
            },
            {
                "id": "MG-003",
                "type": "Injection",
                "severity": "Medium",
                "severity_score": 6,
                "description": "Potential NoSQL injection vulnerabilities",
                "mitigation": "Use parameterized queries, input validation",
                "cve_references": ["CVE-2019-10758"],
            },
        ]
        return vulnerabilities

    def _get_risk_level(self, score: float) -> str:
        """Get risk level based on severity score"""
        if score >= 8:
            return "Critical"
        elif score >= 6:
            return "High"
        elif score >= 4:
            return "Medium"
        else:
            return "Low"

    def calculate_overall_security_scores(self):
        """Calculate overall security scores for each database"""
        print("\nCalculating overall security scores...")

        security_scores = {}

        for db_name, assessment in self.analysis_results[
            "database_assessments"
        ].items():
            scores = []

            # Compliance scores (weight: 40%)
            compliance_scores = assessment.get("compliance_status", {})
            avg_compliance = (
                np.mean([v["score"] for v in compliance_scores.values()])
                if compliance_scores
                else 0
            )
            scores.append(avg_compliance * 0.4)

            # Feature completeness (weight: 30%)
            features = assessment.get("security_features", [])
            feature_score = self._calculate_feature_score(features)
            scores.append(feature_score * 0.3)

            # Vulnerability risk (weight: 20%)
            vulns = self.analysis_results["vulnerability_analysis"].get(db_name, [])
            risk_score = 100 - (
                self._calculate_risk_score(vulns) * 10
            )  # Convert to 0-100 scale
            scores.append(risk_score * 0.2)

            # Maturity/community (weight: 10%)
            maturity_score = self._calculate_maturity_score(db_name)
            scores.append(maturity_score * 0.1)

            overall_score = sum(scores)

            security_scores[db_name] = {
                "overall_score": round(overall_score, 2),
                "category_scores": {
                    "compliance": round(avg_compliance, 2),
                    "features": round(feature_score, 2),
                    "risk": round(risk_score, 2),
                    "maturity": round(maturity_score, 2),
                },
                "security_grade": self._get_security_grade(overall_score),
            }

        self.analysis_results["compliance_scores"] = security_scores
        return security_scores

    def _calculate_feature_score(self, features: List[Dict]) -> float:
        """Calculate feature completeness score"""
        if not features:
            return 0

        # Score based on strength ratings
        strength_scores = {"High": 100, "Medium": 70, "Low": 40}

        total_score = sum(
            strength_scores.get(f.get("strength", "Low"), 40) for f in features
        )
        avg_score = total_score / len(features)

        return avg_score

    def _calculate_risk_score(self, vulnerabilities: List[Dict]) -> float:
        """Calculate risk score from vulnerabilities"""
        if not vulnerabilities:
            return 0

        # Get severity scores from vulnerability entries (excluding summary)
        severity_scores = [
            v.get("severity_score", 0) for v in vulnerabilities if "severity_score" in v
        ]

        if not severity_scores:
            return 0

        return np.mean(severity_scores)

    def _calculate_maturity_score(self, database: str) -> float:
        """Calculate maturity/community support score"""
        maturity_scores = {
            "postgresql": 95,  # Very mature, large community
            "mongodb": 85,  # Mature, good community
            "influxdb": 75,  # Less mature but growing
        }

        return maturity_scores.get(database, 50)

    def _get_security_grade(self, score: float) -> str:
        """Get security grade from score"""
        if score >= 90:
            return "A+"
        elif score >= 85:
            return "A"
        elif score >= 80:
            return "A-"
        elif score >= 75:
            return "B+"
        elif score >= 70:
            return "B"
        elif score >= 65:
            return "B-"
        elif score >= 60:
            return "C+"
        elif score >= 55:
            return "C"
        elif score >= 50:
            return "C-"
        else:
            return "D"

    def generate_recommendations(self):
        """Generate security recommendations"""
        print("\nGenerating security recommendations...")

        recommendations = {
            "postgresql": self._generate_postgresql_recommendations(),
            "influxdb": self._generate_influxdb_recommendations(),
            "mongodb": self._generate_mongodb_recommendations(),
            "general": self._generate_general_recommendations(),
        }

        self.analysis_results["recommendations"] = recommendations
        return recommendations

    def _generate_postgresql_recommendations(self) -> List[Dict]:
        """Generate PostgreSQL-specific recommendations"""
        return [
            {
                "priority": "High",
                "category": "Authentication",
                "recommendation": "Implement strong password policies and consider LDAP/PAM integration",
                "effort": "Medium",
                "impact": "High",
            },
            {
                "priority": "High",
                "category": "Encryption",
                "recommendation": "Enable TLS for all connections and consider column-level encryption for sensitive data",
                "effort": "Low",
                "impact": "High",
            },
            {
                "priority": "Medium",
                "category": "Monitoring",
                "recommendation": "Implement comprehensive audit logging using pgaudit extension",
                "effort": "Medium",
                "impact": "Medium",
            },
        ]

    def _generate_influxdb_recommendations(self) -> List[Dict]:
        """Generate InfluxDB-specific recommendations"""
        return [
            {
                "priority": "Critical",
                "category": "Authentication",
                "recommendation": "Always enable authentication in production environments",
                "effort": "Low",
                "impact": "High",
            },
            {
                "priority": "High",
                "category": "Encryption",
                "recommendation": "Use TLS for API communications and consider Enterprise edition for encryption at rest",
                "effort": "Medium",
                "impact": "High",
            },
            {
                "priority": "Medium",
                "category": "Access Control",
                "recommendation": "Implement fine-grained token permissions and regular token rotation",
                "effort": "Low",
                "impact": "Medium",
            },
        ]

    def _generate_mongodb_recommendations(self) -> List[Dict]:
        """Generate MongoDB-specific recommendations"""
        return [
            {
                "priority": "Critical",
                "category": "Configuration",
                "recommendation": "Enable access control before deploying to production",
                "effort": "Low",
                "impact": "High",
            },
            {
                "priority": "High",
                "category": "Encryption",
                "recommendation": "Implement encryption at rest using WiredTiger or filesystem encryption",
                "effort": "Medium",
                "impact": "High",
            },
            {
                "priority": "Medium",
                "category": "Network Security",
                "recommendation": "Bind MongoDB to internal interfaces only and use firewalls",
                "effort": "Low",
                "impact": "Medium",
            },
        ]

    def _generate_general_recommendations(self) -> List[Dict]:
        """Generate general security recommendations"""
        return [
            {
                "priority": "High",
                "category": "All Databases",
                "recommendation": "Implement regular security patching and updates",
                "effort": "Low",
                "impact": "High",
            },
            {
                "priority": "High",
                "category": "All Databases",
                "recommendation": "Use network segmentation and firewalls to isolate database servers",
                "effort": "Medium",
                "impact": "High",
            },
            {
                "priority": "Medium",
                "category": "All Databases",
                "recommendation": "Implement regular security audits and penetration testing",
                "effort": "High",
                "impact": "Medium",
            },
        ]

    def generate_reports(self):
        """Generate comprehensive security reports"""
        print("\nGenerating security reports...")

        # Generate text report
        self._generate_text_report()

        # Generate JSON report
        self._generate_json_report()

        # Generate visualizations
        self._generate_visualizations()

        # Generate compliance matrix
        self._generate_compliance_matrix()

    def _generate_text_report(self):
        """Generate detailed text report"""
        report_file = self.output_dir / f"security_analysis_report_{self.run_id}.txt"

        with open(report_file, "w") as f:
            f.write("=" * 80 + "\n")
            f.write("HEALTH IOT DATABASE SECURITY ANALYSIS REPORT\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Run ID: {self.run_id}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Overall Security Scores
            f.write("OVERALL SECURITY SCORES\n")
            f.write("-" * 80 + "\n")

            if self.analysis_results.get("compliance_scores"):
                for db, scores in self.analysis_results["compliance_scores"].items():
                    f.write(f"\n{db.upper()}:\n")
                    f.write(f"  Overall Score: {scores['overall_score']}/100\n")
                    f.write(f"  Security Grade: {scores['security_grade']}\n")
                    f.write("  Category Scores:\n")
                    for category, score in scores["category_scores"].items():
                        f.write(f"    • {category.capitalize()}: {score}/100\n")

            # Compliance Assessment
            f.write("\n\nCOMPLIANCE ASSESSMENT\n")
            f.write("-" * 80 + "\n")

            for db, assessment in self.analysis_results["database_assessments"].items():
                f.write(f"\n{db.upper()}:\n")
                for framework, status in assessment.get(
                    "compliance_status", {}
                ).items():
                    f.write(
                        f"  {status['framework']}: {status['score']}/100 ({status['status']})\n"
                    )

            # Vulnerability Analysis
            f.write("\n\nVULNERABILITY ANALYSIS\n")
            f.write("-" * 80 + "\n")

            vulnerabilities = self.analysis_results.get("vulnerability_analysis", {})
            for db, vulns in vulnerabilities.items():
                f.write(f"\n{db.upper()}:\n")
                for vuln in vulns:
                    if "id" in vuln:  # Skip summary entries
                        f.write(f"  • {vuln['id']}: {vuln['description']}\n")
                        f.write(
                            f"    Severity: {vuln['severity']} ({vuln['severity_score']}/10)\n"
                        )
                        f.write(f"    Mitigation: {vuln['mitigation']}\n")

            # Recommendations
            f.write("\n\nSECURITY RECOMMENDATIONS\n")
            f.write("-" * 80 + "\n")

            recommendations = self.analysis_results.get("recommendations", {})
            for db, recs in recommendations.items():
                f.write(f"\n{db.upper()}:\n")
                for rec in recs:
                    f.write(
                        f"  [{rec['priority']}] {rec['category']}: {rec['recommendation']}\n"
                    )
                    f.write(f"    Effort: {rec['effort']}, Impact: {rec['impact']}\n")

            f.write("\n" + "=" * 80 + "\n")
            f.write("END OF REPORT\n")
            f.write("=" * 80 + "\n")

        print(f"Text report saved to: {report_file}")

    def _generate_json_report(self):
        """Generate JSON report"""
        json_file = self.output_dir / f"security_analysis_{self.run_id}.json"

        with open(json_file, "w") as f:
            json.dump(self.analysis_results, f, indent=2, default=str)

        print(f"JSON report saved to: {json_file}")

    def _generate_visualizations(self):
        """Generate security visualization charts"""
        print("Generating security visualizations...")

        try:
            # Set style
            plt.style.use("seaborn-v0_8-darkgrid")
            sns.set_palette("husl")

            # 1. Overall Security Scores Bar Chart
            if self.analysis_results.get("compliance_scores"):
                scores = self.analysis_results["compliance_scores"]
                databases = list(scores.keys())
                overall_scores = [scores[db]["overall_score"] for db in databases]

                fig, ax = plt.subplots(figsize=(10, 6))
                bars = ax.bar(
                    databases, overall_scores, color=["#4CAF50", "#2196F3", "#FF9800"]
                )

                # Add value labels
                for bar, score in zip(bars, overall_scores):
                    height = bar.get_height()
                    ax.text(
                        bar.get_x() + bar.get_width() / 2.0,
                        height + 1,
                        f"{score:.1f}",
                        ha="center",
                        va="bottom",
                    )

                ax.set_ylabel("Security Score (0-100)")
                ax.set_title("Overall Database Security Scores")
                ax.set_ylim(0, 105)

                plt.tight_layout()
                plt.savefig(
                    self.output_dir / f"overall_security_scores_{self.run_id}.png",
                    dpi=300,
                )
                plt.close()

            # 2. Compliance Radar Chart
            self._generate_radar_chart()

            # 3. Vulnerability Severity Chart
            self._generate_vulnerability_chart()

        except Exception as e:
            print(f"  Error generating visualizations: {e}")

    def _generate_radar_chart(self):
        """Generate radar chart for compliance scores"""
        try:
            import plotly.graph_objects as go

            fig = go.Figure()

            for db, assessment in self.analysis_results["database_assessments"].items():
                compliance = assessment.get("compliance_status", {})
                if compliance:
                    frameworks = list(compliance.keys())
                    scores = [compliance[f]["score"] for f in frameworks]

                    # Add trace
                    fig.add_trace(
                        go.Scatterpolar(
                            r=scores + [scores[0]],  # Close the polygon
                            theta=frameworks + [frameworks[0]],
                            fill="toself",
                            name=db.capitalize(),
                        )
                    )

            fig.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
                showlegend=True,
                title="Compliance Framework Scores",
            )

            fig.write_html(
                str(self.output_dir / f"compliance_radar_{self.run_id}.html")
            )
            fig.write_image(
                str(self.output_dir / f"compliance_radar_{self.run_id}.png")
            )

        except ImportError:
            print("  Plotly not available, skipping radar chart")
        except Exception as e:
            logger.exception(f"Error generating radar chart: {e}")

    def _generate_vulnerability_chart(self):
        """Generate vulnerability severity chart"""
        try:
            vulnerabilities = self.analysis_results.get("vulnerability_analysis", {})

            data = []
            for db, vulns in vulnerabilities.items():
                for vuln in vulns:
                    if "severity_score" in vuln:
                        data.append(
                            {
                                "Database": db.capitalize(),
                                "Vulnerability": vuln["id"],
                                "Severity": vuln["severity_score"],
                                "Type": vuln["type"],
                            }
                        )

            if data:
                df = pd.DataFrame(data)

                fig, ax = plt.subplots(figsize=(12, 6))
                ax.scatter(
                    df["Database"],
                    df["Severity"],
                    c=pd.factorize(df["Type"])[0],
                    s=df["Severity"] * 50,
                    alpha=0.6,
                    cmap="RdYlGn_r",
                )

                ax.set_xlabel("Database")
                ax.set_ylabel("Severity Score (0-10)")
                ax.set_title("Vulnerability Severity Analysis")
                ax.grid(True, alpha=0.3)

                # Create legend for vulnerability types
                types = df["Type"].unique()
                for t in types:
                    ax.scatter([], [], label=t, alpha=0.6)
                ax.legend(title="Vulnerability Type")

                plt.tight_layout()
                plt.savefig(
                    self.output_dir / f"vulnerability_analysis_{self.run_id}.png",
                    dpi=300,
                )
                plt.close()

        except Exception as e:
            print(f"  Error generating vulnerability chart: {e}")

    def _generate_compliance_matrix(self):
        """Generate compliance matrix CSV"""
        try:
            matrix_data = []

            for db, assessment in self.analysis_results["database_assessments"].items():
                compliance = assessment.get("compliance_status", {})
                for framework, status in compliance.items():
                    matrix_data.append(
                        {
                            "Database": db.capitalize(),
                            "Framework": status["framework"],
                            "Score": status["score"],
                            "Status": status["status"],
                        }
                    )

            if matrix_data:
                df = pd.DataFrame(matrix_data)
                csv_file = self.output_dir / f"compliance_matrix_{self.run_id}.csv"
                df.to_csv(csv_file, index=False)
                print(f"Compliance matrix saved to: {csv_file}")

        except Exception as e:
            print(f"  Error generating compliance matrix: {e}")

    def run(self):
        """Run complete security analysis"""
        print("=" * 80)
        print("SECURITY ANALYSIS - Health IoT Database Benchmarking")
        print("=" * 80)

        # Analyze each database
        self.analyze_postgresql_security()
        self.analyze_influxdb_security()
        self.analyze_mongodb_security()

        # Perform vulnerability analysis
        self.perform_vulnerability_analysis()

        # Calculate overall scores
        self.calculate_overall_security_scores()

        # Generate recommendations
        self.generate_recommendations()

        # Generate reports and visualizations
        self.generate_reports()

        print("\n" + "=" * 80)
        print("SECURITY ANALYSIS COMPLETED SUCCESSFULLY")
        print("=" * 80)

        # Print summary
        print("\n📊 SECURITY ANALYSIS SUMMARY:")
        if self.analysis_results.get("compliance_scores"):
            for db, scores in self.analysis_results["compliance_scores"].items():
                print(
                    f"   {db.upper()}: {scores['overall_score']}/100 ({scores['security_grade']})"
                )

        return self.analysis_results


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Security analysis for database benchmarking"
    )
    parser.add_argument("--run-id", help="Run ID for this analysis session")
    parser.add_argument("--base-dir", help="Base directory of the project")

    args = parser.parse_args()

    try:
        analyzer = SecurityAnalyzer(base_dir=args.base_dir, run_id=args.run_id)

        analyzer.run()

        return 0

    except Exception as e:
        print(f"Error during security analysis: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
