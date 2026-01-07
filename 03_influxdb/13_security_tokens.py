#!/usr/bin/env python3
"""
InfluxDB Security Token Management
Manage authentication tokens and security configurations
"""

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from influxdb_client import InfluxDBClient
from influxdb_client.domain.authorization import Authorization
from influxdb_client.domain.authorization_update_request import (
    AuthorizationUpdateRequest,
)
from influxdb_client.domain.permission import Permission
from influxdb_client.domain.permission_resource import PermissionResource


@dataclass
class TokenInfo:
    """Container for token information"""

    id: str
    description: str
    permissions: List[str]
    created_at: datetime
    updated_at: datetime
    status: str
    user_name: str
    user_id: str
    org_name: str
    org_id: str


class SecurityTokenManager:
    """Manager for InfluxDB security tokens and permissions"""

    def __init__(self, url: str, token: str, org: str):
        """Initialize InfluxDB client"""
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.org = org

    def list_tokens(self, user_id: Optional[str] = None) -> List[TokenInfo]:
        """List all authentication tokens"""

        tokens = []

        try:
            # Get all authorizations
            auths_api = self.client.authorizations_api()
            authorizations = auths_api.find_authorizations(
                org=self.org, user_id=user_id
            )

            for auth in authorizations:
                # Extract permissions
                permissions = []
                for perm in auth.permissions:
                    action = (
                        perm.action.value
                        if hasattr(perm.action, "value")
                        else str(perm.action)
                    )
                    resource_type = (
                        perm.resource.resource_type.value
                        if hasattr(perm.resource.resource_type, "value")
                        else str(perm.resource.resource_type)
                    )

                    if perm.resource.name:
                        permissions.append(
                            f"{action} {resource_type} '{perm.resource.name}'"
                        )
                    elif perm.resource.id:
                        permissions.append(
                            f"{action} {resource_type} with id '{perm.resource.id}'"
                        )
                    else:
                        permissions.append(f"{action} {resource_type}")

                # Create token info
                token_info = TokenInfo(
                    id=auth.id,
                    description=auth.description or "No description",
                    permissions=permissions,
                    created_at=auth.created_at,
                    updated_at=auth.updated_at,
                    status="active" if auth.status == "active" else "inactive",
                    user_name=auth.user if hasattr(auth, "user") else "Unknown",
                    user_id=auth.user_id,
                    org_name=auth.org if hasattr(auth, "org") else self.org,
                    org_id=auth.org_id,
                )

                tokens.append(token_info)

        except Exception as e:
            print(f"Error listing tokens: {e}")

        return tokens

    def create_token(
        self, description: str, permissions: List[Dict], user_id: Optional[str] = None
    ) -> Optional[TokenInfo]:
        """Create a new authentication token"""

        try:
            # Convert permissions to InfluxDB Permission objects
            influx_permissions = []

            for perm in permissions:
                resource = PermissionResource(
                    type=perm["resource_type"],
                    id=perm.get("resource_id"),
                    name=perm.get("resource_name"),
                    org_id=perm.get("org_id"),
                    org=perm.get("org_name"),
                )

                permission = Permission(action=perm["action"], resource=resource)

                influx_permissions.append(permission)

            # Create authorization
            auths_api = self.client.authorizations_api()
            authorization = Authorization(
                org_id=self.get_org_id(),
                user_id=user_id,
                description=description,
                permissions=influx_permissions,
                status="active",
            )

            created_auth = auths_api.create_authorization(authorization=authorization)

            # Get the token (only available immediately after creation)
            if hasattr(created_auth, "token"):
                token_value = created_auth.token
                print("\nToken created successfully!")
                print(f"Token: {token_value}")
                print("IMPORTANT: Save this token now. It won't be shown again.")

            # Get token info
            token_info = self.get_token_info(created_auth.id)

            return token_info

        except Exception as e:
            print(f"Error creating token: {e}")
            return None

    def get_token_info(self, token_id: str) -> Optional[TokenInfo]:
        """Get detailed information about a specific token"""

        try:
            auths_api = self.client.authorizations_api()
            auth = auths_api.find_authorization_by_id(auth_id=token_id)

            # Extract permissions
            permissions = []
            for perm in auth.permissions:
                action = (
                    perm.action.value
                    if hasattr(perm.action, "value")
                    else str(perm.action)
                )
                resource_type = (
                    perm.resource.resource_type.value
                    if hasattr(perm.resource.resource_type, "value")
                    else str(perm.resource.resource_type)
                )

                if perm.resource.name:
                    permissions.append(
                        f"{action} {resource_type} '{perm.resource.name}'"
                    )
                elif perm.resource.id:
                    permissions.append(
                        f"{action} {resource_type} with id '{perm.resource.id}'"
                    )
                else:
                    permissions.append(f"{action} {resource_type}")

            # Create token info
            token_info = TokenInfo(
                id=auth.id,
                description=auth.description or "No description",
                permissions=permissions,
                created_at=auth.created_at,
                updated_at=auth.updated_at,
                status="active" if auth.status == "active" else "inactive",
                user_name=auth.user if hasattr(auth, "user") else "Unknown",
                user_id=auth.user_id,
                org_name=auth.org if hasattr(auth, "org") else self.org,
                org_id=auth.org_id,
            )

            return token_info

        except Exception as e:
            print(f"Error getting token info: {e}")
            return None

    def update_token(
        self,
        token_id: str,
        description: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Optional[TokenInfo]:
        """Update an existing token"""

        try:
            auths_api = self.client.authorizations_api()

            # Create update request
            update_request = AuthorizationUpdateRequest(
                description=description, status=status
            )

            # Update authorization
            auths_api.update_authorization(
                auth_id=token_id, authorization_update_request=update_request
            )

            print("Token updated successfully")

            # Get updated token info
            token_info = self.get_token_info(token_id)

            return token_info

        except Exception as e:
            print(f"Error updating token: {e}")
            return None

    def delete_token(self, token_id: str) -> bool:
        """Delete an authentication token"""

        try:
            auths_api = self.client.authorizations_api()
            auths_api.delete_authorization(auth_id=token_id)

            print("Token deleted successfully")
            return True

        except Exception as e:
            print(f"Error deleting token: {e}")
            return False

    def get_org_id(self) -> str:
        """Get organization ID"""

        try:
            orgs_api = self.client.organizations_api()
            orgs = orgs_api.find_organizations(org=self.org)

            if orgs:
                return orgs[0].id
            else:
                raise ValueError(f"Organization '{self.org}' not found")

        except Exception as e:
            print(f"Error getting org ID: {e}")
            raise

    def get_bucket_id(self, bucket_name: str) -> str:
        """Get bucket ID by name"""

        try:
            buckets_api = self.client.buckets_api()
            bucket = buckets_api.find_bucket_by_name(bucket_name=bucket_name)

            if bucket:
                return bucket.id
            else:
                raise ValueError(f"Bucket '{bucket_name}' not found")

        except Exception as e:
            print(f"Error getting bucket ID: {e}")
            raise

    def create_health_iot_tokens(self) -> Dict[str, TokenInfo]:
        """Create standard tokens for Health IoT application"""

        print("Creating Health IoT standard tokens...")

        _ = self.get_org_id()

        # Define standard tokens
        token_definitions = [
            {
                "name": "health_iot_admin",
                "description": "Full access token for Health IoT administrators",
                "permissions": [
                    {"action": "read", "resource_type": "authorizations"},
                    {"action": "write", "resource_type": "authorizations"},
                    {"action": "read", "resource_type": "buckets"},
                    {"action": "write", "resource_type": "buckets"},
                    {"action": "read", "resource_type": "dashboards"},
                    {"action": "write", "resource_type": "dashboards"},
                    {"action": "read", "resource_type": "sources"},
                    {"action": "write", "resource_type": "sources"},
                    {"action": "read", "resource_type": "tasks"},
                    {"action": "write", "resource_type": "tasks"},
                    {"action": "read", "resource_type": "telegrafs"},
                    {"action": "write", "resource_type": "telegrafs"},
                    {"action": "read", "resource_type": "users"},
                    {"action": "write", "resource_type": "users"},
                    {"action": "read", "resource_type": "variables"},
                    {"action": "write", "resource_type": "variables"},
                    {"action": "read", "resource_type": "scrapers"},
                    {"action": "write", "resource_type": "scrapers"},
                    {"action": "read", "resource_type": "secrets"},
                    {"action": "write", "resource_type": "secrets"},
                    {"action": "read", "resource_type": "labels"},
                    {"action": "write", "resource_type": "labels"},
                    {"action": "read", "resource_type": "views"},
                    {"action": "write", "resource_type": "views"},
                    {"action": "read", "resource_type": "documents"},
                    {"action": "write", "resource_type": "documents"},
                    {"action": "read", "resource_type": "notificationRules"},
                    {"action": "write", "resource_type": "notificationRules"},
                    {"action": "read", "resource_type": "notificationEndpoints"},
                    {"action": "write", "resource_type": "notificationEndpoints"},
                    {"action": "read", "resource_type": "checks"},
                    {"action": "write", "resource_type": "checks"},
                    {"action": "read", "resource_type": "dbrp"},
                    {"action": "write", "resource_type": "dbrp"},
                    {"action": "read", "resource_type": "notebooks"},
                    {"action": "write", "resource_type": "notebooks"},
                    {"action": "read", "resource_type": "annotations"},
                    {"action": "write", "resource_type": "annotations"},
                    {"action": "read", "resource_type": "remotes"},
                    {"action": "write", "resource_type": "remotes"},
                    {"action": "read", "resource_type": "replications"},
                    {"action": "write", "resource_type": "replications"},
                ],
            },
            {
                "name": "health_iot_write",
                "description": "Write-only token for data ingestion",
                "permissions": [
                    {
                        "action": "write",
                        "resource_type": "buckets",
                        "resource_name": "health_iot_metrics",
                    },
                    {
                        "action": "write",
                        "resource_type": "buckets",
                        "resource_name": "raw_vitals",
                    },
                    {
                        "action": "write",
                        "resource_type": "buckets",
                        "resource_name": "alerts",
                    },
                ],
            },
            {
                "name": "health_iot_read",
                "description": "Read-only token for dashboard and queries",
                "permissions": [
                    {
                        "action": "read",
                        "resource_type": "buckets",
                        "resource_name": "health_iot_metrics",
                    },
                    {
                        "action": "read",
                        "resource_type": "buckets",
                        "resource_name": "aggregated_stats",
                    },
                    {
                        "action": "read",
                        "resource_type": "buckets",
                        "resource_name": "alerts",
                    },
                    {"action": "read", "resource_type": "dashboards"},
                    {"action": "read", "resource_type": "tasks"},
                ],
            },
            {
                "name": "health_iot_alert",
                "description": "Token for alert processing and notifications",
                "permissions": [
                    {
                        "action": "read",
                        "resource_type": "buckets",
                        "resource_name": "health_iot_metrics",
                    },
                    {
                        "action": "write",
                        "resource_type": "buckets",
                        "resource_name": "alerts",
                    },
                    {"action": "write", "resource_type": "notificationEndpoints"},
                    {"action": "read", "resource_type": "checks"},
                    {"action": "write", "resource_type": "checks"},
                ],
            },
            {
                "name": "health_iot_backup",
                "description": "Token for backup and maintenance operations",
                "permissions": [
                    {"action": "read", "resource_type": "buckets"},
                    {"action": "read", "resource_type": "tasks"},
                    {
                        "action": "write",
                        "resource_type": "buckets",
                        "resource_name": "benchmark_results",
                    },
                ],
            },
        ]

        created_tokens = {}

        for token_def in token_definitions:
            print(f"\nCreating token: {token_def['name']}")
            print(f"Description: {token_def['description']}")

            token_info = self.create_token(
                description=token_def["description"],
                permissions=token_def["permissions"],
            )

            if token_info:
                created_tokens[token_def["name"]] = token_info
                print(f"  ✓ Created successfully (ID: {token_info.id})")
            else:
                print("  ✗ Failed to create token")

        return created_tokens

    def audit_tokens(self) -> Dict:
        """Audit all tokens for security compliance"""

        print("Auditing security tokens...")

        tokens = self.list_tokens()

        audit_results = {
            "total_tokens": len(tokens),
            "active_tokens": 0,
            "inactive_tokens": 0,
            "tokens_without_description": 0,
            "admin_tokens": 0,
            "write_tokens": 0,
            "read_tokens": 0,
            "recommendations": [],
            "security_issues": [],
        }

        for token in tokens:
            # Count active/inactive
            if token.status == "active":
                audit_results["active_tokens"] += 1
            else:
                audit_results["inactive_tokens"] += 1

            # Check for missing descriptions
            if not token.description or token.description == "No description":
                audit_results["tokens_without_description"] += 1
                audit_results["security_issues"].append(
                    {
                        "token_id": token.id,
                        "issue": "Missing description",
                        "severity": "low",
                    }
                )

            # Analyze permissions
            is_admin = False
            can_write = False
            can_read = False

            for perm in token.permissions:
                if "write" in perm.lower() and "authorizations" in perm.lower():
                    is_admin = True
                if "write" in perm.lower():
                    can_write = True
                if "read" in perm.lower():
                    can_read = True

            if is_admin:
                audit_results["admin_tokens"] += 1

                # Check if admin token has limited use
                if "health_iot" not in token.description.lower():
                    audit_results["security_issues"].append(
                        {
                            "token_id": token.id,
                            "issue": "Admin token without proper description",
                            "severity": "high",
                        }
                    )

            if can_write:
                audit_results["write_tokens"] += 1

            if can_read:
                audit_results["read_tokens"] += 1

        # Generate recommendations
        if audit_results["tokens_without_description"] > 0:
            audit_results["recommendations"].append(
                {
                    "issue": "Tokens without descriptions",
                    "recommendation": "Add descriptions to all tokens for better audit trail",
                    "priority": "medium",
                }
            )

        if audit_results["admin_tokens"] > 3:
            audit_results["recommendations"].append(
                {
                    "issue": "Too many admin tokens",
                    "recommendation": "Review and reduce the number of admin tokens",
                    "priority": "high",
                }
            )

        if audit_results["inactive_tokens"] > 0:
            audit_results["recommendations"].append(
                {
                    "issue": "Inactive tokens present",
                    "recommendation": "Consider deleting inactive tokens",
                    "priority": "low",
                }
            )

        return audit_results

    def rotate_tokens(self, token_ids: List[str]) -> Dict:
        """Rotate (recreate) specified tokens"""

        print(f"Rotating {len(token_ids)} tokens...")

        results = {"successful": [], "failed": []}

        for token_id in token_ids:
            try:
                # Get token info
                token_info = self.get_token_info(token_id)

                if not token_info:
                    results["failed"].append(
                        {"token_id": token_id, "error": "Token not found"}
                    )
                    continue

                print(f"\nRotating token: {token_info.description}")

                # Extract permissions
                permissions = []
                for perm_str in token_info.permissions:
                    # Parse permission string back to dictionary
                    # This is simplified - in production you'd want better parsing
                    if "write" in perm_str and "read" in perm_str:
                        action = "write"
                    elif "write" in perm_str:
                        action = "write"
                    elif "read" in perm_str:
                        action = "read"
                    else:
                        action = "read"  # default

                    # Extract resource type
                    resource_parts = perm_str.split()
                    resource_type = (
                        resource_parts[1] if len(resource_parts) > 1 else "buckets"
                    )

                    permissions.append(
                        {"action": action, "resource_type": resource_type}
                    )

                # Deactivate old token
                self.update_token(token_id, status="inactive")

                # Create new token with same permissions
                new_token_info = self.create_token(
                    description=f"{token_info.description} (Rotated {datetime.now().date()})",
                    permissions=permissions,
                    user_id=token_info.user_id,
                )

                if new_token_info:
                    results["successful"].append(
                        {
                            "old_token_id": token_id,
                            "new_token_id": new_token_info.id,
                            "description": new_token_info.description,
                        }
                    )
                    print("  ✓ Token rotated successfully")
                else:
                    results["failed"].append(
                        {"token_id": token_id, "error": "Failed to create new token"}
                    )
                    print("  ✗ Failed to rotate token")

            except Exception as e:
                results["failed"].append({"token_id": token_id, "error": str(e)})
                print(f"  ✗ Error rotating token: {e}")

        return results

    def export_tokens(self, output_file: str) -> bool:
        """Export token information to JSON file (without actual token values)"""

        try:
            tokens = self.list_tokens()

            export_data = []
            for token in tokens:
                export_data.append(
                    {
                        "id": token.id,
                        "description": token.description,
                        "permissions": token.permissions,
                        "created_at": token.created_at.isoformat(),
                        "updated_at": token.updated_at.isoformat(),
                        "status": token.status,
                        "user_name": token.user_name,
                        "user_id": token.user_id,
                        "org_name": token.org_name,
                        "org_id": token.org_id,
                    }
                )

            with open(output_file, "w") as f:
                json.dump(export_data, f, indent=2)

            print(f"Exported {len(tokens)} tokens to {output_file}")
            return True

        except Exception as e:
            print(f"Error exporting tokens: {e}")
            return False

    def print_token_summary(self, tokens: List[TokenInfo]):
        """Print summary of tokens"""

        print("\n" + "=" * 80)
        print("TOKEN SUMMARY")
        print("=" * 80)

        if not tokens:
            print("No tokens found")
            return

        print(f"Total tokens: {len(tokens)}")
        print(f"Active tokens: {sum(1 for t in tokens if t.status == 'active')}")
        print(f"Inactive tokens: {sum(1 for t in tokens if t.status == 'inactive')}")

        print("\nToken Details:")
        print("-" * 80)

        for token in tokens:
            print(f"\nID: {token.id}")
            print(f"Description: {token.description}")
            print(f"Status: {token.status}")
            print(f"User: {token.user_name}")
            print(f"Created: {token.created_at}")
            print(f"Permissions ({len(token.permissions)}):")
            for perm in token.permissions[:3]:  # Show first 3 permissions
                print(f"  • {perm}")
            if len(token.permissions) > 3:
                print(f"  ... and {len(token.permissions) - 3} more")


def main():
    parser = argparse.ArgumentParser(description="InfluxDB Security Token Management")

    # Connection parameters
    parser.add_argument("--url", default="http://localhost:8086", help="InfluxDB URL")
    parser.add_argument(
        "--token", required=True, help="InfluxDB authentication token (admin)"
    )
    parser.add_argument("--org", default="HealthIoT", help="Organization name")

    # Actions
    parser.add_argument(
        "--action",
        required=True,
        choices=[
            "list",
            "create",
            "update",
            "delete",
            "info",
            "setup",
            "audit",
            "rotate",
            "export",
        ],
        help="Action to perform",
    )

    # Token parameters
    parser.add_argument("--token-id", help="Token ID (for specific actions)")
    parser.add_argument("--description", help="Token description")
    parser.add_argument("--status", choices=["active", "inactive"], help="Token status")

    # File parameters
    parser.add_argument("--output-file", help="Output file for export")
    parser.add_argument("--input-file", help="Input file with token definitions")

    # Setup parameters
    parser.add_argument(
        "--setup-type",
        default="health_iot",
        choices=["health_iot", "custom"],
        help="Type of token setup to create",
    )

    args = parser.parse_args()

    # Initialize manager
    manager = SecurityTokenManager(url=args.url, token=args.token, org=args.org)

    try:
        if args.action == "list":
            # List all tokens
            tokens = manager.list_tokens()
            manager.print_token_summary(tokens)

        elif args.action == "info":
            # Get token info
            if not args.token_id:
                print("Error: --token-id required for info action")
                sys.exit(1)

            token_info = manager.get_token_info(args.token_id)
            if token_info:
                print("\nToken Information:")
                print("-" * 40)
                print(f"ID: {token_info.id}")
                print(f"Description: {token_info.description}")
                print(f"Status: {token_info.status}")
                print(f"User: {token_info.user_name}")
                print(f"Created: {token_info.created_at}")
                print(f"Updated: {token_info.updated_at}")
                print(f"Organization: {token_info.org_name}")
                print("\nPermissions:")
                for perm in token_info.permissions:
                    print(f"  • {perm}")
            else:
                print(f"Token not found: {args.token_id}")

        elif args.action == "create":
            # Create new token
            if not args.description:
                print("Error: --description required for create action")
                sys.exit(1)

            # For now, create a read/write token for health_iot_metrics bucket
            org_id = manager.get_org_id()
            bucket_id = manager.get_bucket_id("health_iot_metrics")

            permissions = [
                {
                    "action": "read",
                    "resource_type": "buckets",
                    "resource_id": bucket_id,
                    "org_id": org_id,
                },
                {
                    "action": "write",
                    "resource_type": "buckets",
                    "resource_id": bucket_id,
                    "org_id": org_id,
                },
            ]

            token_info = manager.create_token(
                description=args.description, permissions=permissions
            )

            if token_info:
                print("\nToken created successfully!")
                print(f"ID: {token_info.id}")
                print(f"Description: {token_info.description}")

        elif args.action == "update":
            # Update token
            if not args.token_id:
                print("Error: --token-id required for update action")
                sys.exit(1)

            token_info = manager.update_token(
                token_id=args.token_id, description=args.description, status=args.status
            )

            if token_info:
                print("\nToken updated successfully!")
                print(f"ID: {token_info.id}")
                print(f"New status: {token_info.status}")

        elif args.action == "delete":
            # Delete token
            if not args.token_id:
                print("Error: --token-id required for delete action")
                sys.exit(1)

            confirm = input(
                f"Are you sure you want to delete token {args.token_id}? (yes/no): "
            )
            if confirm.lower() == "yes":
                success = manager.delete_token(args.token_id)
                if success:
                    print("Token deleted successfully")
            else:
                print("Deletion cancelled")

        elif args.action == "setup":
            # Setup standard tokens
            if args.setup_type == "health_iot":
                tokens = manager.create_health_iot_tokens()
                print(f"\nCreated {len(tokens)} Health IoT tokens")

        elif args.action == "audit":
            # Audit tokens
            audit_results = manager.audit_tokens()

            print("\n" + "=" * 80)
            print("SECURITY AUDIT RESULTS")
            print("=" * 80)

            print("\nSummary:")
            print(f"  Total tokens: {audit_results['total_tokens']}")
            print(f"  Active tokens: {audit_results['active_tokens']}")
            print(f"  Inactive tokens: {audit_results['inactive_tokens']}")
            print(
                f"  Tokens without description: {audit_results['tokens_without_description']}"
            )
            print(f"  Admin tokens: {audit_results['admin_tokens']}")
            print(f"  Write tokens: {audit_results['write_tokens']}")
            print(f"  Read tokens: {audit_results['read_tokens']}")

            if audit_results["security_issues"]:
                print(f"\nSecurity Issues ({len(audit_results['security_issues'])}):")
                for issue in audit_results["security_issues"]:
                    print(
                        f"  • [{issue['severity'].upper()}] {issue['issue']} (Token: {issue['token_id']})"
                    )

            if audit_results["recommendations"]:
                print("\nRecommendations:")
                for rec in audit_results["recommendations"]:
                    print(
                        f"  • [{rec['priority'].upper()}] {rec['issue']}: {rec['recommendation']}"
                    )

        elif args.action == "rotate":
            # Rotate tokens
            if not args.token_id:
                print("Error: --token-id required for rotate action")
                sys.exit(1)

            results = manager.rotate_tokens([args.token_id])

            print("\nRotation Results:")
            print(f"  Successful: {len(results['successful'])}")
            print(f"  Failed: {len(results['failed'])}")

            if results["failed"]:
                print("\nFailures:")
                for failure in results["failed"]:
                    print(f"  • Token {failure['token_id']}: {failure['error']}")

        elif args.action == "export":
            # Export tokens
            if not args.output_file:
                print("Error: --output-file required for export action")
                sys.exit(1)

            success = manager.export_tokens(args.output_file)
            if success:
                print(f"Tokens exported to {args.output_file}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
