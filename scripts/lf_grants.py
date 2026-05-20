"""Idempotent Lake Formation grants for all OTT-managed Glue tables.

Background: every CFN deploy in this audit that touched a table managed by
LF column-level RBAC failed with `Insufficient Lake Formation permission(s):
Required Alter on <table>`. The fix each time was a manual
`lakeformation:grant-permissions ALL` to the deploying principal.

This script pre-emptively grants ALL to:
  - terraform-admin (the human admin who runs ad-hoc deploys)
  - sdlf-ott-cicd-codebuild (the role CodeBuild assumes during CI/CD)

on every OTT-managed table in both the analytics and gold databases.

Idempotent: re-running is a no-op (LF treats duplicate grants as success).
Run after any time a new table is created — or as a one-off cleanup like now.

Usage:
  python scripts/lf_grants.py            # dry-run, prints what it would grant
  python scripts/lf_grants.py --apply    # actually call grant-permissions
"""
import argparse
import sys
import io
import boto3
from botocore.exceptions import ClientError

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
CATALOG_ID = "703668403514"

DATABASES = [
    "fpt_ott_searchevents_analytics",
    "fpt_ott_searchevents_gold",
]

PRINCIPALS = [
    f"arn:aws:iam::{CATALOG_ID}:user/terraform-admin",
    f"arn:aws:iam::{CATALOG_ID}:role/sdlf-ott/sdlf-ott-cicd-codebuild",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually issue grants (default is dry-run)")
    args = parser.parse_args()

    glue = boto3.client("glue", region_name=REGION)
    lf = boto3.client("lakeformation", region_name=REGION)

    granted = 0
    skipped = 0
    failed = 0

    for db in DATABASES:
        try:
            tables = glue.get_tables(DatabaseName=db, CatalogId=CATALOG_ID)["TableList"]
        except ClientError as e:
            print(f"[{db}] ERROR listing tables: {e.response['Error']['Code']}")
            continue
        print(f"\n=== {db} ({len(tables)} tables) ===")
        for t in tables:
            tname = t["Name"]
            for principal in PRINCIPALS:
                short = principal.rsplit("/", 1)[-1]
                if not args.apply:
                    print(f"  [DRY] would grant ALL on {tname} to {short}")
                    skipped += 1
                    continue
                try:
                    lf.grant_permissions(
                        Principal={"DataLakePrincipalIdentifier": principal},
                        Resource={"Table": {"CatalogId": CATALOG_ID, "DatabaseName": db, "Name": tname}},
                        Permissions=["ALL"],
                        PermissionsWithGrantOption=["ALL"],
                    )
                    print(f"  OK   ALL on {tname} -> {short}")
                    granted += 1
                except ClientError as e:
                    code = e.response["Error"]["Code"]
                    if code in ("AlreadyExistsException",):
                        print(f"  IDEM ALL on {tname} -> {short} (already granted)")
                        granted += 1
                    else:
                        print(f"  FAIL ALL on {tname} -> {short}: {code}")
                        failed += 1

    print()
    if args.apply:
        print(f"Summary: granted={granted} failed={failed}")
        sys.exit(0 if failed == 0 else 1)
    else:
        print(f"Dry-run: would attempt {skipped} grants. Re-run with --apply to execute.")
        sys.exit(0)


if __name__ == "__main__":
    main()
