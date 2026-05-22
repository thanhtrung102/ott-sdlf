"""Activate Lake Formation column-level RBAC on the curated table.

Why this script exists:
  `AWS::LakeFormation::PrincipalPermissions` with `TableWithColumns` is documented
  as the way to declare column-excluded grants, but when IAM_ALLOWED_PRINCIPALS is
  still active on the table, CFN reports CREATE_COMPLETE while the underlying LF
  grant never lands. Result: ListPermissions(ResourceType=TABLE_WITH_COLUMNS)
  returns 0 even though CFN says 7 resources exist.

What it does:
  1. Imperative `grant_permissions` for each of the 5 ott-main* Lambda/DQ roles,
     the Crawler role, and the Glue ETL role — using the exact column exclusions
     declared in pipeline-ott-lakeformation.yaml.
  2. Revokes IAM_ALLOWED_PRINCIPALS on curated so the explicit grants enforce.

Idempotent. Dry-run by default; --apply commits.

Run from a Lake Formation administrator principal.
"""
from __future__ import annotations

import argparse
import sys

import boto3
from botocore.exceptions import ClientError

REGION = "ap-southeast-1"

SSM_DB = "/sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents"
TABLE = "curated"

ROLE_SSM_PATHS = {
    "LutRefresh":  "/sdlf/pipeline/rRole/ott-mainLUT",
    "Trending":    "/sdlf/pipeline/rRole/ott-mainTR",
    "DQExec":      "/sdlf/pipeline/rRole/ott-mainDQExec",
    "DQGlue":      "/sdlf/pipeline/rRole/ott-mainDQGlue",
}

COLUMN_EXCLUDES = {
    "LutRefresh":  ["user_id_hashed", "search_session_id", "has_premium", "subscription_count"],
    "Trending":    [],
    "DQExec":      [],
    "DQGlue":      [],
}


def grant_table_with_columns(lf, account, db, principal_arn, exclude_cols, dry):
    resource = {
        "TableWithColumns": {
            "CatalogId": account,
            "DatabaseName": db,
            "Name": TABLE,
            "ColumnWildcard": {"ExcludedColumnNames": exclude_cols} if exclude_cols else {},
        }
    }
    if dry:
        return "DRY"
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource=resource,
            Permissions=["SELECT"],
        )
        return "OK"
    except ClientError as e:
        return f"FAIL {e.response['Error']['Code']}: {e.response['Error']['Message']}"


def grant_table(lf, account, db, principal_arn, perms, dry):
    if dry:
        return "DRY"
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Table": {"CatalogId": account, "DatabaseName": db, "Name": TABLE}},
            Permissions=perms,
        )
        return "OK"
    except ClientError as e:
        return f"FAIL {e.response['Error']['Code']}: {e.response['Error']['Message']}"


def revoke_iam_allowed(lf, account, db, dry):
    if dry:
        return "DRY"
    try:
        lf.revoke_permissions(
            Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
            Resource={"Table": {"CatalogId": account, "DatabaseName": db, "Name": TABLE}},
            Permissions=["ALL"],
        )
        return "OK"
    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"].lower()
        # IAM_ALLOWED_PRINCIPALS already revoked: revoking again raises
        # InvalidInputException. AWS phrases this as "No permissions revoked.
        # Grantee does not have:[ALL]" (and historically "...does not exist").
        # Either way the table is already enforced — that is success, not FAIL.
        if code == "InvalidInputException" and (
            "does not have" in msg
            or "does not exist" in msg
            or "no permissions revoked" in msg
        ):
            return "ALREADY_REVOKED"
        return f"FAIL {code}: {e.response['Error']['Message']}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually issue grants/revokes (default is dry-run)")
    args = parser.parse_args()
    dry = not args.apply

    sess = boto3.session.Session(region_name=REGION)
    lf = sess.client("lakeformation")
    ssm = sess.client("ssm")
    sts = sess.client("sts")
    glue = sess.client("glue")

    account = sts.get_caller_identity()["Account"]
    db = ssm.get_parameter(Name=SSM_DB)["Parameter"]["Value"]

    crawler_role = ssm.get_parameter(Name="/sdlf/dataset/rDatalakeCrawlerRole/searchevents")["Parameter"]["Value"]
    glue_role_arn = f"arn:aws:iam::{account}:role/service-role/sdlf-ott-searchevents-glue-role"
    try:
        glue.get_role  # noqa: B018  -- not a callable check, just import
    except Exception:
        pass

    mode = "DRY-RUN" if dry else "APPLY"
    print(f"=== Activate LF column-level RBAC on {db}.{TABLE} ({mode}) ===")
    print()

    print("Step 1: Grant ott-main* roles with column exclusions")
    for short, ssm_path in ROLE_SSM_PATHS.items():
        try:
            arn = ssm.get_parameter(Name=ssm_path)["Parameter"]["Value"]
        except ClientError as e:
            print(f"  SKIP  {short}: SSM lookup failed: {e.response['Error']['Code']}")
            continue
        exc = COLUMN_EXCLUDES[short]
        status = grant_table_with_columns(lf, account, db, arn, exc, dry)
        kind = f"excludes {exc}" if exc else "all columns"
        print(f"  {status:6s}  {short:12s}  ->  {kind}")

    print()
    print("Step 2: Grant Glue ETL role (SELECT/INSERT/ALTER, all columns)")
    status = grant_table_with_columns(lf, account, db, glue_role_arn, [], dry)
    print(f"  {status:6s}  GlueETL      ->  SELECT all columns ({glue_role_arn.split('/')[-1]})")
    if not dry:
        for perm in ("INSERT", "ALTER"):
            try:
                lf.grant_permissions(
                    Principal={"DataLakePrincipalIdentifier": glue_role_arn},
                    Resource={"Table": {"CatalogId": account, "DatabaseName": db, "Name": TABLE}},
                    Permissions=[perm],
                )
                print(f"  OK      GlueETL      ->  {perm} (table-level)")
            except ClientError as e:
                print(f"  FAIL    GlueETL      ->  {perm}: {e.response['Error']['Code']}")

    print()
    print("Step 3: Grant Crawler role (table-level ALTER/DESCRIBE/INSERT)")
    status = grant_table(lf, account, db, crawler_role, ["ALTER", "DESCRIBE", "INSERT"], dry)
    print(f"  {status:6s}  Crawler      ->  ALTER+DESCRIBE+INSERT  ({crawler_role.split('/')[-1]})")

    print()
    print("Step 4: Revoke IAM_ALLOWED_PRINCIPALS to activate enforcement")
    status = revoke_iam_allowed(lf, account, db, dry)
    print(f"  {status:6s}  IAM_ALLOWED_PRINCIPALS  ->  ALL  (revoked)")

    print()
    if dry:
        print("Dry-run complete. Re-run with --apply to commit.")
        return 0

    print("=== Post-state verification ===")
    by_principal: dict[str, list[str]] = {}
    next_token = None
    while True:
        kw = {}
        if next_token:
            kw["NextToken"] = next_token
        resp = lf.list_permissions(**kw)
        for g in resp.get("PrincipalResourcePermissions", []):
            res = g["Resource"]
            if "TableWithColumns" in res and res["TableWithColumns"].get("Name") == TABLE:
                p = g["Principal"]["DataLakePrincipalIdentifier"]
                wc = res["TableWithColumns"].get("ColumnWildcard", {})
                exc = wc.get("ExcludedColumnNames", [])
                short = p.split("/")[-1][:40]
                by_principal.setdefault(short, []).append(
                    f"perms={g['Permissions']} excludes={exc if exc else 'NONE'}"
                )
        next_token = resp.get("NextToken")
        if not next_token:
            break
    print(f"  TableWithColumns grants now visible: {len(by_principal)}")
    for short, grants in by_principal.items():
        for g in grants:
            print(f"    {short[:50]:52s}  {g}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
