"""Behavior-level verification of monitoring + Lake Formation.

"Deployed" != "works". This script checks:
  Monitoring
    M1. All 11 alarms exist with sdlf-ott prefix.
    M2. Each alarm has at least one alarm action (SNS, etc.).
    M3. The SNS notification topic has at least one subscriber.
    M4. At least one alarm has transitioned to ALARM in the last 30 days
        (proves the metric -> alarm -> state-change pipe works end-to-end).
    M5. CloudWatch dashboard exists.
  Lake Formation
    L1. Curated table has the 5 expected grants.
    L2. IAM_ALLOWED_PRINCIPALS SELECT status (enforcement on or off).
    L3. Each role has the column exclusions documented in the template.

Read-only. Exit 0 on success, 1 if a hard FAIL.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "ap-southeast-1"
SNS_TOPIC_SSM = "/SDLF/SNS/ott/Notifications"
DB_SSM = "/sdlf/dataset/rAnalyticsGlueDataCatalog/searchevents"
CURATED_TABLE = "curated"
DASHBOARD_NAME = "sdlf-ott-searchevents-pipeline"

EXPECTED_LF_GRANTS = {
    "lutrefresh":   {"user_id_hashed", "search_session_id", "has_premium", "subscription_count"},
    "trending":     set(),
    "rDQExecu":     set(),
    "rGlueDQRol":   set(),
    "searchevents-glue-role": set(),
}

results: list[tuple[str, str, str]] = []


def record(level: str, msg: str, detail: str = "") -> None:
    results.append((level, msg, detail))
    glyph = {"PASS": "OK ", "WARN": "!! ", "FAIL": "XX "}[level]
    print(f"  {glyph}{msg}" + (f"  ({detail})" if detail else ""))


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def get_ssm(ssm, name: str) -> str:
    return ssm.get_parameter(Name=name)["Parameter"]["Value"]


def verify_monitoring(session) -> None:
    cw = session.client("cloudwatch")
    sns = session.client("sns")
    ssm = session.client("ssm")

    section("MONITORING")

    # M1 alarms exist
    alarms = cw.describe_alarms(AlarmNamePrefix="sdlf-ott")["MetricAlarms"]
    if len(alarms) >= 11:
        record("PASS", "M1 alarm count", f"{len(alarms)} alarms (expected >=11)")
    else:
        record("FAIL", "M1 alarm count", f"{len(alarms)} alarms (expected >=11)")

    # M2 every alarm has actions
    no_actions = [a["AlarmName"] for a in alarms if not a.get("AlarmActions")]
    if not no_actions:
        record("PASS", "M2 alarm actions wired", "all alarms have AlarmActions")
    else:
        record("FAIL", "M2 alarm actions wired", f"{len(no_actions)} without actions: {no_actions[:3]}")

    # M3 SNS topic has subscribers
    try:
        topic_arn = get_ssm(ssm, SNS_TOPIC_SSM)
        subs = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
        confirmed = [s for s in subs if s["SubscriptionArn"] != "PendingConfirmation"]
        if confirmed:
            record("PASS", "M3 SNS topic has subscribers",
                   f"{len(confirmed)} confirmed: {[s['Protocol'] + ':' + s['Endpoint'] for s in confirmed]}")
        elif subs:
            record("WARN", "M3 SNS topic has only pending subscribers",
                   f"{len(subs)} pending — confirm to receive alerts")
        else:
            record("WARN", "M3 SNS topic has NO subscribers",
                   "alarms will fire but nobody is listening")
    except ClientError as e:
        record("FAIL", "M3 SNS topic lookup", str(e))

    # M4 any alarm has fired recently
    cutoff = datetime.now(timezone.utc) - timedelta(days=30)
    fired_recently = []
    currently_alarming = []
    for a in alarms:
        if a["StateValue"] == "ALARM":
            currently_alarming.append(a["AlarmName"])
        try:
            history = cw.describe_alarm_history(
                AlarmName=a["AlarmName"],
                HistoryItemType="StateUpdate",
                MaxRecords=20,
            )["AlarmHistoryItems"]
            for h in history:
                if h["Timestamp"] >= cutoff and '"newState":{"stateValue":"ALARM"' in h["HistoryData"].replace(" ", ""):
                    fired_recently.append((a["AlarmName"], h["Timestamp"]))
                    break
        except ClientError:
            pass

    if currently_alarming:
        record("PASS", "M4 alarms actively detecting issues",
               f"{len(currently_alarming)} currently ALARM: {currently_alarming}")
    elif fired_recently:
        record("PASS", "M4 alarms have fired in last 30d",
               f"{len(fired_recently)} alarms transitioned to ALARM recently")
    else:
        record("WARN", "M4 no alarms have fired in last 30d",
               "alarm pipe is untested end-to-end (could be healthy, could be broken)")

    # M5 dashboard
    try:
        cw.get_dashboard(DashboardName=DASHBOARD_NAME)
        record("PASS", "M5 CloudWatch dashboard exists", DASHBOARD_NAME)
    except ClientError as e:
        record("FAIL", "M5 CloudWatch dashboard", str(e))


def verify_lakeformation(session) -> None:
    lf = session.client("lakeformation")
    ssm = session.client("ssm")
    sts = session.client("sts")

    section("LAKE FORMATION")

    account = sts.get_caller_identity()["Account"]
    db = get_ssm(ssm, DB_SSM)

    # L1 list all grants on curated
    grants = []
    next_token = None
    try:
        while True:
            kwargs = {
                "Resource": {"Table": {"CatalogId": account, "DatabaseName": db, "Name": CURATED_TABLE}},
                "ResourceType": "TABLE",
            }
            if next_token:
                kwargs["NextToken"] = next_token
            resp = lf.list_permissions(**kwargs)
            grants.extend(resp.get("PrincipalResourcePermissions", []))
            next_token = resp.get("NextToken")
            if not next_token:
                break
    except ClientError as e:
        record("FAIL", "L1 list-permissions", str(e))
        return

    by_principal: dict[str, set[str]] = {}
    for g in grants:
        principal = g["Principal"]["DataLakePrincipalIdentifier"]
        resource = g["Resource"]
        if "TableWithColumns" not in resource:
            continue
        wc = resource["TableWithColumns"].get("ColumnWildcard", {})
        col_excludes = frozenset(wc.get("ExcludedColumnNames", []))
        by_principal[principal] = set(col_excludes)

    def find_principal(needle: str) -> tuple[str, set[str]] | None:
        for p, exc in by_principal.items():
            if needle in p:
                return p, exc
        return None

    found_count = sum(1 for needle in EXPECTED_LF_GRANTS if find_principal(needle))
    if found_count == len(EXPECTED_LF_GRANTS):
        record("PASS", "L1 expected role grants present", f"{found_count}/{len(EXPECTED_LF_GRANTS)}")
    else:
        missing = [n for n in EXPECTED_LF_GRANTS if not find_principal(n)]
        record("FAIL", "L1 some expected role grants missing", f"missing: {missing}")

    # L2 enforcement status (IAM_ALLOWED_PRINCIPALS with any of ALL/SELECT bypasses LF)
    iam_allowed_bypass = any(
        g["Principal"]["DataLakePrincipalIdentifier"] == "IAM_ALLOWED_PRINCIPALS"
        and (set(g["Permissions"]) & {"SELECT", "ALL", "Super"})
        for g in grants
    )
    if iam_allowed_bypass:
        record("WARN", "L2 IAM_ALLOWED_PRINCIPALS SELECT still granted",
               "column exclusions DEFINED but NOT ENFORCED — see activation command")
        print(f"\n     Activation command:")
        print(f"     aws lakeformation revoke-permissions --region {REGION} \\")
        print(f"       --principal '{{\"DataLakePrincipalIdentifier\":\"IAM_ALLOWED_PRINCIPALS\"}}' \\")
        print(f"       --resource '{{\"Table\":{{\"CatalogId\":\"{account}\",\"DatabaseName\":\"{db}\",\"Name\":\"{CURATED_TABLE}\"}}}}' \\")
        print(f"       --permissions SELECT")
    else:
        record("PASS", "L2 IAM_ALLOWED_PRINCIPALS revoked",
               "column-level RBAC is ACTIVELY ENFORCED")

    for needle, expected in EXPECTED_LF_GRANTS.items():
        found = find_principal(needle)
        if not found:
            continue
        principal, actual = found
        short = principal.split("/")[-1][:45]
        if actual == expected:
            note = f"excludes: {sorted(expected)}" if expected else "all columns"
            record("PASS", f"L3 {needle} grant matches template", f"{short}  {note}")
        else:
            record("FAIL", f"L3 {needle} exclusions mismatch",
                   f"expected: {sorted(expected)}, got: {sorted(actual)}")


def main() -> int:
    session = boto3.session.Session(region_name=REGION)
    print(f"Account: {session.client('sts').get_caller_identity()['Account']}  Region: {REGION}")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")

    verify_monitoring(session)
    verify_lakeformation(session)

    section("SUMMARY")
    by_level = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for level, _, _ in results:
        by_level[level] += 1
    print(f"  PASS={by_level['PASS']}  WARN={by_level['WARN']}  FAIL={by_level['FAIL']}")
    return 1 if by_level["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
