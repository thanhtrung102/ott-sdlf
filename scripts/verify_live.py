"""Live infrastructure verification for the OTT SDLF pipeline.

Walks every deployed functionality and asserts it against live AWS:
CI/CD, CloudFormation stacks, the Glue ETL job, the Glue catalog, the
state machines, the analytics Lambdas, the DLQs, CloudWatch alarms + the
dashboard, and Lake Formation enforcement.

Reproducible on any machine: needs only AWS credentials for the target
account (region ap-southeast-1) and `pip install boto3`.

  python scripts/verify_live.py

Each line is PASS / WARN / FAIL. WARN marks a real live state worth knowing
(e.g. an alarm firing) that is not a script failure. Exit 0 unless a FAIL.
"""
import io
import json
import sys
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import boto3

# Vietnamese diacritics appear in live keyword data; force UTF-8 stdout.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
ANALYTICS_DB = "fpt_ott_searchevents_analytics"
OTT_STACKS = [
    "sdlf-ott-searchevents-glue-job",
    "sdlf-pipeline-ott-mainA",
    "sdlf-pipeline-ott-mainB",
    "sdlf-pipeline-ott-dataquality",
    "sdlf-pipeline-ott-lutrefresh",
    "sdlf-pipeline-ott-trending",
    "sdlf-pipeline-ott-monitoring",
    "sdlf-pipeline-ott-lakeformation",
    "sdlf-pipeline-ott-dashboard",
]
STATE_MACHINES = ["mainA", "mainB", "mainDQ"]
ANALYTICS_LAMBDAS = [
    "sdlf-ott-mainTR-report",
    "sdlf-ott-mainLUT-refresh",
]
DLQS = [
    "sdlf-ott-mainA-dlq.fifo",
    "sdlf-ott-mainB-dlq.fifo",
    "sdlf-ott-mainLUT-dlq",
    "sdlf-ott-mainTR-dlq",
]
EXPECTED_ANALYTICS_TABLES = {
    "raw_search_events", "curated", "dq_results",
}

counts = {"PASS": 0, "WARN": 0, "FAIL": 0}


def line(status: str, label: str, detail: str = "") -> None:
    counts[status] += 1
    msg = f"  [{status}] {label}"
    if detail:
        msg += f"  ({detail})"
    print(msg)


def section(title: str) -> None:
    print(f"\n=== {title} ===")


sts = boto3.client("sts", region_name=REGION)
ACCOUNT = sts.get_caller_identity()["Account"]


def verify_cicd() -> None:
    section("CI/CD pipeline")
    cp = boto3.client("codepipeline", region_name=REGION)
    execs = cp.list_pipeline_executions(
        pipelineName="sdlf-ott-cicd", maxResults=1)["pipelineExecutionSummaries"]
    if not execs:
        line("FAIL", "sdlf-ott-cicd has executions")
        return
    status = execs[0]["status"]
    line("PASS" if status == "Succeeded" else "WARN",
         "sdlf-ott-cicd latest execution", f"status={status}")


def verify_stacks() -> None:
    section("CloudFormation — 9 OTT stacks")
    cfn = boto3.client("cloudformation", region_name=REGION)
    for name in OTT_STACKS:
        try:
            st = cfn.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"]
            # UPDATE_COMPLETE_CLEANUP_IN_PROGRESS is a healthy terminal state —
            # the stack update succeeded and CFN is just removing replaced
            # resources in the background. Treat it as PASS, not FAIL.
            ok = st in ("CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS")
            line("PASS" if ok else "FAIL", name, st)
        except cfn.exceptions.ClientError as e:
            line("FAIL", name, str(e.response["Error"]["Code"]))


def verify_glue() -> None:
    section("Glue ETL job + catalog")
    glue = boto3.client("glue", region_name=REGION)
    job = glue.get_job(JobName="sdlf-ott-searchevents-glue-job")["Job"]
    line("PASS", "Glue job sdlf-ott-searchevents-glue-job",
         f"Glue {job['GlueVersion']}, {job['NumberOfWorkers']}x{job['WorkerType']}")
    runs = glue.get_job_runs(
        JobName="sdlf-ott-searchevents-glue-job", MaxResults=1)["JobRuns"]
    if runs:
        r = runs[0]
        line("PASS" if r["JobRunState"] == "SUCCEEDED" else "WARN",
             "Glue job last run", f"{r['JobRunState']}, {r['ExecutionTime']}s")

    dbs = {d["Name"] for d in glue.get_databases()["DatabaseList"]}
    line("PASS" if ANALYTICS_DB in dbs else "FAIL", f"Glue database {ANALYTICS_DB}")

    tables = {t["Name"] for t in glue.get_paginator("get_tables")
              .paginate(DatabaseName=ANALYTICS_DB).build_full_result()["TableList"]}
    missing = EXPECTED_ANALYTICS_TABLES - tables
    expected_count = len(EXPECTED_ANALYTICS_TABLES)
    line("PASS" if not missing else "FAIL",
         f"{ANALYTICS_DB}: {expected_count} expected tables",
         f"{len(tables & EXPECTED_ANALYTICS_TABLES)}/{expected_count}"
         + (f", missing {sorted(missing)}" if missing else ""))
    # Surface any stragglers from retired stacks (former content-gap +
    # trending CSV tables) so the cleanup state is visible.
    legacy = {"content_gaps", "premium_vs_free", "repeat_search_rate",
              "hour_of_day_heatmap", "guest_vs_auth_demand",
              "trending_all", "trending_unknown"}
    leftover = sorted(tables & legacy)
    if leftover:
        line("WARN", "legacy CSV-backed tables still in catalog",
             ", ".join(leftover))


def verify_state_machines() -> None:
    section("Step Functions — 3 state machines")
    sf = boto3.client("stepfunctions", region_name=REGION)
    for sm in STATE_MACHINES:
        arn = f"arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:sdlf-ott-{sm}-sm"
        try:
            execs = sf.list_executions(
                stateMachineArn=arn, maxResults=1)["executions"]
        except sf.exceptions.StateMachineDoesNotExist:
            line("FAIL", f"sdlf-ott-{sm}-sm", "does not exist")
            continue
        if not execs:
            line("WARN", f"sdlf-ott-{sm}-sm", "no executions yet")
            continue
        st = execs[0]["status"]
        line("PASS" if st == "SUCCEEDED" else "WARN",
             f"sdlf-ott-{sm}-sm latest execution", st)


def verify_lambdas() -> None:
    section("Lambda — analytics functions")
    lam = boto3.client("lambda", region_name=REGION)
    for fn in ANALYTICS_LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            line("PASS", fn, f"{cfg['Runtime']}, {cfg['MemorySize']}MB")
        except lam.exceptions.ResourceNotFoundException:
            line("FAIL", fn, "not found")


def verify_dlqs() -> None:
    section("SQS — 4 dead-letter queues")
    sqs = boto3.client("sqs", region_name=REGION)
    for q in DLQS:
        url = f"https://sqs.{REGION}.amazonaws.com/{ACCOUNT}/{q}"
        try:
            depth = int(sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )["Attributes"]["ApproximateNumberOfMessages"])
            line("PASS" if depth == 0 else "WARN", q, f"depth={depth}")
        except sqs.exceptions.QueueDoesNotExist:
            line("FAIL", q, "does not exist")


def verify_monitoring() -> None:
    section("CloudWatch — dashboard + alarms")
    cw = boto3.client("cloudwatch", region_name=REGION)
    dashboards = [d["DashboardName"] for d in
                  cw.list_dashboards()["DashboardEntries"]]
    line("PASS" if "sdlf-ott-searchevents-pipeline" in dashboards else "FAIL",
         "dashboard sdlf-ott-searchevents-pipeline")

    alarms = cw.describe_alarms(AlarmNamePrefix="sdlf-ott")["MetricAlarms"]
    line("PASS" if alarms else "FAIL", "sdlf-ott alarms deployed",
         f"{len(alarms)} alarms")
    firing = [a["AlarmName"] for a in alarms if a["StateValue"] == "ALARM"]
    if firing:
        line("WARN", "alarms currently in ALARM state",
             ", ".join(firing))
    else:
        line("PASS", "no alarms in ALARM state")


def verify_dashboard() -> None:
    section("CloudFront dashboard — single user-facing surface")
    ssm = boto3.client("ssm", region_name=REGION)
    try:
        url = ssm.get_parameter(Name="/sdlf/pipeline/rDashboardUrl/ott")["Parameter"]["Value"]
    except ssm.exceptions.ParameterNotFound:
        line("FAIL", "Dashboard URL published to SSM", "param missing")
        return
    line("PASS" if url.startswith("https://") else "FAIL",
         "Dashboard URL published to SSM", url)
    try:
        with urlopen(url, timeout=15) as resp:
            raw = resp.read()
            body = raw.decode("utf-8", errors="replace")
            ok = resp.status == 200 and len(raw) > 8000 and "<!DOCTYPE html" in body
            line("PASS" if ok else "FAIL",
                 "Dashboard HTTP 200 + non-trivial HTML",
                 f"status={resp.status} bytes={len(raw)}")
    except (HTTPError, URLError) as e:
        line("FAIL", "Dashboard fetch", str(e))
        return
    # Source-attribution caption — every render should stamp the dt window.
    line("PASS" if "Source: curated" in body else "FAIL",
         "Header source-attribution stamp present")
    for section_name in (
        "Total Searches", "Distinct Keywords", "Top 20 Keywords",
        "Trending Keywords", "Content Gaps", "Premium vs Free",
        "Repeat Search Rate", "Guest vs Authenticated",
        "Search Volume by Hour",
    ):
        line("PASS" if section_name in body else "FAIL",
             f"Dashboard section present: {section_name}")


def verify_lake_formation() -> None:
    section("Lake Formation — column RBAC enforcement")
    lf = boto3.client("lakeformation", region_name=REGION)
    perms = lf.list_permissions(Resource={"Table": {
        "CatalogId": ACCOUNT,
        "DatabaseName": ANALYTICS_DB,
        "Name": "curated",
    }})["PrincipalResourcePermissions"]
    iam_open = any(p["Principal"]["DataLakePrincipalIdentifier"]
                   == "IAM_ALLOWED_PRINCIPALS" for p in perms)
    if iam_open:
        line("WARN", "curated table — IAM_ALLOWED_PRINCIPALS still granted",
             "column exclusions DEFINED but NOT ENFORCED — revoke to activate")
    else:
        line("PASS", "curated table — IAM_ALLOWED_PRINCIPALS revoked",
             "column-level RBAC enforced")


def main() -> int:
    print(f"OTT SDLF pipeline — live verification")
    print(f"account={ACCOUNT}  region={REGION}")
    for fn in (verify_cicd, verify_stacks, verify_glue, verify_state_machines,
               verify_lambdas, verify_dlqs, verify_monitoring, verify_dashboard,
               verify_lake_formation):
        try:
            fn()
        except Exception as e:  # noqa: BLE001 — report, never abort the sweep
            counts["FAIL"] += 1
            print(f"  [FAIL] {fn.__name__} raised {type(e).__name__}: {e}")

    section("Summary")
    print(f"  PASS={counts['PASS']}  WARN={counts['WARN']}  FAIL={counts['FAIL']}")
    return 1 if counts["FAIL"] else 0


if __name__ == "__main__":
    sys.exit(main())
