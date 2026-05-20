"""Live infrastructure verification for the OTT SDLF pipeline.

Walks every deployed functionality and asserts it against live AWS:
CI/CD, CloudFormation stacks, the Glue ETL job, the Glue catalog, the four
state machines, the analytics Lambdas, the DLQs, CloudWatch alarms + the
dashboard, the HTTP API, and Lake Formation enforcement.

Reproducible on any machine: needs only AWS credentials for the target
account (region ap-southeast-1) and `pip install boto3`. The API key is
read from SSM, so no secret is passed on the command line.

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
GOLD_DB = "fpt_ott_searchevents_gold"

OTT_STACKS = [
    "sdlf-ott-searchevents-glue-job",
    "sdlf-pipeline-ott-mainA",
    "sdlf-pipeline-ott-mainB",
    "sdlf-pipeline-ott-dataquality",
    "sdlf-pipeline-ott-lutrefresh",
    "sdlf-pipeline-ott-contentgap",
    "sdlf-pipeline-ott-trending",
    "sdlf-pipeline-ott-goldquality",
    "sdlf-pipeline-ott-monitoring",
    "sdlf-pipeline-ott-lakeformation",
    "sdlf-pipeline-ott-api",
]
STATE_MACHINES = ["mainA", "mainB", "mainDQ", "mainGoldDQ"]
ANALYTICS_LAMBDAS = [
    "sdlf-ott-mainTR-report",
    "sdlf-ott-mainCG-report",
    "sdlf-ott-mainLUT-refresh",
    "sdlf-ott-api",
]
DLQS = [
    "sdlf-ott-mainA-dlq.fifo",
    "sdlf-ott-mainB-dlq.fifo",
    "sdlf-ott-mainCG-dlq",
    "sdlf-ott-mainLUT-dlq",
    "sdlf-ott-mainTR-dlq",
]
EXPECTED_ANALYTICS_TABLES = {
    "raw_search_events", "curated", "dq_results",
    "content_gaps", "premium_vs_free", "repeat_search_rate",
    "hour_of_day_heatmap", "guest_vs_auth_demand",
    "trending_all", "trending_unknown",
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
    section("CloudFormation — 11 OTT stacks")
    cfn = boto3.client("cloudformation", region_name=REGION)
    for name in OTT_STACKS:
        try:
            st = cfn.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"]
            ok = st in ("CREATE_COMPLETE", "UPDATE_COMPLETE")
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
    for db in (ANALYTICS_DB, GOLD_DB):
        line("PASS" if db in dbs else "FAIL", f"Glue database {db}")

    tables = {t["Name"] for t in glue.get_paginator("get_tables")
              .paginate(DatabaseName=ANALYTICS_DB).build_full_result()["TableList"]}
    missing = EXPECTED_ANALYTICS_TABLES - tables
    line("PASS" if not missing else "FAIL",
         f"{ANALYTICS_DB}: 10 expected tables",
         f"{len(tables & EXPECTED_ANALYTICS_TABLES)}/10"
         + (f", missing {sorted(missing)}" if missing else ""))

    gold_tables = {t["Name"] for t in
                   glue.get_tables(DatabaseName=GOLD_DB)["TableList"]}
    line("PASS" if "keyword_trends" in gold_tables else "FAIL",
         f"{GOLD_DB}.keyword_trends")


def verify_state_machines() -> None:
    section("Step Functions — 4 state machines")
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
    section("Lambda — analytics + API functions")
    lam = boto3.client("lambda", region_name=REGION)
    for fn in ANALYTICS_LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            line("PASS", fn, f"{cfg['Runtime']}, {cfg['MemorySize']}MB")
        except lam.exceptions.ResourceNotFoundException:
            line("FAIL", fn, "not found")


def verify_dlqs() -> None:
    section("SQS — 5 dead-letter queues")
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


def verify_api() -> None:
    section("HTTP API — endpoints + auth + freshness")
    ssm = boto3.client("ssm", region_name=REGION)
    api = ssm.get_parameter(Name="/sdlf/pipeline/rApiUrl/ott")["Parameter"]["Value"]
    key = ssm.get_parameter(
        Name="/sdlf/ott/api-key/prod", WithDecryption=True)["Parameter"]["Value"]

    def call(path: str, with_key: bool):
        req = Request(api.rstrip("/") + path)
        if with_key:
            req.add_header("x-api-key", key)
        return urlopen(req, timeout=30)

    try:
        resp = call("/trending?limit=3", with_key=True)
        body = json.loads(resp.read())
        fresh = resp.headers.get("X-Data-Freshness", "")
        line("PASS" if resp.status == 200 and body else "FAIL",
             "GET /trending (authorised)", f"HTTP {resp.status}, {len(body)} rows")
        line("PASS" if fresh else "FAIL",
             "X-Data-Freshness header present", fresh)
    except (HTTPError, URLError) as e:
        line("FAIL", "GET /trending (authorised)", str(e))

    try:
        call("/trending?limit=1", with_key=False)
        line("FAIL", "GET /trending without key rejected", "expected 401")
    except HTTPError as e:
        line("PASS" if e.code == 401 else "FAIL",
             "GET /trending without key rejected", f"HTTP {e.code}")

    try:
        resp = call("/content-gaps?report=premium_vs_free&limit=2", with_key=True)
        body = json.loads(resp.read())
        line("PASS" if resp.status == 200 and body else "FAIL",
             "GET /content-gaps (authorised)",
             f"HTTP {resp.status}, {len(body)} rows")
    except (HTTPError, URLError) as e:
        line("FAIL", "GET /content-gaps (authorised)", str(e))


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
               verify_lambdas, verify_dlqs, verify_monitoring, verify_api,
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
