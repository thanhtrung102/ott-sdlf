"""
ott-search-glue-job — SDLF Stage B enrichment.

Glue 4.0 (Spark 3.3 / Python 3.10), G.1X, 10 workers.
Reads raw OTT search Parquet from the SDLF raw bucket and writes a
19-column curated layer partitioned by dt + derived_genre.

SDLF passes SOURCE_LOCATION and OUTPUT_LOCATION as job arguments.
Optional PUSH_DOWN_PREDICATE limits which dt partitions are processed;
omit it (or pass "all") for a full historical backfill.

Extra Python dependency:  --extra-py-files s3://<artifacts>/genre_classifier_flat.zip
  Contains genre_classifier/rules.py + lut.json + lut_extended.json
  (the same zip bundled in the CDK Lambda layer).

Enrichment steps (sequence is load-bearing):
  1.  clean_datetime     — Arabic-Indic numerals + Buddhist Era year repair
  2.  event_ts           — parse normalised string → UTC timestamp
  3.  DROP year < 2015   — corrupt year 0004 rows
  4.  hour_of_day_vn     — Vietnam timezone hour (UTC+7)
  5.  session_action     — passthrough of category field
  6.  is_search_abandoned
  7.  user_is_authenticated
  8.  user_id_hashed     — SHA-256, null-safe
  9.  keyword_norm       — lower(trim(keyword))
  10. derived_genre      — rule-based waterfall (lut → regex → fuzzy → UNKNOWN)
  11. platform_group     — 35 raw strings → 6 canonical buckets
  12. network_type_norm  — 9 values → 5 canonical
  13. isp_segment        — upper(proxy_isp)
  14. has_premium        — VIP / HBO GO+ / K+ / MAX in userplansmap
  15. subscription_count
  16. plan_names         — parsed ARRAY<STRING> of subscription plan names (e.g. ["VIP", "K+"])
  17. search_session_id  — SHA-256(user_id|30-min bucket), anon-safe
  18. is_repeat_search   — LAG window over session
  19. _pipeline_run_id   — Glue JOB_RUN_ID for lineage
"""
import re as _re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, expr, floor, hour, lit,
    lower, sha2, to_timestamp, trim, udf, when, year,
)
from pyspark.sql.types import (
    ArrayType, BooleanType, IntegerType, StringType,
    StructField, StructType,
)

# ── Job arguments ─────────────────────────────────────────────────────────────

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_LOCATION", "OUTPUT_LOCATION"],
)

try:
    _PIPELINE_RUN_ID = getResolvedOptions(sys.argv, ["JOB_RUN_ID"])["JOB_RUN_ID"]
except Exception:
    _PIPELINE_RUN_ID = "unknown"

# Optional: push-down predicate to limit dt partitions.
# Pass "all" or omit entirely for a full historical backfill.
try:
    _pdp_raw = getResolvedOptions(sys.argv, ["PUSH_DOWN_PREDICATE"])["PUSH_DOWN_PREDICATE"]
    _PUSH_DOWN_PREDICATE = None if _pdp_raw.lower() in ("all", "none", "") else _pdp_raw
except Exception:
    _PUSH_DOWN_PREDICATE = None

SOURCE = args["SOURCE_LOCATION"].rstrip("/")
OUTPUT = args["OUTPUT_LOCATION"].rstrip("/")

# ── Spark / Glue context ──────────────────────────────────────────────────────

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Prevents EXCEPTION-mode timestamp failures on corrupted datetime strings.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

import logging as _logging
logger = _logging.getLogger(__name__)

# ── genre_classifier (bundled via --extra-py-files) ───────────────────────────

try:
    from genre_classifier.rules import (
        classify_keyword as _classify_kw,
        bucket_platform as _bucket_platform,
        normalize_network_type as _norm_nt,
    )
except ImportError:
    # Baked-in degraded-mode fallback so a missing/corrupt classifier zip
    # doesn't reduce all output to UNKNOWN. Covers the top high-confidence
    # Vietnamese OTT genre signals only — the full LUT (~118k entries) lives
    # in genre_classifier_pkg.zip, refreshed by the LUT Refresh Lambda.
    logger.error("genre_classifier zip missing — falling back to baked-in mini-LUT")

    _FALLBACK_PLATFORM = {
        "android": "Mobile", "androidtv": "TV", "ios": "Mobile",
        "iphone": "Mobile", "ipad": "Mobile", "smarttv": "TV",
        "samsung": "TV", "lg": "TV", "web": "Web", "browser": "Web",
    }
    _FALLBACK_NETWORK = {"wifi": "wifi", "4g": "mobile", "5g": "mobile", "3g": "mobile"}
    _FALLBACK_GENRE_REGEX = [
        (_re.compile(r"\b(anime|manga|naruto|one[\s-]?piece|doraemon|conan)\b", _re.I), "ANIME"),
        (_re.compile(r"\b(bong[\s-]?da|world[\s-]?cup|premier[\s-]?league|champions[\s-]?league|the[\s-]?thao|euro)\b", _re.I), "THE_THAO"),
        (_re.compile(r"\b(nhac|music|mv|karaoke|sing|son[\s-]?tung)\b", _re.I), "NHAC"),
        (_re.compile(r"\b(phim[\s-]?(viet|vn|vietnam))\b", _re.I), "PHIM_VIET"),
        (_re.compile(r"\b(phim[\s-]?(han|korea|kbs|sbs))\b", _re.I), "PHIM_HAN"),
        (_re.compile(r"\b(phim[\s-]?(trung|tq|hoa|china))\b", _re.I), "PHIM_TRUNG"),
        (_re.compile(r"\b(phim[\s-]?(my|au[\s-]?my|hollywood))\b", _re.I), "PHIM_AU_MY"),
        (_re.compile(r"\b(vtv|htv|vtc|truyen[\s-]?hinh)\b", _re.I), "TRUYEN_HINH"),
    ]

    def _classify_kw(kw):
        if not kw:
            return "EMPTY_QUERY"
        for rx, label in _FALLBACK_GENRE_REGEX:
            if rx.search(kw):
                return label
        return "UNKNOWN"

    def _bucket_platform(p):
        if not p:
            return "Other"
        return _FALLBACK_PLATFORM.get(p.lower(), "Other")

    def _norm_nt(n):
        if not n:
            return "unknown"
        return _FALLBACK_NETWORK.get(n.lower(), "unknown")

classify_keyword_udf     = udf(_classify_kw, StringType())
bucket_platform_udf      = udf(_bucket_platform, StringType())
normalize_network_udf    = udf(_norm_nt, StringType())

# ── Step 1: Arabic-Indic / Buddhist Era datetime repair ───────────────────────

_AR = str.maketrans(
    "٠١٢٣٤٥٦٧٨٩"    # Arabic-Indic   U+0660–U+0669
    "۰۱۲۳۴۵۶۷۸۹",   # Ext. Arabic    U+06F0–U+06F9
    "01234567890123456789",
)

_DT_RE = _re.compile(
    r'^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?'
)


@udf(StringType())
def clean_datetime(dt_str: str):
    if dt_str is None:
        return None
    s = dt_str.strip().translate(_AR)
    if s.startswith("2565"):
        s = "2022" + s[4:]
    m = _DT_RE.match(s)
    if not m:
        return None
    date_p, hh, mm, ss, frac = m.groups()
    ms = ((frac or "") + "000")[:3]
    return f"{date_p} {hh.zfill(2)}:{mm}:{ss}.{ms}"


# ── Step 14: Premium flag ─────────────────────────────────────────────────────

_PREMIUM_PLANS = {"VIP", "HBO GO+", "K+", "MAX"}


@udf(BooleanType())
def has_premium_udf(plans):
    if plans is None:
        return False
    for entry in plans:
        name = entry.split(":")[0].strip() if ":" in str(entry) else str(entry).strip()
        if name in _PREMIUM_PLANS:
            return True
    return False


@udf(IntegerType())
def subscription_count_udf(plans):
    if plans is None:
        return None
    return len(plans)


@udf(ArrayType(StringType()))
def plan_names_udf(plans):
    if plans is None:
        return None
    return [
        (entry.split(":")[0].strip() if ":" in str(entry) else str(entry).strip())
        for entry in plans
    ]


# ── Main enrichment pipeline ──────────────────────────────────────────────────

def run() -> None:
    # Read using the parquet-embedded schema to avoid name-case mismatches
    # (source uses camelCase: eventID, networkType, userPlansMap).
    # Normalise to snake_case immediately after read.
    df = (
        spark.read
        .option("mergeSchema", "false")
        .option("recursiveFileLookup", "true")
        .parquet(SOURCE)
    )

    # Normalise camelCase column names from the source parquet to the names
    # used throughout the rest of this script.
    df = (
        df
        .withColumnRenamed("eventID",     "eventid")
        .withColumnRenamed("networkType", "networktype")
        .withColumnRenamed("userPlansMap", "userplansmap")
    )

    # raw-source layout uses bare YYYYMMDD directories (not Hive key=value), so Spark
    # won't create a partition column automatically — extract dt from the file path.
    df = (
        df
        .withColumn("_path", F.input_file_name())
        .withColumn("dt", F.regexp_extract(col("_path"), r"/(\d{8})/", 1))
        .drop("_path")
    )

    if _PUSH_DOWN_PREDICATE:
        df = df.filter(
            F.to_date(col("dt"), "yyyyMMdd") >=
            F.to_date(F.lit(_PUSH_DOWN_PREDICATE.replace("dt >= ", "")), "yyyy-MM-dd")
        )

    # Lineage counter: rows read from raw.
    _raw_rows = df.count()

    # ── Steps 1-2: Datetime clean + parse ────────────────────────────────────
    df = (
        df
        .withColumn("datetime_clean", clean_datetime(col("datetime")))
        .withColumn("event_ts", to_timestamp(col("datetime_clean"), "yyyy-MM-dd HH:mm:ss.SSS"))
    )

    # ── Step 3: Drop corrupt year 0004 ───────────────────────────────────────
    df = df.filter(year(col("event_ts")) >= 2015)
    _post_year_rows = df.count()

    # Deduplicate on event_id — guards against double-writes from Glue job retries.
    df = df.dropDuplicates(["eventid"])
    _post_dedup_rows = df.count()

    # ── Step 4: Vietnam timezone hour ────────────────────────────────────────
    df = df.withColumn(
        "hour_of_day_vn",
        hour(col("event_ts") + expr("interval 7 hours")).cast(IntegerType()),
    )

    # ── Steps 5-6 ────────────────────────────────────────────────────────────
    df = (
        df
        .withColumn(
            "session_action",
            when(col("category") == lit("quit"),  lit("ABANDON"))
            .when(col("category") == lit("enter"), lit("SUBMIT"))
            .when(col("category").isNotNull(),     F.upper(col("category")))
            .otherwise(lit(None)),
        )
        .withColumn("is_search_abandoned", col("category") == lit("quit"))
    )

    # ── Steps 7-8 ────────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("user_is_authenticated", col("user_id").isNotNull())
        .withColumn(
            "user_id_hashed",
            when(col("user_id").isNotNull(), sha2(col("user_id"), 256)).otherwise(lit(None)),
        )
    )

    # ── Step 9 ───────────────────────────────────────────────────────────────
    df = df.withColumn(
        "keyword_norm",
        when(col("keyword").isNotNull(), lower(trim(col("keyword")))).otherwise(lit(None)),
    )

    # ── Step 10: Genre classification ────────────────────────────────────────
    # A null keyword_norm means the search event carried no query text at all —
    # that is an empty query, not an unclassified one. Bucket it as EMPTY_QUERY
    # so the UNKNOWN bucket holds only keywords that *could* be classified but
    # are not yet in the LUT. (The classifier UDF already returns EMPTY_QUERY
    # for empty-string keywords; this aligns the null case with it.)
    df = df.withColumn(
        "derived_genre",
        when(col("keyword_norm").isNotNull(), classify_keyword_udf(col("keyword_norm")))
        .otherwise(lit("EMPTY_QUERY")),
    )

    # ── Steps 11-13 ──────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("platform_group",    bucket_platform_udf(col("platform")))
        .withColumn("network_type_norm", normalize_network_udf(col("networktype")))
        .withColumn("isp_segment",       F.upper(col("proxy_isp")))
    )

    # ── Steps 14-15 ──────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("has_premium",        has_premium_udf(col("userplansmap")))
        .withColumn("subscription_count", subscription_count_udf(col("userplansmap")))
        .withColumn("plan_names",          plan_names_udf(col("userplansmap")))
    )

    # ── Step 16: Session ID (30-min bucket) ──────────────────────────────────
    half_hour_bucket = floor(col("event_ts").cast("long") / lit(1800)).cast("string")

    df = df.withColumn(
        "search_session_id",
        when(
            col("user_is_authenticated"),
            sha2(F.concat(col("user_id"), lit("|"), half_hour_bucket), 256),
        ).otherwise(
            sha2(F.concat(col("proxy_isp"), lit("|"), col("platform"), lit("|"), half_hour_bucket), 256),
        ),
    )

    # ── Step 17: Repeat search (LAG over session window) ─────────────────────
    session_w = (
        Window
        .partitionBy("search_session_id")
        .orderBy(col("event_ts"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    prev_keyword = F.last(
        when(col("keyword_norm").isNotNull(), col("keyword_norm")),
        ignorenulls=True,
    ).over(session_w)

    df = df.withColumn(
        "is_repeat_search",
        when(col("keyword_norm").isNull(), lit(None).cast(BooleanType()))
        .otherwise(col("keyword_norm") == prev_keyword),
    )

    # ── Step 18: Pipeline lineage column ─────────────────────────────────────
    df = df.withColumn("_pipeline_run_id", lit(_PIPELINE_RUN_ID))

    # ── Step 19: Normalise dt to ISO yyyy-MM-dd ───────────────────────────────
    # Raw dirs are bare YYYYMMDD; convert to yyyy-MM-dd for consistent partition key.
    df = df.withColumn(
        "dt_norm",
        when(
            F.length(col("dt")) == 8,
            F.concat(col("dt").substr(1, 4), lit("-"), col("dt").substr(5, 2), lit("-"), col("dt").substr(7, 2)),
        ).otherwise(col("dt")),
    )

    # ── Final column selection ────────────────────────────────────────────────
    out = df.select(
        col("eventid").alias("event_id"),
        col("event_ts"),
        col("hour_of_day_vn"),
        col("user_id_hashed"),
        col("user_is_authenticated"),
        col("session_action"),
        col("is_search_abandoned"),
        col("keyword_norm"),
        col("derived_genre"),
        col("platform_group"),
        col("network_type_norm"),
        col("isp_segment"),
        col("has_premium"),
        col("subscription_count"),
        col("plan_names"),
        col("search_session_id"),
        col("is_repeat_search"),
        col("_pipeline_run_id"),
        col("dt_norm").alias("dt"),
    )

    # Shuffle so each (dt, derived_genre) group lands on a single task —
    # eliminates the small-file fan-out from 10 workers × ~50 Hive partitions
    # that would otherwise produce hundreds of tiny Parquet files.
    out = out.repartition(col("dt"), col("derived_genre"))

    logger.warning("Writing curated layer to %s", OUTPUT)
    (
        out.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("dt", "derived_genre")
        .parquet(OUTPUT)
    )

    _output_rows = out.count()
    # Structured single-line lineage marker — greppable in CloudWatch Logs Insights.
    # Retention <0.7 across raw->output should page someone: implies a regression
    # in the year filter, dedup logic, or input source.
    _ratio = _output_rows / _raw_rows if _raw_rows else 0
    logger.warning(
        "LINEAGE run_id=%s raw=%d post_year=%d post_dedup=%d output=%d retention=%.3f",
        _PIPELINE_RUN_ID, _raw_rows, _post_year_rows, _post_dedup_rows, _output_rows, _ratio,
    )
    if _ratio < 0.7:
        logger.error("LINEAGE_ALARM retention %.3f below 0.7 threshold", _ratio)


run()
job.commit()
