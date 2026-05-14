"""
ott-search-glue-job — SDLF Stage B enrichment.

Glue 4.0 (Spark 3.3 / Python 3.10), G.1X, 10 workers.
Reads raw OTT search Parquet from the SDLF raw bucket and writes an
18-column curated layer partitioned by dt + derived_genre.

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
  16. search_session_id  — SHA-256(user_id|30-min bucket), anon-safe
  17. is_repeat_search   — LAG window over session
  18. is_cross_partition_date
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

# Optional: push-down predicate to limit dt partitions.
# Pass "all" or omit entirely for a full historical backfill.
try:
    _pdp_raw = getResolvedOptions(sys.argv, ["PUSH_DOWN_PREDICATE"])["PUSH_DOWN_PREDICATE"]
    _PUSH_DOWN_PREDICATE = None if _pdp_raw.lower() in ("all", "none", "") else _pdp_raw
except Exception:
    _PUSH_DOWN_PREDICATE = "dt >= date_format(date_sub(current_date(), 2), 'yyyy-MM-dd')"

SOURCE = args["SOURCE_LOCATION"].rstrip("/")
OUTPUT = args["OUTPUT_LOCATION"].rstrip("/")

# ── Spark / Glue context ──────────────────────────────────────────────────────

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Prevents EXCEPTION-mode timestamp failures on corrupted datetime strings.
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

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
    logger.warning("genre_classifier not found — all genres will be UNKNOWN")
    def _classify_kw(kw): return "UNKNOWN"
    def _bucket_platform(p): return "Other"
    def _norm_nt(n): return "unknown"

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

_PREMIUM_PLANS = {"VIP", "HBO GO+", "K+", "MAX", "MAX XMAS"}


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

    # ── Steps 1-2: Datetime clean + parse ────────────────────────────────────
    df = (
        df
        .withColumn("datetime_clean", clean_datetime(col("datetime")))
        .withColumn("event_ts", to_timestamp(col("datetime_clean"), "yyyy-MM-dd HH:mm:ss.SSS"))
    )

    # ── Step 3: Drop corrupt year 0004 ───────────────────────────────────────
    df = df.filter(year(col("event_ts")) >= 2015)

    # ── Step 4: Vietnam timezone hour ────────────────────────────────────────
    df = df.withColumn(
        "hour_of_day_vn",
        hour(col("event_ts") + expr("interval 7 hours")).cast(IntegerType()),
    )

    # ── Steps 5-6 ────────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("session_action", col("category"))
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
    df = df.withColumn(
        "derived_genre",
        when(col("keyword_norm").isNotNull(), classify_keyword_udf(col("keyword_norm")))
        .otherwise(lit("UNKNOWN")),
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

    # ── Step 18: Cross-partition date flag ────────────────────────────────────
    # dt comes from either the Hive partition or the YYYYMMDD dir extracted above.
    # Normalise to YYYY-MM-DD string for the flag.
    df = df.withColumn(
        "dt_norm",
        when(
            F.length(col("dt")) == 8,
            F.concat(col("dt").substr(1, 4), lit("-"), col("dt").substr(5, 2), lit("-"), col("dt").substr(7, 2)),
        ).otherwise(col("dt")),
    )

    df = df.withColumn(
        "is_cross_partition_date",
        year(col("event_ts")) != col("dt_norm").substr(1, 4).cast(IntegerType()),
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
        col("search_session_id"),
        col("is_repeat_search"),
        col("is_cross_partition_date"),
        col("dt_norm").alias("dt"),
    )

    logger.warning("Writing curated layer to %s", OUTPUT)
    (
        out.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("dt", "derived_genre")
        .parquet(OUTPUT)
    )

    count = out.count()
    logger.warning("Curated rows written: %d", count)


run()
job.commit()
