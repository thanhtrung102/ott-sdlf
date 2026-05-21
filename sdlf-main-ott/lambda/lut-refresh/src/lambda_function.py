import io
import json
import os
import re
import time
import zipfile

import boto3
from datalake_library.commons import init_logger

logger = init_logger(__name__)

athena  = boto3.client("athena")
s3      = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime")

DB             = os.environ["ATHENA_DATABASE"]
RESULTS        = os.environ["ATHENA_RESULTS"]
ART_BUCKET     = os.environ["ARTIFACTS_BUCKET"]
ART_KEY        = os.environ["ARTIFACTS_KEY"]
# Project bucket the Glue ETL reads --extra-py-files from. save_lut() mirrors
# the refreshed zip here so the next Stage B run actually uses it. Empty
# string disables the mirror (back-compat for envs without the var).
PROJECT_BUCKET = os.environ.get("PROJECT_BUCKET", "")
MODEL_ID       = os.environ["BEDROCK_MODEL_ID"]
MAX_KW         = int(os.environ["MAX_NEW_KEYWORDS"])
BATCH_SZ       = int(os.environ["BATCH_SIZE"])
THROTTLE       = float(os.environ["THROTTLE_SECONDS"])

VALID = frozenset({
    "NHAC", "THE_THAO", "ANIME", "PHIM_TRUNG", "PHIM_VIET",
    "PHIM_AU_MY", "PHIM_HAN", "TRUYEN_HINH", "UNKNOWN",
})
ALIAS = {
    "PHIM_CHINA": "PHIM_TRUNG", "PHIM_TQ": "PHIM_TRUNG", "PHIM_TRUNG_QUOC": "PHIM_TRUNG",
    "PHIM_KOREAN": "PHIM_HAN", "KDRAMA": "PHIM_HAN", "K_DRAMA": "PHIM_HAN",
    "PHIM_JAPAN": "ANIME", "PHIM_NHAT": "ANIME", "CARTOON": "ANIME", "HOAT_HINH": "ANIME",
    "KPOP": "NHAC", "K_POP": "NHAC",
    "VARIETY": "TRUYEN_HINH", "SHOW": "TRUYEN_HINH",
    "PHIM_CHIEU_RAP": "PHIM_AU_MY",
}

_CLEAN = re.compile(r"[\x00-\x1f\x7f\\]")
_CODE  = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)

# Keywords Bedrock structurally cannot classify — single/two-char type-ahead
# fragments, empty strings, and voice-search UI artifacts. They only ever come
# back UNKNOWN, so they are filtered before the Bedrock call and recorded as
# resolved (see is_classifiable + the resolved set) instead of being re-sent
# on every run.
_META_QUERIES = frozenset({
    "tìm kiếm bằng giọng nói",  # Vietnamese for "search by voice"
})


def is_classifiable(kw):
    if not kw:
        return False
    s = kw.strip()
    if len(s) <= 2:
        return False
    return s not in _META_QUERIES

SYS = (
    "You are a genre classifier for FPT Play, a Vietnamese OTT platform.\n"
    "Genre codes: PHIM_TRUNG PHIM_VIET PHIM_HAN PHIM_AU_MY ANIME THE_THAO NHAC TRUYEN_HINH UNKNOWN\n"
    "PHIM_TRUNG=Chinese C-drama/wuxia, PHIM_VIET=Vietnamese drama, PHIM_HAN=Korean K-drama, "
    "PHIM_AU_MY=Western/Hollywood, ANIME=Japanese anime/manga, THE_THAO=Sports, "
    "NHAC=Music/Kpop/Vpop, TRUYEN_HINH=TV channels/variety/live. "
    "Use UNKNOWN ONLY for gibberish, single chars, or meta-queries like 'voice search'.\n"
    "Most keywords are partial movie/drama titles — classify them specifically."
)
TPL = (
    "Classify these Vietnamese OTT search keywords into genres.\n"
    "Output ONLY a JSON object where keys=keywords and values=genre codes.\n"
    'Example: {{"fairy tail": "ANIME", "bong da": "THE_THAO", "tam cam": "PHIM_VIET"}}\n\n'
    "Keywords:\n{kws}\n\nReturn ONLY the JSON object, no explanation."
)


def normalize(v):
    if not isinstance(v, str):
        return "UNKNOWN"
    u = v.upper().strip()
    u = ALIAS.get(u, u)
    return u if u in VALID else "UNKNOWN"


def athena_query(sql):
    r = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        ResultConfiguration={"OutputLocation": RESULTS},
    )
    qid = r["QueryExecutionId"]
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED"):
            raise RuntimeError(f"Athena query {st}: {qid}")
        time.sleep(3)
    rows = []
    first = True
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for row in page["ResultSet"]["Rows"]:
            if first:
                first = False
                continue
            v = row["Data"][0].get("VarCharValue", "")
            if v:
                rows.append(v)
    return rows


_LUT_FILE      = "genre_classifier/lut_extended.json"
_RESOLVED_FILE = "genre_classifier/resolved.json"


def load_lut():
    """Return (lut, resolved). resolved = keywords already attempted and
    confirmed unclassifiable — skipped on subsequent runs so each run makes
    forward progress instead of re-attempting the same noise."""
    data = s3.get_object(Bucket=ART_BUCKET, Key=ART_KEY)["Body"].read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        names = set(z.namelist())
        lut = json.loads(z.read(_LUT_FILE).decode("utf-8"))
        resolved = set()
        if _RESOLVED_FILE in names:
            resolved = set(json.loads(z.read(_RESOLVED_FILE).decode("utf-8")))
    return lut, resolved


def save_lut(lut, resolved):
    existing = s3.get_object(Bucket=ART_BUCKET, Key=ART_KEY)["Body"].read()
    managed = {
        _LUT_FILE: json.dumps(lut, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8"),
        _RESOLVED_FILE: json.dumps(sorted(resolved), ensure_ascii=False).encode("utf-8"),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(existing)) as zin:
        existing_names = set(zin.namelist())
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
            for info in zin.infolist():
                if info.filename in managed:
                    zout.writestr(info, managed[info.filename])
                else:
                    zout.writestr(info, zin.read(info.filename))
            for name, payload in managed.items():
                if name not in existing_names:
                    zout.writestr(name, payload)
    zip_bytes = buf.getvalue()
    s3.put_object(
        Bucket=ART_BUCKET, Key=ART_KEY,
        Body=zip_bytes, ContentType="application/zip",
    )
    logger.info(
        f"Zip rebuilt: {len(lut)} LUT entries, {len(resolved)} resolved-unknown "
        f"-> s3://{ART_BUCKET}/{ART_KEY}"
    )
    # Mirror to the project bucket the Glue ETL actually reads
    # --extra-py-files from. Without this the refreshed classifier is a
    # dead artifact and curated keeps using the stale LUT.
    if PROJECT_BUCKET:
        s3.put_object(
            Bucket=PROJECT_BUCKET, Key=ART_KEY,
            Body=zip_bytes, ContentType="application/zip",
        )
        logger.info(f"Zip mirrored to project bucket -> s3://{PROJECT_BUCKET}/{ART_KEY}")
    else:
        logger.warning(
            "PROJECT_BUCKET not set — refreshed classifier NOT mirrored; "
            "next Stage B run will use the stale LUT"
        )


def classify_batch(batch):
    san = {}
    for kw in batch:
        c = _CLEAN.sub(" ", kw).strip()
        if c and c not in san:
            san[c] = kw
    for attempt in range(4):
        try:
            resp = bedrock.converse(
                modelId=MODEL_ID,
                system=[{"text": SYS}],
                messages=[{"role": "user", "content": [{"text": TPL.format(kws="\n".join(san))}]}],
                inferenceConfig={"maxTokens": 8192, "temperature": 0},
            )
            raw = resp["output"]["message"]["content"][0]["text"]
            parsed = json.loads(_CODE.sub("", raw.strip()))
            if parsed and sum(1 for k in parsed if k.upper() in VALID) / len(parsed) > 0.5:
                parsed = {v: k for k, v in parsed.items() if isinstance(v, str)}
            result = {
                san.get(k, k): normalize(v)
                for k, v in parsed.items()
                if san.get(k, k) in batch and normalize(v) != "UNKNOWN"
            }
            # errored=False: the call + parse succeeded. An empty result is a
            # valid outcome (all keywords genuinely UNKNOWN) — NOT an error.
            return result, False
        except Exception as e:
            err = str(e)
            if "ThrottlingException" in err or "TooManyRequests" in err or "throttl" in err.lower():
                wait = 2 ** (attempt + 1)
                logger.warning(f"throttled (attempt {attempt + 1}/4) — backing off {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"batch failed: {e}")
            return {}, True
    logger.warning("batch failed after 4 throttle retries")
    return {}, True


def lambda_handler(event, context):
    logger.info(
        f"LUT refresh triggered — source: {event.get('source', '?')} "
        f"detail-type: {event.get('detail-type', '?')}"
    )

    sql = (
        f"SELECT keyword_norm, COUNT(*) AS cnt "
        f"FROM {DB}.curated "
        f"WHERE derived_genre = 'UNKNOWN' AND keyword_norm IS NOT NULL "
        f"GROUP BY keyword_norm ORDER BY cnt DESC LIMIT {MAX_KW * 3}"
    )
    unknowns = athena_query(sql)
    logger.info(f"Athena: {len(unknowns)} UNKNOWN keyword_norm values in curated table")

    lut, resolved = load_lut()
    logger.info(f"Loaded {len(lut)} LUT entries, {len(resolved)} resolved-unknown markers")

    # Skip keywords already classified (lut) or already attempted (resolved).
    candidates = [k for k in unknowns if k not in lut and k not in resolved]
    # Structurally unclassifiable keywords (single/2-char fragments, empty,
    # voice-search artifacts) are recorded as resolved without a Bedrock call —
    # they would only ever return UNKNOWN and otherwise burn the whole budget.
    skipped = [k for k in candidates if not is_classifiable(k)]
    resolved.update(skipped)
    new_kws = [k for k in candidates if is_classifiable(k)][:MAX_KW]
    logger.info(
        f"Candidates: {len(candidates)} | skipped as unclassifiable: {len(skipped)} | "
        f"to classify via Bedrock: {len(new_kws)}"
    )
    if not new_kws:
        if skipped:
            save_lut(lut, resolved)
        logger.info("No classifiable new keywords — LUT is up to date")
        return {"classified": 0, "lut_size": len(lut),
                "resolved": len(resolved), "message": "up-to-date"}

    classified, errors, unresolved_total = 0, 0, 0
    for i in range(0, len(new_kws), BATCH_SZ):
        batch = new_kws[i: i + BATCH_SZ]
        result, errored = classify_batch(batch)
        if errored:
            errors += 1
        else:
            lut.update(result)
            classified += len(result)
            # Keywords the model saw but did not classify are confirmed
            # unclassifiable — mark resolved so they are never re-attempted.
            unresolved = set(batch) - set(result)
            resolved.update(unresolved)
            unresolved_total += len(unresolved)
        if (i // BATCH_SZ) % 20 == 0:
            logger.info(
                f"  [{i // BATCH_SZ + 1}] classified: {classified}, "
                f"unresolved: {unresolved_total}, batch errors: {errors}"
            )
        time.sleep(THROTTLE)
        if context.get_remaining_time_in_millis() < 120_000:
            logger.info(f"  Timeout buffer — stopping at {i + len(batch)}/{len(new_kws)}")
            break

    logger.info(
        f"Classification complete: {classified} new genres, "
        f"{unresolved_total} confirmed-unclassifiable, {errors} batch errors"
    )

    save_lut(lut, resolved)

    return {"classified": classified, "lut_size": len(lut),
            "resolved": len(resolved), "errors": errors}
