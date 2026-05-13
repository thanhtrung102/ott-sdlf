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

DB         = os.environ["ATHENA_DATABASE"]
RESULTS    = os.environ["ATHENA_RESULTS"]
ART_BUCKET = os.environ["ARTIFACTS_BUCKET"]
ART_KEY    = os.environ["ARTIFACTS_KEY"]
MODEL_ID   = os.environ["BEDROCK_MODEL_ID"]
MAX_KW     = int(os.environ["MAX_NEW_KEYWORDS"])
BATCH_SZ   = int(os.environ["BATCH_SIZE"])
THROTTLE   = float(os.environ["THROTTLE_SECONDS"])

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


def load_lut():
    data = s3.get_object(Bucket=ART_BUCKET, Key=ART_KEY)["Body"].read()
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        return json.loads(z.read("genre_classifier/lut_extended.json").decode("utf-8"))


def save_lut(lut):
    existing = s3.get_object(Bucket=ART_BUCKET, Key=ART_KEY)["Body"].read()
    lut_bytes = json.dumps(lut, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    buf = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(existing)) as zin:
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
            for info in zin.infolist():
                if info.filename == "genre_classifier/lut_extended.json":
                    zout.writestr(info, lut_bytes)
                else:
                    zout.writestr(info, zin.read(info.filename))
    buf.seek(0)
    s3.put_object(
        Bucket=ART_BUCKET, Key=ART_KEY,
        Body=buf.getvalue(), ContentType="application/zip",
    )
    logger.info(f"Zip rebuilt: {len(lut)} LUT entries -> s3://{ART_BUCKET}/{ART_KEY}")


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
                inferenceConfig={"maxTokens": 4096, "temperature": 0},
            )
            raw = resp["output"]["message"]["content"][0]["text"]
            parsed = json.loads(_CODE.sub("", raw.strip()))
            if parsed and sum(1 for k in parsed if k.upper() in VALID) / len(parsed) > 0.5:
                parsed = {v: k for k, v in parsed.items() if isinstance(v, str)}
            return {
                san.get(k, k): normalize(v)
                for k, v in parsed.items()
                if san.get(k, k) in batch and normalize(v) != "UNKNOWN"
            }
        except Exception as e:
            err = str(e)
            if "ThrottlingException" in err or "TooManyRequests" in err or "throttl" in err.lower():
                wait = 2 ** (attempt + 1)
                logger.warning(f"throttled (attempt {attempt + 1}/4) — backing off {wait}s")
                time.sleep(wait)
                continue
            logger.warning(f"batch failed: {e}")
            return {}
    logger.warning("batch failed after 4 throttle retries")
    return {}


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

    lut = load_lut()
    logger.info(f"Loaded {len(lut)} existing LUT entries from artifact zip")

    new_kws = [k for k in unknowns if k not in lut][:MAX_KW]
    logger.info(f"New keywords to classify: {len(new_kws)}")
    if not new_kws:
        logger.info("LUT is already up to date — no Bedrock calls needed")
        return {"classified": 0, "lut_size": len(lut), "message": "up-to-date"}

    classified, errors = 0, 0
    for i in range(0, len(new_kws), BATCH_SZ):
        batch = new_kws[i: i + BATCH_SZ]
        result = classify_batch(batch)
        lut.update(result)
        classified += len(result)
        if not result:
            errors += 1
        if (i // BATCH_SZ) % 20 == 0:
            logger.info(f"  [{i // BATCH_SZ + 1}] classified: {classified}, errors: {errors}")
        time.sleep(THROTTLE)
        if context.get_remaining_time_in_millis() < 120_000:
            logger.info(f"  Timeout buffer — stopping at {i + len(batch)}/{len(new_kws)}")
            break

    logger.info(f"Classification complete: {classified} new genres, {errors} batch errors")

    lut_clean = {k: v for k, v in lut.items() if v != "UNKNOWN"}
    save_lut(lut_clean)

    return {"classified": classified, "lut_size": len(lut_clean), "errors": errors}
