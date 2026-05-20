---
title: "3. Data Ingestion"
date: 2026-05-20
weight: 3
chapter: false
pre: <b>3. </b>
---

# 3. Data Ingestion

> **Reference content — placeholder.** Raw / curated schemas, Glue ETL details, and the Stage A → B handoff live here. For the hands-on path, see [Workshop chapter 4 — Ingest](../workshop/4-ingest/).

## What runs

- **Stage A** (`mainA-sm`) — S3 ObjectCreated → SDLF event-routing Step Function → DDB PEH row → EventBridge `Stage A SUCCEEDED`.
- **Stage B** (`mainB-sm`) — picks up the event → invokes Glue 4.0 / Spark 3.3 ETL → waits for completion.
- **Glue ETL** (`sdlf-ott-searchevents-glue-job`) — 19-step enrichment, G.1X × 10 workers, ~25 min for the 14-day reference dataset.

Output: curated Parquet at `s3://<analytics>/ott/searchevents/curated/dt=YYYY-MM-DD/derived_genre=<GENRE>/` — one ~1 MB file per `(dt, derived_genre)` thanks to `repartition` in the Glue script.
