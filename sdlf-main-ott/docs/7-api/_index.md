---
title: "7. HTTP API"
date: 2026-05-20
weight: 7
chapter: false
pre: <b>7. </b>
---

# 7. HTTP API

> **Reference content — placeholder.** Endpoint specs, auth, and freshness headers live here. For the hands-on path, see [Workshop chapter 5 — Analyze](../workshop/5-analyze/) §5.5 HTTP API and [chapter 6 — Verify](../workshop/6-verify/) §6.5 Live API hit.

## Endpoints (HTTP API v2, `arm64` Lambda)

| Verb | Path | Returns |
|---|---|---|
| `GET` | `/trending` | Top-N rows from `gold.keyword_trends` |
| `GET` | `/content-gaps` | Top-N rows from one of 5 content-gap report tables (selected via `?report=`) |

**Auth**: in-Lambda `x-api-key` header check; key in SSM at `/sdlf/ott/api-key/prod`.

**Freshness headers**: every response includes `X-Data-Freshness` (ISO-8601), `Last-Modified` (RFC-1123), and `Cache-Control: public, max-age=300`.

Source: `pipeline-ott-api.yaml`.
