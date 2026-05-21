"""Save a local copy of the live OTT dashboard.

Since the sdlf-pipeline-ott-dashboard stack was added, the Trending Lambda
regenerates the dashboard into S3 + CloudFront on every pipeline run
(write_dashboard in lambda/trending/src/lambda_function.py). The dashboard is
therefore always current and population-true — sourced from the unfiltered
fpt_ott_searchevents_analytics.curated table, not the volume-thresholded gold
table.

This script no longer queries Athena or bakes numbers itself. It just pulls
the live dashboard down to dashboard/index.html for offline viewing.

Usage:
    python D:/ott-sdlf/scripts/regenerate_dashboard.py
"""
import io
import sys
import urllib.request
from pathlib import Path

import boto3

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

REGION = "ap-southeast-1"
HTML_PATH = Path(r"D:\ott-sdlf\dashboard\index.html")

ssm = boto3.client("ssm", region_name=REGION)
url = ssm.get_parameter(Name="/sdlf/pipeline/rDashboardUrl/ott")["Parameter"]["Value"]
print(f"Live dashboard: {url}")

req = urllib.request.Request(url, headers={"User-Agent": "regenerate_dashboard"})
with urllib.request.urlopen(req, timeout=30) as resp:
    html = resp.read().decode("utf-8")

HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
HTML_PATH.write_text(html, encoding="utf-8")
print(f"Saved local copy -> {HTML_PATH}  ({len(html):,} bytes)")
print(f"  Hosted (always current): {url}")
print(f"  Local copy:              file:///{HTML_PATH.as_posix()}")
