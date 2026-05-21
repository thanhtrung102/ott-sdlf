"""Package each analytics Lambda's source .py as a zip and upload to S3.

After the CFN templates switch from Code.ZipFile (4 KB limit) to
Code.S3Bucket/S3Key, the .py files in lambda/<name>/src/ become the single
source of truth. This helper:

  1. Reads each lambda/<name>/src/lambda_function.py
  2. Validates syntax with ast.parse
  3. Wraps as a zip in memory with key 'index.py' (matches handler config)
  4. Uploads to s3://<artifacts>/lambda/<name>.zip
  5. Optionally calls lambda:UpdateFunctionCode to push to live Lambda
     (use --update-live flag for code-only updates; skip for CFN-driven deploys)

Run before `aws cloudformation deploy` of any Lambda stack. After CFN deploy
finishes, the function points to the latest zip in S3.
"""
import argparse
import ast
import hashlib
import io
import sys
import zipfile
from pathlib import Path

import boto3

REGION = "ap-southeast-1"
ARTIFACTS_BUCKET_SSM = "/sdlf/storage/rArtifactsBucket/prod"

# Repo-relative paths so this runs identically on Windows (ott-pipeline.ps1)
# and Linux (CI/CD CodeBuild): this file is <repo>/scripts/, sources are at
# <repo>/sdlf-main-ott/lambda/<subdir>/src/lambda_function.py.
REPO = Path(__file__).resolve().parent.parent
LAMBDA_ROOT = REPO / "sdlf-main-ott" / "lambda"

# (lambda-key, lambda-subdir, live-function-name)
LAMBDAS = [
    ("trending",   "trending",    "sdlf-ott-mainTR-report"),
    ("lutrefresh", "lut-refresh", "sdlf-ott-mainLUT-refresh"),
]


def package(src_path: Path) -> tuple[bytes, str]:
    src = src_path.read_text(encoding="utf-8")
    ast.parse(src)  # raises on syntax error
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("index.py", src)
    blob = buf.getvalue()
    digest = hashlib.sha256(blob).hexdigest()[:12]
    return blob, digest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--update-live", action="store_true",
                        help="Also call lambda:UpdateFunctionCode (for code-only changes)")
    parser.add_argument("--only", help="Limit to one lambda key (trending|lutrefresh)")
    args = parser.parse_args()

    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)

    artifacts_bucket = ssm.get_parameter(Name=ARTIFACTS_BUCKET_SSM)["Parameter"]["Value"]

    for key, subdir, fn_name in LAMBDAS:
        if args.only and args.only != key:
            continue
        src_path = LAMBDA_ROOT / subdir / "src" / "lambda_function.py"
        if not src_path.exists():
            print(f"[{key}] SKIP: {src_path} not found")
            continue
        blob, digest = package(src_path)
        s3_key = f"lambda/{key}.zip"
        s3.put_object(
            Bucket=artifacts_bucket, Key=s3_key, Body=blob,
            ContentType="application/zip",
            Metadata={"source-sha256-12": digest},
        )
        print(f"[{key}] uploaded s3://{artifacts_bucket}/{s3_key} "
              f"({len(blob)} bytes, sha256={digest})")
        if args.update_live:
            r = lam.update_function_code(FunctionName=fn_name, ZipFile=blob)
            print(f"[{key}]   live update: {fn_name} CodeSize={r['CodeSize']} "
                  f"LastModified={r['LastModified']}")


if __name__ == "__main__":
    sys.exit(main())
