"""
csv_to_parquet.py — Phase 1: Bronze → Silver transform
=======================================================
Reads the raw sales CSV from the Bronze zone of MinIO, performs light cleansing,
converts it to Parquet (via pandas + pyarrow), and uploads the result to the
Silver zone at  silver/clean/sales/year=<YYYY>/sales.parquet.

This is the canonical Bronze → Silver step in a medallion lakehouse architecture.
Real pipelines use Spark or Flink for scale, but pandas + pyarrow demonstrates
the concept with zero cluster overhead.

Usage
-----
    # Activate the shared venv first
    source ~/lakehouse/.venv/bin/activate

    # Run with defaults (reads credentials from .env)
    python csv_to_parquet.py

    # Override source/destination keys
    python csv_to_parquet.py \
        --bronze-key "sales/year=2026/sales.csv" \
        --silver-key "clean/sales/year=2026/sales.parquet"

Environment variables  (set in .env or export before running)
-------------------------------------------------------------
    MINIO_ENDPOINT_URL   default: http://localhost:9000
    MINIO_ROOT_USER      default: admin
    MINIO_ROOT_PASSWORD  required (no default)
    MINIO_REGION         default: us-east-1

Key concepts
------------
* Bronze zone  — raw, immutable, unmodified data as it arrived.
* Silver zone  — cleansed, typed, and deduplicated; ready for analytics.
* Parquet is columnar + binary: smaller on disk, faster to query than CSV.
* pyarrow is called under the hood by pandas.to_parquet(); no extra code needed.
"""

import argparse
import io
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants / defaults
# ---------------------------------------------------------------------------
DEFAULT_ENDPOINT = "http://localhost:9000"
DEFAULT_REGION   = "us-east-1"
BRONZE_BUCKET    = "bronze"
SILVER_BUCKET    = "silver"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_s3_client() -> boto3.client:
    """Construct a boto3 S3 client pointed at MinIO (or any S3-compatible store)."""
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", DEFAULT_ENDPOINT)
    user     = os.environ.get("MINIO_ROOT_USER", "admin")
    password = os.environ.get("MINIO_ROOT_PASSWORD", "")
    region   = os.environ.get("MINIO_REGION", DEFAULT_REGION)

    if not password:
        log.warning(
            "MINIO_ROOT_PASSWORD is not set — using empty string. "
            "Set it in .env or export MINIO_ROOT_PASSWORD=<value>"
        )

    log.info("Connecting to MinIO at %s", endpoint)
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=user,
        aws_secret_access_key=password,
        region_name=region,
        verify=endpoint.startswith("https"),
    )


def read_bronze_csv(s3: boto3.client, key: str) -> pd.DataFrame:
    """Download the CSV from bronze and parse it into a DataFrame."""
    log.info("Reading  s3://%s/%s", BRONZE_BUCKET, key)
    try:
        obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        log.error(
            "Cannot read s3://%s/%s — %s. "
            "Have you run upload_bronze.py yet?",
            BRONZE_BUCKET, key, code,
        )
        sys.exit(1)

    df = pd.read_csv(obj["Body"])
    log.info("Loaded %d rows × %d columns", len(df), len(df.columns))
    log.info("Schema:\n%s", df.dtypes.to_string())
    return df


def cleanse(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light Silver-zone cleansing:
      - Drop rows with any null values
      - Cast 'amount' to float64 (ensures correct Parquet type)
      - Cast 'id' to int64
      - Parse 'ts' as UTC-aware datetime (stored as int96 / TIMESTAMP in Parquet)
    """
    before = len(df)
    df = df.dropna()
    dropped = before - len(df)
    if dropped:
        log.warning("Dropped %d null row(s)", dropped)

    df["amount"] = df["amount"].astype("float64")
    df["id"]     = df["id"].astype("int64")

    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)

    log.info("Cleansed DataFrame — %d rows remain", len(df))
    return df


def upload_parquet(s3: boto3.client, df: pd.DataFrame, key: str) -> int:
    """Serialise df to Parquet in memory and PUT it to the silver bucket."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    parquet_bytes = buf.getvalue()
    size_kb = len(parquet_bytes) / 1024

    log.info(
        "Serialised to Parquet — %d bytes (%.1f KB) with Snappy compression",
        len(parquet_bytes), size_kb,
    )

    try:
        s3.head_bucket(Bucket=SILVER_BUCKET)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        raise RuntimeError(
            f"Bucket '{SILVER_BUCKET}' does not exist (error {code}). "
            "Create it in the MinIO console or via Terraform before running this script."
        ) from e

    buf.seek(0)
    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=key,
        Body=parquet_bytes,
        ContentType="application/octet-stream",
        Metadata={
            "converted-at": datetime.now(timezone.utc).isoformat(),
            "rows":         str(len(df)),
            "phase":        "01-silver",
        },
    )
    log.info("✓  Uploaded → s3://%s/%s", SILVER_BUCKET, key)
    return len(parquet_bytes)


def verify_upload(s3: boto3.client, silver_key: str, parquet_size: int) -> None:
    """HeadObject the uploaded Parquet file and sanity-check the size."""
    head = s3.head_object(Bucket=SILVER_BUCKET, Key=silver_key)
    remote_size = head["ContentLength"]
    if remote_size != parquet_size:
        log.error(
            "Size mismatch — uploaded %d bytes but S3 reports %d bytes",
            parquet_size, remote_size,
        )
        sys.exit(1)
    log.info(
        "Verification OK — size: %d bytes, ETag: %s",
        remote_size, head["ETag"],
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    year = datetime.now(timezone.utc).strftime("%Y")
    parser = argparse.ArgumentParser(
        description="Convert the Bronze CSV to a Silver Parquet file in MinIO.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--bronze-key",
        default=f"sales/year={year}/sales.csv",
        metavar="S3_KEY",
        help=(
            "Key of the source CSV inside the bronze bucket. "
            f"Default: sales/year={year}/sales.csv"
        ),
    )
    parser.add_argument(
        "--silver-key",
        default=f"clean/sales/year={year}/sales.parquet",
        metavar="S3_KEY",
        help=(
            "Destination key inside the silver bucket. "
            f"Default: clean/sales/year={year}/sales.parquet"
        ),
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip the post-upload HeadObject verification.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    s3 = build_s3_client()

    # 1. Read raw CSV from Bronze
    df = read_bronze_csv(s3, args.bronze_key)

    # 2. Cleanse + type-cast (Bronze → Silver transform)
    df = cleanse(df)

    # 3. Serialise to Parquet and upload to Silver
    size = upload_parquet(s3, df, args.silver_key)

    # 4. Verify
    if not args.skip_verify:
        verify_upload(s3, args.silver_key, size)

    log.info(
        "Done.  Bronze CSV  →  Silver Parquet\n"
        "  s3://bronze/%s\n"
        "  s3://silver/%s",
        args.bronze_key, args.silver_key,
    )


if __name__ == "__main__":
    main()
