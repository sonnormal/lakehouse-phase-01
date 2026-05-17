"""
upload_bronze.py — Phase 1: Storage Layer (MinIO + Parquet)
============================================================
Uploads a sample CSV file to the Bronze zone of a local MinIO instance
using the boto3 S3-compatible API.

Usage
-----
    # Activate the shared venv first
    source ~/lakehouse/.venv/bin/activate

    # Run with defaults (reads credentials from .env or env vars)
    python upload_bronze.py

    # Override source CSV
    python upload_bronze.py --csv path/to/your/file.csv --key raw/custom_name.csv

Environment variables (set in .env or export before running)
-------------------------------------------------------------
    MINIO_ENDPOINT_URL   default: http://localhost:9000
    MINIO_ROOT_USER      default: admin
    MINIO_ROOT_PASSWORD  required (no default)
    MINIO_REGION         default: us-east-1  (MinIO ignores this; boto3 requires it)

Key concepts from Phase 1
--------------------------
* Object storage uses a flat namespace: bucket + key (e.g. bronze/raw/sales.csv).
  The slash in the key is just a prefix convention — there are no real folders.
* MinIO is fully S3-compatible. Changing MINIO_ENDPOINT_URL to an AWS/Azure/GCS
  endpoint is the only change needed to run this script against a cloud provider.
* Bronze zone = raw, unmodified data exactly as it arrived.  Never modify in place.
"""

import argparse
import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
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
GOLD_BUCKET      = "gold"

# Sample data that mimics a real sales extract
SAMPLE_ROWS: list[dict] = [
    {"id": 1, "name": "Alice",  "amount": 120.50, "currency": "USD", "ts": "2024-01-15T08:00:00Z"},
    {"id": 2, "name": "Bob",    "amount":  85.00, "currency": "USD", "ts": "2024-01-15T09:30:00Z"},
    {"id": 3, "name": "Carol",  "amount": 200.00, "currency": "USD", "ts": "2024-01-15T11:00:00Z"},
    {"id": 4, "name": "David",  "amount":  45.75, "currency": "USD", "ts": "2024-01-15T13:15:00Z"},
    {"id": 5, "name": "Eve",    "amount": 310.20, "currency": "USD", "ts": "2024-01-15T14:45:00Z"},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_env() -> None:
    """Optionally load a .env file from the phase-01 directory (no dependency on dotenv)."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        log.debug(".env not found — relying on shell environment variables")
        return
    log.debug("Loading %s", env_path)
    with env_path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


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
        # Disable SSL verification for local http:// endpoints
        verify=endpoint.startswith("https"),
    )


def make_sample_csv(rows: list[dict]) -> bytes:
    """Serialise sample rows to an in-memory CSV (no temp file on disk)."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def upload_to_bronze(
    s3: boto3.client,
    body: bytes,
    key: str,
    source_label: str = "in-memory sample",
) -> None:
    """PUT an object into the bronze bucket under the given key.

    The bucket must already exist — create it via Terraform or the MinIO console
    before running this script.  If it is missing, a clear error is raised.
    """
    log.info("Uploading %d bytes from [%s] → s3://%s/%s", len(body), source_label, BRONZE_BUCKET, key)
    try:
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=body,
            ContentType="text/csv",
            Metadata={
                "ingested-at": datetime.now(timezone.utc).isoformat(),
                "source":      source_label,
                "phase":       "01-bronze",
            },
        )
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code == "NoSuchBucket":
            log.error(
                "Bucket '%s' does not exist. "
                "Create it first with Terraform or the MinIO console, then re-run.",
                BRONZE_BUCKET,
            )
        else:
            log.error("Upload failed (%s): %s", code, error)
        sys.exit(1)
    log.info("✓  Uploaded → s3://%s/%s", BRONZE_BUCKET, key)


def verify_upload(s3: boto3.client, key: str) -> None:
    """Head-object the uploaded key to confirm it is accessible."""
    try:
        head = s3.head_object(Bucket=BRONZE_BUCKET, Key=key)
        log.info(
            "Verification OK — size: %d bytes, ETag: %s",
            head["ContentLength"],
            head["ETag"],
        )
    except ClientError as error:
        log.error("Verification FAILED for s3://%s/%s — %s", BRONZE_BUCKET, key, error)
        sys.exit(1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a CSV file to the Bronze zone of a MinIO lakehouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--csv",
        metavar="PATH",
        help="Path to a local CSV file to upload. Omit to use the built-in sample data.",
    )
    parser.add_argument(
        "--key",
        metavar="S3_KEY",
        default=None,
        help=(
            "S3 key within the bronze bucket (e.g. sales/year=2024/sales.csv). "
            "Defaults to <stem>/year=<YYYY>/<filename> when --csv is provided, "
            "or sales/year=<YYYY>/sales.csv for sample data."
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
    load_env()
    args = parse_args()

    # Resolve body + key
    year = datetime.now(timezone.utc).strftime("%Y")
    if args.csv:
        csv_path = Path(args.csv).expanduser().resolve()
        if not csv_path.exists():
            log.error("File not found: %s", csv_path)
            sys.exit(1)
        body  = csv_path.read_bytes()
        key   = args.key or f"{csv_path.stem}/year={year}/{csv_path.name}"
        label = str(csv_path)
    else:
        body  = make_sample_csv(SAMPLE_ROWS)
        key   = args.key or f"sales/year={year}/sales.csv"
        label = "in-memory sample (5 rows)"

    # Connect
    s3 = build_s3_client()

    # Upload
    upload_to_bronze(s3, body, key, source_label=label)

    # Verify
    if not args.skip_verify:
        verify_upload(s3, key)


if __name__ == "__main__":
    main()
