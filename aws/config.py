"""Configuration for RetailFlow's optional AWS data-lake integration."""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent


def load_aws_settings():
    load_dotenv(ROOT / ".env", override=False)
    return {
        "enabled": os.getenv("RETAILFLOW_AWS_ENABLED", "false").strip().lower()
        in {"1", "true", "yes", "on"},
        "region": os.getenv("RETAILFLOW_AWS_REGION", "ap-south-1").strip(),
        "bucket": os.getenv("RETAILFLOW_AWS_BUCKET", "").strip(),
        "prefix": os.getenv("RETAILFLOW_AWS_PREFIX", "retailflow").strip().strip("/"),
        "database": os.getenv("RETAILFLOW_GLUE_DATABASE", "retailflow").strip().lower(),
        "workgroup": os.getenv("RETAILFLOW_ATHENA_WORKGROUP", "primary").strip(),
        "athena_output": os.getenv("RETAILFLOW_ATHENA_OUTPUT_LOCATION", "").strip(),
        "upload_images": os.getenv("RETAILFLOW_AWS_UPLOAD_IMAGES", "false").strip().lower()
        in {"1", "true", "yes", "on"},
    }


def require_aws_settings():
    settings = load_aws_settings()
    if not settings["enabled"]:
        raise RuntimeError(
            "AWS integration is disabled. Set RETAILFLOW_AWS_ENABLED=true in .env."
        )
    bucket = settings["bucket"]
    if not bucket:
        raise RuntimeError("Set RETAILFLOW_AWS_BUCKET to your S3 bucket name.")
    if (
        len(bucket) < 3 or len(bucket) > 63 or bucket != bucket.lower()
        or not bucket[0].isalnum() or not bucket[-1].isalnum()
        or any(not (ch.isalnum() or ch in ".-") for ch in bucket)
        or ".." in bucket or ".-" in bucket or "-." in bucket
    ):
        raise ValueError("RETAILFLOW_AWS_BUCKET is not a valid S3 bucket name.")
    if not settings["region"]:
        raise ValueError("RETAILFLOW_AWS_REGION cannot be empty.")
    if not settings["database"] or not settings["database"].replace("_", "").isalnum():
        raise ValueError("RETAILFLOW_GLUE_DATABASE must use letters, digits and underscores.")
    if not settings["prefix"]:
        raise ValueError("RETAILFLOW_AWS_PREFIX cannot be empty.")
    if not settings["athena_output"]:
        settings["athena_output"] = (
            f"s3://{bucket}/{settings['prefix']}/athena-results/"
        )
    if not settings["athena_output"].startswith("s3://"):
        raise ValueError("RETAILFLOW_ATHENA_OUTPUT_LOCATION must be an s3:// URI.")
    return settings
