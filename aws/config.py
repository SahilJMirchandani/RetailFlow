"""Small AWS integration helper for RetailFlow.

Uses boto3's standard credential provider chain; credentials are never stored
in the repository.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent


def load_aws_settings():
    load_dotenv(ROOT / ".env", override=False)
    return {
        "enabled": os.getenv("RETAILFLOW_AWS_ENABLED", "false").lower() in {"1", "true", "yes"},
        "region": os.getenv("RETAILFLOW_AWS_REGION", "ap-south-1"),
        "bucket": os.getenv("RETAILFLOW_AWS_BUCKET", "").strip(),
        "prefix": os.getenv("RETAILFLOW_AWS_PREFIX", "retailflow").strip("/"),
        "database": os.getenv("RETAILFLOW_GLUE_DATABASE", "retailflow").lower(),
    }


def require_aws_settings():
    settings = load_aws_settings()
    if not settings["enabled"]:
        raise RuntimeError("AWS integration is disabled. Set RETAILFLOW_AWS_ENABLED=true.")
    if not settings["bucket"]:
        raise RuntimeError("Set RETAILFLOW_AWS_BUCKET to your S3 bucket name.")
    return settings
