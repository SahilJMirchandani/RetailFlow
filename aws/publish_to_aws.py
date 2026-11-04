"""Command-line entry point for publishing RetailFlow 2.0 to AWS."""
import json
import sys

from botocore.exceptions import BotoCoreError, ClientError

from aws.data_lake import publish_data_and_register


def main() -> int:
    try:
        report = publish_data_and_register()
    except (BotoCoreError, ClientError, OSError, RuntimeError, ValueError, TypeError) as error:
        print(f"AWS PUBLICATION FAILED: {error}", file=sys.stderr)
        return 1

    print("=" * 68)
    print("RETAILFLOW 2.0 — AWS DATA LAKE PUBLICATION")
    print("=" * 68)
    print(f"Status       : {report['status']}")
    print(f"Region       : {report['region']}")
    print(f"S3 location  : s3://{report['bucket']}/{report['prefix']}/")
    print(f"Files uploaded: {report['files_uploaded']}")
    print(f"Files unchanged: {report['files_unchanged']}")
    print(f"Glue database: {report['glue_database']}")
    print(f"Gold tables  : {len(report['glue_tables'])}")
    for table in report["glue_tables"]:
        print(f"  - {table['table']} ({table['operation']})")
    print(f"S3 manifest  : {report.get('s3_manifest_uri', 'not available')}")
    print(f"Local report : data/output/aws_publication_latest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
