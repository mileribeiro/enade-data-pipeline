"""AWS Glue job that publishes ENADE files already staged in Bronze.

Required Glue parameters:
    --JOB_NAME       Name configured for the AWS Glue job.
    --TARGET_BUCKET  Name of the S3 bucket that stores the Bronze layer.
    --YEAR           Four-digit reference year supplied when the job is started.

Optional Glue parameters:
    --EXPECTED_TEXT_FILES  Expected number of TXT files (default: 32).
"""

from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


DEFAULT_EXPECTED_TEXT_FILES = 32
FILE_CHUNK_SIZE = 1024 * 1024


class BronzeIngestionError(RuntimeError):
    """Raised when staged Bronze files cannot be safely published."""


def optional_argument(name: str, default: str) -> str:
    """Read an optional Glue argument while keeping a useful local default."""

    flag = f"--{name}"
    if flag not in sys.argv:
        return default

    position = sys.argv.index(flag)
    if position + 1 >= len(sys.argv):
        raise BronzeIngestionError(f"Argument {flag} requires a value.")
    return sys.argv[position + 1]


def resolve_year() -> str:
    """Read the year supplied in the Glue Job arguments."""

    year = optional_argument("YEAR", "")
    if not year:
        raise BronzeIngestionError("--YEAR is required.")
    return year


def safe_archive_path(member_name: str) -> PurePosixPath:
    """Reject unsafe ZIP member names before using them in an S3 key."""

    path = PurePosixPath(member_name)
    if path.is_absolute() or ".." in path.parts:
        raise BronzeIngestionError(f"Unsafe ZIP member path: {member_name}")
    return path


def download_staged_archive(s3_client: Any, bucket: str, key: str, destination: Path) -> str:
    """Read a ZIP manually uploaded to Bronze and return its SHA-256."""

    try:
        s3_client.download_file(bucket, key, str(destination))
    except (BotoCoreError, ClientError) as error:
        raise BronzeIngestionError(f"Could not read staged ZIP s3://{bucket}/{key}: {error}") from error

    if not destination.exists() or destination.stat().st_size == 0:
        raise BronzeIngestionError("Staged ZIP is empty.")
    if not zipfile.is_zipfile(destination):
        raise BronzeIngestionError("Staged file is not a valid ZIP archive.")

    checksum = hashlib.sha256()
    with destination.open("rb") as input_file:
        for chunk in iter(lambda: input_file.read(FILE_CHUNK_SIZE), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def txt_members(archive: zipfile.ZipFile, expected_count: int) -> list[zipfile.ZipInfo]:
    """Return the original ENADE TXT members and validate the expected source shape."""

    corrupt_member = archive.testzip()
    if corrupt_member:
        raise BronzeIngestionError(f"ZIP archive is corrupt at member: {corrupt_member}")

    members = []
    for member in archive.infolist():
        path = safe_archive_path(member.filename)
        if not member.is_dir() and path.suffix.lower() == ".txt":
            members.append(member)

    if len(members) != expected_count:
        raise BronzeIngestionError(
            f"Expected {expected_count} TXT files in the official archive; found {len(members)}."
        )

    return members


def dictionary_member(archive: zipfile.ZipFile) -> zipfile.ZipInfo:
    """Return the single XLSX data dictionary included with the microdata."""

    members = [
        member
        for member in archive.infolist()
        if not member.is_dir() and safe_archive_path(member.filename).suffix.lower() == ".xlsx"
    ]
    if len(members) != 1:
        raise BronzeIngestionError(f"Expected one XLSX dictionary in the archive; found {len(members)}.")
    return members[0]


def upload_dictionary(
    s3_client: Any,
    bucket: str,
    key: str,
    archive: zipfile.ZipFile,
    member: zipfile.ZipInfo,
    source_key: str,
    archive_sha256: str,
) -> None:
    """Publish the original XLSX dictionary with a stable Bronze object name."""

    with archive.open(member, "r") as source_file:
        s3_client.upload_fileobj(
            source_file,
            bucket,
            key,
            ExtraArgs={
                "ContentType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "Metadata": {"source_key": source_key, "archive_sha256": archive_sha256},
            },
        )


def upload_txt_files(
    s3_client: Any,
    bucket: str,
    prefix: str,
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
    source_key: str,
    archive_sha256: str,
) -> list[str]:
    """Upload unchanged TXT files while preserving their relative archive paths."""

    uploaded_keys: list[str] = []
    for member in members:
        path = safe_archive_path(member.filename)
        key = f"{prefix}/{path.name}"

        with archive.open(member, "r") as source_file:
            s3_client.upload_fileobj(
                source_file,
                bucket,
                key,
                ExtraArgs={
                    "ContentType": "text/plain; charset=iso-8859-1",
                    "Metadata": {
                        "source_key": source_key,
                        "archive_sha256": archive_sha256,
                    },
                },
            )
        uploaded_keys.append(key)

    return uploaded_keys


def run_ingestion(
    target_bucket: str,
    year: str,
    expected_text_files: int,
) -> dict[str, Any]:
    """Validate a ZIP staged in Bronze and publish its unchanged TXT members."""

    if expected_text_files <= 0:
        raise BronzeIngestionError("EXPECTED_TEXT_FILES must be greater than zero.")

    target_prefix = f"{year}/bronze"
    archive_key = f"{target_prefix}/archive/microdados_enade_{year}.zip"
    s3_client = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL_S3") or os.getenv("AWS_ENDPOINT_URL"))

    with tempfile.TemporaryDirectory(prefix="enade_bronze_") as temporary_directory:
        archive_path = Path(temporary_directory) / f"microdados_enade_{year}.zip"
        archive_sha256 = download_staged_archive(s3_client, target_bucket, archive_key, archive_path)

        try:
            with zipfile.ZipFile(archive_path) as archive:
                members = txt_members(archive, expected_text_files)
                dictionary = dictionary_member(archive)
                dictionary_key = f"{target_prefix}/dictionary.xlsx"
                upload_dictionary(
                    s3_client,
                    target_bucket,
                    dictionary_key,
                    archive,
                    dictionary,
                    archive_key,
                    archive_sha256,
                )
                uploaded_keys = upload_txt_files(
                    s3_client,
                    target_bucket,
                    target_prefix,
                    archive,
                    members,
                    archive_key,
                    archive_sha256,
                )
        except (OSError, zipfile.BadZipFile, BotoCoreError, ClientError) as error:
            raise BronzeIngestionError(f"Could not publish Bronze source files: {error}") from error

    return {
        "bucket": target_bucket,
        "archive_key": archive_key,
        "archive_sha256": archive_sha256,
        "dictionary_key": dictionary_key,
        "text_file_count": len(uploaded_keys),
        "text_file_keys": uploaded_keys,
    }


def main() -> None:
    required_arguments = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "TARGET_BUCKET"],
    )
    year = resolve_year()
    if not year.isdigit() or len(year) != 4:
        raise BronzeIngestionError("YEAR must contain exactly four digits.")
    expected_text_files = int(optional_argument("EXPECTED_TEXT_FILES", str(DEFAULT_EXPECTED_TEXT_FILES)))

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(required_arguments["JOB_NAME"], required_arguments)

    result = run_ingestion(
        target_bucket=required_arguments["TARGET_BUCKET"],
        year=year,
        expected_text_files=expected_text_files,
    )
    print(json.dumps(result, ensure_ascii=False))
    job.commit()


if __name__ == "__main__":
    main()
