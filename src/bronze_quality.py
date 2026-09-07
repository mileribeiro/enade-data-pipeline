"""AWS Glue Python Shell job that validates the ENADE Bronze layer.

Required Glue parameters:
    --TARGET_BUCKET  Bucket that stores the data lake.
    --YEAR           Four-digit reference year.

Optional Glue parameters:
    --KNOWN_MISSING_FILES        Comma-separated dictionary file names accepted as warnings.
    --KNOWN_SCHEMA_DIVERGENCES  Comma-separated files with documented schema differences.
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from xml.etree import ElementTree

import boto3
from botocore.exceptions import BotoCoreError, ClientError


XLSX_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELATIONSHIP_NAMESPACE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_RELATIONSHIP_NAMESPACE = "http://schemas.openxmlformats.org/package/2006/relationships"
NAMESPACES = {"xlsx": XLSX_NAMESPACE, "rel": RELATIONSHIP_NAMESPACE}
HEADER_READ_BYTES = 1024 * 1024
DICTIONARY_SHEET_NAME = "DICIONÁRIO_ARQUIVOS"


class BronzeQualityError(RuntimeError):
    """Raised after the Bronze quality report is published with critical findings."""


def required_argument(name: str) -> str:
    flag = f"--{name}"
    if flag not in sys.argv:
        raise BronzeQualityError(f"Missing required Glue argument: {flag}")
    position = sys.argv.index(flag)
    if position + 1 >= len(sys.argv):
        raise BronzeQualityError(f"Argument {flag} requires a value.")
    return sys.argv[position + 1]


def optional_argument(name: str, default: str) -> str:
    flag = f"--{name}"
    return default if flag not in sys.argv else required_argument(name)


def resolve_year() -> str:
    """Read YEAR directly or from the active Glue Workflow run."""

    direct_year = optional_argument("YEAR", "")
    if direct_year:
        return direct_year

    workflow_name = optional_argument("WORKFLOW_NAME", "")
    workflow_run_id = optional_argument("WORKFLOW_RUN_ID", "")
    if not workflow_name or not workflow_run_id:
        raise BronzeQualityError("YEAR is required for a direct run or must be supplied as a Workflow run property.")

    try:
        properties = boto3.client("glue").get_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
        )["RunProperties"]
    except (BotoCoreError, ClientError) as error:
        raise BronzeQualityError(f"Could not read Workflow run properties: {error}") from error

    year = properties.get("YEAR", "")
    if not year:
        raise BronzeQualityError("Workflow run property YEAR is required.")
    return year


def normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value).replace("\ufeff", "").strip().upper()


def cell_column(reference: str) -> str:
    match = re.match(r"([A-Z]+)", reference)
    if not match:
        raise BronzeQualityError(f"Invalid XLSX cell reference: {reference}")
    return match.group(1)


def shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    return ["".join(node.text or "" for node in item.iter(f"{{{XLSX_NAMESPACE}}}t")) for item in root]


def dictionary_sheet_path(archive: zipfile.ZipFile) -> str:
    workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    relationship_id = None
    for sheet in workbook.findall("xlsx:sheets/xlsx:sheet", NAMESPACES):
        if sheet.attrib.get("name") == DICTIONARY_SHEET_NAME:
            relationship_id = sheet.attrib.get(f"{{{RELATIONSHIP_NAMESPACE}}}id")
            break
    if not relationship_id:
        raise BronzeQualityError(f"XLSX sheet {DICTIONARY_SHEET_NAME!r} was not found.")

    relationships = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for relation in relationships.findall(f"{{{PACKAGE_RELATIONSHIP_NAMESPACE}}}Relationship"):
        if relation.attrib.get("Id") == relationship_id:
            return f"xl/{relation.attrib['Target'].lstrip('/')}"
    raise BronzeQualityError(f"XLSX relationship for sheet {DICTIONARY_SHEET_NAME!r} was not found.")


def xlsx_cell_value(cell: ElementTree.Element, strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.iter(f"{{{XLSX_NAMESPACE}}}t"))
    value = cell.find("xlsx:v", NAMESPACES)
    if value is None or value.text is None:
        return ""
    return strings[int(value.text)] if cell_type == "s" else value.text


def expected_files_from_dictionary(dictionary_path: Path) -> dict[str, list[str]]:
    """Read file names and expected columns from DICIONÁRIO_ARQUIVOS."""

    with zipfile.ZipFile(dictionary_path) as archive:
        strings = shared_strings(archive)
        root = ElementTree.fromstring(archive.read(dictionary_sheet_path(archive)))
        rows: list[dict[str, str]] = []
        for row in root.findall(".//xlsx:sheetData/xlsx:row", NAMESPACES):
            values = {
                cell_column(cell.attrib["r"]): xlsx_cell_value(cell, strings)
                for cell in row.findall("xlsx:c", NAMESPACES)
            }
            rows.append(values)

    if not rows:
        raise BronzeQualityError("DICIONÁRIO_ARQUIVOS is empty.")

    header = {normalize(value): column for column, value in rows[0].items()}
    name_column = header.get("NOME DO ARQUIVO")
    variables_column = header.get("VARIÁVEIS")
    if not name_column or not variables_column:
        raise BronzeQualityError("DICIONÁRIO_ARQUIVOS must contain Nome do arquivo and Variáveis columns.")

    expected: dict[str, list[str]] = {}
    for row in rows[1:]:
        file_name = row.get(name_column, "").strip()
        variables = row.get(variables_column, "")
        if not file_name:
            continue
        if not variables.strip():
            raise BronzeQualityError(f"Dictionary entry {file_name} does not define expected variables.")
        expected[file_name] = [normalize(value) for value in variables.split(",") if normalize(value)]
    if not expected:
        raise BronzeQualityError("DICIONÁRIO_ARQUIVOS has no file definitions.")
    return expected


def list_bronze_txt_files(s3_client: Any, bucket: str, prefix: str) -> tuple[dict[str, str], list[str]]:
    files: dict[str, str] = {}
    duplicates: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            path = PurePosixPath(key)
            if path.suffix.lower() != ".txt":
                continue
            file_name = path.stem
            if file_name in files:
                duplicates.append(file_name)
            else:
                files[file_name] = key
    return files, duplicates


def txt_header(s3_client: Any, bucket: str, key: str) -> list[str]:
    response = s3_client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{HEADER_READ_BYTES - 1}")
    content = response["Body"].read()
    header_bytes = content.splitlines()[0] if content.splitlines() else b""
    if not header_bytes:
        raise BronzeQualityError(f"TXT file is empty: s3://{bucket}/{key}")
    header = header_bytes.decode("utf-8-sig", errors="strict")
    return [normalize(value) for value in header.split(";") if normalize(value)]


def finding(severity: str, code: str, message: str, **details: Any) -> dict[str, Any]:
    return {"severity": severity, "code": code, "message": message, "details": details}


def run_quality(
    target_bucket: str,
    year: str,
    known_missing: set[str],
    known_schema_divergences: set[str],
) -> dict[str, Any]:
    bronze_prefix = f"{year}/bronze"
    dictionary_key = f"{bronze_prefix}/dictionary.xlsx"
    report_key = f"{bronze_prefix}/quality.json"
    findings: list[dict[str, Any]] = []
    s3_client = boto3.client("s3")

    with tempfile.TemporaryDirectory(prefix="enade_bronze_quality_") as directory:
        dictionary_path = Path(directory) / "dictionary.xlsx"
        try:
            s3_client.download_file(target_bucket, dictionary_key, str(dictionary_path))
            expected_files = expected_files_from_dictionary(dictionary_path)
        except (BotoCoreError, ClientError, OSError, ValueError, zipfile.BadZipFile, BronzeQualityError) as error:
            findings.append(finding("CRITICAL", "DICTIONARY_UNREADABLE", str(error), dictionary_key=dictionary_key))
            expected_files = {}

    try:
        actual_files, duplicates = list_bronze_txt_files(s3_client, target_bucket, bronze_prefix)
    except (BotoCoreError, ClientError) as error:
        findings.append(finding("CRITICAL", "BRONZE_LIST_UNREADABLE", str(error), prefix=bronze_prefix))
        actual_files, duplicates = {}, []

    for file_name in duplicates:
        findings.append(finding("CRITICAL", "DUPLICATE_TXT_FILE", "More than one TXT has this base name.", file_name=file_name))

    for file_name, expected_columns in expected_files.items():
        key = actual_files.get(file_name)
        if not key:
            if file_name in known_missing:
                findings.append(
                    finding(
                        "WARNING",
                        "KNOWN_SOURCE_DIVERGENCE",
                        "Listed in DICIONÁRIO_ARQUIVOS but absent from the official ZIP; accepted exception.",
                        file_name=file_name,
                    )
                )
            else:
                findings.append(finding("CRITICAL", "EXPECTED_TXT_MISSING", "Expected TXT is absent from Bronze.", file_name=file_name))
            continue

        try:
            actual_columns = txt_header(s3_client, target_bucket, key)
        except (BotoCoreError, ClientError, UnicodeDecodeError, BronzeQualityError) as error:
            findings.append(finding("CRITICAL", "TXT_HEADER_UNREADABLE", str(error), file_name=file_name, key=key))
            continue

        if actual_columns != expected_columns:
            missing_columns = [column for column in expected_columns if column not in actual_columns]
            unexpected_columns = [column for column in actual_columns if column not in expected_columns]
            severity = "WARNING" if file_name in known_schema_divergences else "CRITICAL"
            code = "KNOWN_SOURCE_SCHEMA_DIVERGENCE" if severity == "WARNING" else "TXT_SCHEMA_MISMATCH"
            message = (
                "Official dictionary lists columns absent from the official TXT; accepted exception."
                if severity == "WARNING"
                else "TXT header differs from DICIONÁRIO_ARQUIVOS."
            )
            findings.append(
                finding(
                    severity,
                    code,
                    message,
                    file_name=file_name,
                    key=key,
                    missing_columns=missing_columns,
                    unexpected_columns=unexpected_columns,
                    expected_columns=expected_columns,
                    actual_columns=actual_columns,
                )
            )

    for file_name, key in actual_files.items():
        if file_name not in expected_files:
            findings.append(finding("CRITICAL", "UNEXPECTED_TXT_FILE", "TXT is not listed in DICIONÁRIO_ARQUIVOS.", file_name=file_name, key=key))

    critical_count = sum(item["severity"] == "CRITICAL" for item in findings)
    warning_count = sum(item["severity"] == "WARNING" for item in findings)
    return {
        "status": "FAILED" if critical_count else "PASSED_WITH_WARNINGS" if warning_count else "PASSED",
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "bucket": target_bucket,
        "bronze_prefix": bronze_prefix,
        "dictionary_key": dictionary_key,
        "report_key": report_key,
        "summary": {
            "expected_files": len(expected_files),
            "found_txt_files": len(actual_files),
            "critical_findings": critical_count,
            "warning_findings": warning_count,
        },
        "findings": findings,
    }


def main() -> None:
    target_bucket = required_argument("TARGET_BUCKET")
    year = resolve_year()
    if not year.isdigit() or len(year) != 4:
        raise BronzeQualityError("YEAR must contain exactly four digits.")
    known_missing = {value.strip() for value in optional_argument("KNOWN_MISSING_FILES", "").split(",") if value.strip()}
    known_schema_divergences = {
        value.strip()
        for value in optional_argument("KNOWN_SCHEMA_DIVERGENCES", "").split(",")
        if value.strip()
    }

    report = run_quality(target_bucket, year, known_missing, known_schema_divergences)
    s3_client = boto3.client("s3")
    try:
        s3_client.put_object(
            Bucket=target_bucket,
            Key=report["report_key"],
            Body=json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
    except (BotoCoreError, ClientError) as error:
        raise BronzeQualityError(f"Could not publish quality report: {error}") from error

    print(json.dumps(report, ensure_ascii=False))
    if report["status"] == "FAILED":
        raise BronzeQualityError(f"Bronze quality failed. Report: s3://{target_bucket}/{report['report_key']}")


if __name__ == "__main__":
    main()
