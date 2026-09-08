"""Validate Silver ENADE Parquet datasets against the normalized dictionary."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

import os
import boto3
from botocore.exceptions import BotoCoreError, ClientError

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


MAX_SAMPLE_VALUES = 20


class SilverQualityError(RuntimeError):
    """Raised when a Silver dataset violates its data contract."""


def optional_argument(name: str, default: str) -> str:
    flag = f"--{name}"
    if flag not in sys.argv:
        return default
    position = sys.argv.index(flag)
    if position + 1 >= len(sys.argv):
        raise SilverQualityError(f"Argument {flag} requires a value.")
    return sys.argv[position + 1]


def resolve_year() -> str:
    year = optional_argument("YEAR", "")
    if not year:
        raise SilverQualityError("--YEAR is required.")
    return year


def silver_dataset_names(s3_client: Any, bucket: str, data_prefix: str) -> list[str]:
    names: set[str] = set()
    for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{data_prefix}/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            relative = PurePosixPath(key).relative_to(data_prefix)
            if len(relative.parts) >= 2 and relative.suffix.lower() == ".parquet":
                names.add(relative.parts[0])
    reserved_names = {"data", "metadata", "dim_variable", "dim_variable_value", "dim_ies_reference"}
    return sorted(names - reserved_names)


def invalid_values(frame: Any, column_name: str, rule: dict[str, Any]) -> list[str]:
    values = frame.select(F.trim(F.col(column_name).cast("string")).alias("value")).where(F.col("value").isNotNull() & (F.col("value") != "")).distinct()
    if rule["rule_type"] == "ENUM":
        invalid = values.where(~F.col("value").isin(rule["allowed_values"]))
    elif rule["rule_type"] == "RANGE":
        numeric_value = F.col("value").cast("double")
        invalid = values.where(numeric_value.isNull() | (numeric_value < F.lit(float(rule["minimum_value"]))) | (numeric_value > F.lit(float(rule["maximum_value"]))))
    elif rule["rule_type"] == "FIXED":
        invalid = values.where(F.col("value") != F.lit(rule["minimum_value"]))
    elif rule["rule_type"] == "VECTOR_CHARSET":
        allowed_characters = "".join(sorted(set(rule["allowed_values"])))
        if not allowed_characters:
            return []
        invalid_condition = ~F.col("value").rlike(f"^[{re.escape(allowed_characters)}]+$")
        if rule.get("max_length"):
            invalid_condition = invalid_condition | (F.length("value") != F.lit(rule["max_length"]))
        invalid = values.where(invalid_condition)
    else:
        return []
    return [row["value"] for row in invalid.orderBy("value").limit(MAX_SAMPLE_VALUES + 1).collect()]


def nt_ger_quality(frame: Any, dataset: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    if "NT_GER" not in frame.columns:
        return None, []

    metrics_row = frame.agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(F.col("NT_GER").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("null_records"),
    ).first()
    total_records = int(metrics_row["total_records"])
    null_records = int(metrics_row["null_records"] or 0)
    metrics = {
        "dataset": dataset,
        "total_records": total_records,
        "null_records": null_records,
        "null_percentage": round((null_records / total_records) * 100, 2) if total_records else 0.0,
    }
    findings: list[dict[str, Any]] = []
    score_type = frame.schema["NT_GER"].dataType
    if not isinstance(score_type, DecimalType):
        findings.append({"severity": "CRITICAL", "code": "NT_GER_INVALID_TYPE", "dataset": dataset, "actual_type": str(score_type), "expected_type": "decimal(5,2)"})

    if "_nt_ger_raw" not in frame.columns:
        findings.append({"severity": "CRITICAL", "code": "NT_GER_RAW_LINEAGE_MISSING", "dataset": dataset})
        return metrics, findings

    raw_score = F.trim(F.col("_nt_ger_raw").cast("string"))
    invalid_conversion = frame.where(
        raw_score.isNotNull()
        & ~raw_score.isin("", ".")
        & F.col("NT_GER").isNull()
    ).select(raw_score.alias("value")).distinct().orderBy("value").limit(MAX_SAMPLE_VALUES + 1).collect()
    if invalid_conversion:
        findings.append({"severity": "CRITICAL", "code": "NT_GER_NOT_NUMERIC", "dataset": dataset, "invalid_value_sample": [row["value"] for row in invalid_conversion[:MAX_SAMPLE_VALUES]], "sample_truncated": len(invalid_conversion) > MAX_SAMPLE_VALUES})

    outside_range = frame.where(
        F.col("NT_GER").isNotNull()
        & ((F.col("NT_GER") < F.lit(0)) | (F.col("NT_GER") > F.lit(100)))
    ).select(F.col("NT_GER").cast("string").alias("value")).distinct().orderBy("value").limit(MAX_SAMPLE_VALUES + 1).collect()
    if outside_range:
        findings.append({"severity": "CRITICAL", "code": "NT_GER_OUTSIDE_RANGE", "dataset": dataset, "invalid_value_sample": [row["value"] for row in outside_range[:MAX_SAMPLE_VALUES]], "sample_truncated": len(outside_range) > MAX_SAMPLE_VALUES})
    return metrics, findings



def ies_reference_quality(frame: Any) -> tuple[int, list[dict[str, Any]]]:
    required_columns = {"year", "co_ies", "no_ies", "source_reference_key", "source_url", "source_staged_at", "source_row"}
    findings: list[dict[str, Any]] = []
    missing = sorted(required_columns - set(frame.columns))
    if missing:
        return 0, [{"severity": "CRITICAL", "code": "IES_REFERENCE_SCHEMA_INVALID", "columns": missing}]
    row_count = frame.count()
    if row_count == 0:
        findings.append({"severity": "CRITICAL", "code": "IES_REFERENCE_EMPTY"})
    if frame.where(F.col("co_ies").isNull() | (F.trim(F.col("co_ies")) == "") | F.col("no_ies").isNull() | (F.trim(F.col("no_ies")) == "")).limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "IES_REFERENCE_REQUIRED_VALUE_MISSING"})
    if frame.groupBy("year", "co_ies").count().where("count > 1").limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "IES_REFERENCE_DUPLICATE_CODE"})
    return row_count, findings


def main() -> None:
    arguments = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET"])
    year = resolve_year()
    if not year.isdigit() or len(year) != 4:
        raise SilverQualityError("YEAR must contain exactly four digits.")

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(arguments["JOB_NAME"], arguments)
    spark = glue_context.spark_session
    bucket = arguments["TARGET_BUCKET"]
    silver_path = f"s3://{bucket}/{year}/silver"
    variables = spark.read.parquet(f"{silver_path}/dim_variable/")
    values = spark.read.parquet(f"{silver_path}/dim_variable_value/")
    ies_reference = spark.read.parquet(f"{silver_path}/dim_ies_reference/")
    allowed_values = {row["variable_name"]: sorted(row["allowed_values"]) for row in values.groupBy("variable_name").agg(F.collect_set("value_code").alias("allowed_values")).collect()}
    rules = [row.asDict(recursive=True) for row in variables.where(F.col("rule_type").isin("ENUM", "RANGE", "FIXED", "VECTOR_CHARSET")).collect()]
    for rule in rules:
        rule["allowed_values"] = allowed_values.get(rule["variable_name"], [])
    documented_columns = {row["variable_name"] for row in variables.select("variable_name").collect()}

    data_prefix = f"{year}/silver"
    try:
        datasets = silver_dataset_names(boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL_S3") or os.getenv("AWS_ENDPOINT_URL")), bucket, data_prefix)
    except (BotoCoreError, ClientError) as error:
        raise SilverQualityError(f"Could not list Silver datasets: {error}") from error
    if not datasets:
        raise SilverQualityError(f"No Parquet datasets found in s3://{bucket}/{data_prefix}/")

    findings: list[dict[str, Any]] = []
    ies_reference_rows, ies_reference_findings = ies_reference_quality(ies_reference)
    findings.extend(ies_reference_findings)
    nt_ger_metrics: list[dict[str, Any]] = []
    checked_rules = 0
    for dataset in datasets:
        frame = spark.read.parquet(f"{silver_path}/{dataset}/")
        metrics, score_findings = nt_ger_quality(frame, dataset)
        if metrics:
            nt_ger_metrics.append(metrics)
        findings.extend(score_findings)
        business_columns = [column for column in frame.columns if not column.startswith("_")]
        unknown_columns = sorted(set(business_columns) - documented_columns)
        if unknown_columns:
            findings.append({"severity": "CRITICAL", "code": "SILVER_COLUMN_NOT_IN_DICTIONARY", "dataset": dataset, "columns": unknown_columns})
        if frame.limit(1).count() == 0:
            findings.append({"severity": "CRITICAL", "code": "EMPTY_SILVER_DATASET", "dataset": dataset})
        for rule in (item for item in rules if item["variable_name"] in business_columns):
            checked_rules += 1
            sample = invalid_values(frame, rule["variable_name"], rule)
            if sample:
                findings.append({"severity": "CRITICAL", "code": "VALUE_OUTSIDE_DOCUMENTED_DOMAIN", "dataset": dataset, "variable_name": rule["variable_name"], "rule_type": rule["rule_type"], "expected_length": rule.get("max_length"), "invalid_value_sample": sample[:MAX_SAMPLE_VALUES], "sample_truncated": len(sample) > MAX_SAMPLE_VALUES})

    report = {"status": "FAILED" if findings else "PASSED", "validated_at": datetime.now(timezone.utc).isoformat(), "bucket": bucket, "silver_prefix": f"{year}/silver", "report_key": f"{year}/silver/quality.json", "summary": {"datasets_checked": len(datasets), "rules_checked": checked_rules, "critical_findings": len(findings), "nt_ger": nt_ger_metrics, "ies_reference_rows": ies_reference_rows}, "findings": findings}
    try:
        boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL_S3") or os.getenv("AWS_ENDPOINT_URL")).put_object(Bucket=bucket, Key=report["report_key"], Body=json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8"), ContentType="application/json")
    except (BotoCoreError, ClientError) as error:
        raise SilverQualityError(f"Could not publish Silver quality report: {error}") from error
    print(json.dumps(report, ensure_ascii=False))
    if findings:
        raise SilverQualityError(f"Silver quality failed. Report: s3://{bucket}/{report['report_key']}")
    job.commit()


if __name__ == "__main__":
    main()
