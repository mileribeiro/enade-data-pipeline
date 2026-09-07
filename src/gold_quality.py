"""Validate Gold dimensional integrity and course performance aggregates."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


class GoldQualityError(RuntimeError):
    pass


def optional_argument(name: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else ""


def resolve_year() -> str:
    year = optional_argument("YEAR")
    if not year:
        raise GoldQualityError("--YEAR is required.")
    return year


def main() -> None:
    arguments = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET"])
    year = resolve_year()
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(arguments["JOB_NAME"], arguments)
    spark = glue_context.spark_session
    bucket = arguments["TARGET_BUCKET"]
    gold = f"s3://{bucket}/{year}/gold"
    course = spark.read.parquet(f"{gold}/dim_course/")
    ies = spark.read.parquet(f"{gold}/dim_ies/")
    area = spark.read.parquet(f"{gold}/dim_area/")
    modalidade = spark.read.parquet(f"{gold}/dim_modalidade/")
    fact = spark.read.parquet(f"{gold}/fato_desempenho_curso/")
    findings = []
    if course.groupBy("year", "co_curso").count().where("count > 1").limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "DUPLICATE_COURSE"})
    if ies.where(F.col("no_ies").isNull() | (F.trim(F.col("no_ies")) == "")).limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "IES_WITHOUT_PUBLIC_REFERENCE"})
    if fact.groupBy("year", "co_curso").count().where("count > 1").limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "DUPLICATE_FACT_GRAIN"})
    for dimension, keys in {
        "dim_ies": ["year", "co_ies"],
        "dim_area": ["year", "co_grupo"],
        "dim_modalidade": ["year", "co_modalidade"],
    }.items():
        frame = {"dim_ies": ies, "dim_area": area, "dim_modalidade": modalidade}[dimension]
        if frame.groupBy(*keys).count().where("count > 1").limit(1).count():
            findings.append({"severity": "CRITICAL", "code": "DUPLICATE_DIMENSION_KEY", "dimension": dimension})
        if course.join(frame.select(*keys), keys, "left_anti").limit(1).count():
            findings.append({"severity": "CRITICAL", "code": "COURSE_WITHOUT_DIMENSION", "dimension": dimension})
    if fact.where(F.col("qtde_registros") <= 0).limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "EMPTY_FACT_GROUP"})
    if fact.where(F.col("qtde_registros") != F.col("qtde_notas_validas") + F.col("qtde_notas_nulas")).limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "INVALID_SCORE_COUNTS"})
    if fact.where(F.col("media_nt_ger").isNotNull() & ~F.col("media_nt_ger").between(0, 100)).limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "AVERAGE_OUT_OF_RANGE"})
    if fact.join(course.select("year", "co_curso"), ["year", "co_curso"], "left_anti").limit(1).count():
        findings.append({"severity": "CRITICAL", "code": "FACT_WITHOUT_COURSE"})
    report = {"status": "FAILED" if findings else "PASSED", "validated_at": datetime.now(timezone.utc).isoformat(), "report_key": f"{year}/gold/quality.json", "summary": {"critical_findings": len(findings)}, "findings": findings}
    boto3.client("s3").put_object(Bucket=bucket, Key=report["report_key"], Body=json.dumps(report, indent=2).encode("utf-8"), ContentType="application/json")
    print(json.dumps(report))
    if findings:
        raise GoldQualityError(f"Gold quality failed. Report: s3://{bucket}/{report['report_key']}")
    job.commit()


if __name__ == "__main__":
    main()
