"""Build the Silver layer from the immutable ENADE Bronze files.

Inputs:
    s3://<bucket>/<year>/bronze/*.txt
    s3://<bucket>/<year>/bronze/dictionary.xlsx

Outputs:
    s3://<bucket>/<year>/silver/<source-file>/
    s3://<bucket>/<year>/silver/dim_variable/
    s3://<bucket>/<year>/silver/dim_variable_value/

Codes remain strings in Silver to preserve identifiers and leading zeroes. NT_GER, the
score required by the case, is converted to DECIMAL(5,2) before Gold aggregation.
"""

from __future__ import annotations

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

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DecimalType, IntegerType, StringType, StructField, StructType


XLSX_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELATIONSHIP_NAMESPACE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE_RELATIONSHIP_NAMESPACE = "http://schemas.openxmlformats.org/package/2006/relationships"
NAMESPACES = {"xlsx": XLSX_NAMESPACE}
VARIABLE_SHEET_NAME = "DICIONÁRIO DE VARIÁVEIS"


class SilverTransformationError(RuntimeError):
    """Raised when the Silver layer cannot be produced safely."""


def optional_argument(name: str, default: str) -> str:
    flag = f"--{name}"
    if flag not in sys.argv:
        return default
    position = sys.argv.index(flag)
    if position + 1 >= len(sys.argv):
        raise SilverTransformationError(f"Argument {flag} requires a value.")
    return sys.argv[position + 1]


def resolve_year() -> str:
    direct_year = optional_argument("YEAR", "")
    if direct_year:
        return direct_year
    workflow_name = optional_argument("WORKFLOW_NAME", "")
    workflow_run_id = optional_argument("WORKFLOW_RUN_ID", "")
    if not workflow_name or not workflow_run_id:
        raise SilverTransformationError("YEAR is required for a direct run or as a Workflow run property.")
    try:
        properties = boto3.client("glue").get_workflow_run_properties(
            Name=workflow_name,
            RunId=workflow_run_id,
        )["RunProperties"]
    except (BotoCoreError, ClientError) as error:
        raise SilverTransformationError(f"Could not read Workflow run properties: {error}") from error
    return properties.get("YEAR", "")


def normalize_header(value: str) -> str:
    return unicodedata.normalize("NFKC", value).replace("\ufeff", "").strip().upper()


def normalize_column_name(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^A-Z0-9]+", "_", normalize_header(ascii_value))
    return normalized.strip("_")


def clean_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).split())


def cell_column(reference: str) -> str:
    match = re.match(r"([A-Z]+)", reference)
    if not match:
        raise SilverTransformationError(f"Invalid XLSX cell reference: {reference}")
    return match.group(1)


def shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    return ["".join(node.text or "" for node in item.iter(f"{{{XLSX_NAMESPACE}}}t")) for item in root]


def worksheet_path(archive: zipfile.ZipFile, sheet_name: str) -> str:
    workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
    relationship_id = next(
        (
            sheet.attrib.get(f"{{{RELATIONSHIP_NAMESPACE}}}id")
            for sheet in workbook.findall("xlsx:sheets/xlsx:sheet", NAMESPACES)
            if sheet.attrib.get("name") == sheet_name
        ),
        None,
    )
    if not relationship_id:
        raise SilverTransformationError(f"XLSX sheet {sheet_name!r} was not found.")
    relationships = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    for relationship in relationships.findall(f"{{{PACKAGE_RELATIONSHIP_NAMESPACE}}}Relationship"):
        if relationship.attrib.get("Id") == relationship_id:
            return f"xl/{relationship.attrib['Target'].lstrip('/')}"
    raise SilverTransformationError(f"XLSX relationship for sheet {sheet_name!r} was not found.")


def cell_value(cell: ElementTree.Element, strings: list[str]) -> str:
    if cell.attrib.get("t") == "inlineStr":
        return "".join(node.text or "" for node in cell.iter(f"{{{XLSX_NAMESPACE}}}t"))
    value = cell.find("xlsx:v", NAMESPACES)
    if value is None or value.text is None:
        return ""
    return strings[int(value.text)] if cell.attrib.get("t") == "s" else value.text


def sheet_rows(dictionary_path: Path) -> list[dict[str, str]]:
    with zipfile.ZipFile(dictionary_path) as archive:
        strings = shared_strings(archive)
        root = ElementTree.fromstring(archive.read(worksheet_path(archive, VARIABLE_SHEET_NAME)))
        return [
            {
                cell_column(cell.attrib["r"]): cell_value(cell, strings)
                for cell in row.findall("xlsx:c", NAMESPACES)
            }
            for row in root.findall(".//xlsx:sheetData/xlsx:row", NAMESPACES)
        ]


def vector_character_values(variable_name: str, categories: list[str]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    if variable_name.startswith("DS_VT_ESC_"):
        # OCE omits the A-E interval; the equivalent OFG definition states it explicitly.
        values.extend((letter, f"Símbolo permitido ({letter})") for letter in "ABCDE")
    for category in categories:
        text = clean_text(category)
        range_match = re.search(r"intervalo de ([A-Z]) a ([A-Z])", text, flags=re.IGNORECASE)
        if range_match:
            start, end = (letter.upper() for letter in range_match.groups())
            values.extend((chr(code), f"Símbolo permitido ({chr(code)})") for code in range(ord(start), ord(end) + 1))
        for match in re.finditer(r'(?:"([^"]+)"|([^\s=,]+))\s*=\s*([^,]+)', text):
            code = (match.group(1) or match.group(2)).strip()
            if len(code) == 1:
                values.append((code, match.group(3).strip()))
    return list(dict.fromkeys(values))


def rule_from_categories(variable_name: str, categories: list[str]) -> tuple[str, str | None, str | None, list[tuple[str, str]]]:
    values = [clean_text(value) for value in categories if clean_text(value)]
    if not values:
        return "NONE", None, None, []
    if variable_name.startswith("DS_VT_"):
        vector_values = vector_character_values(variable_name, values)
        return ("VECTOR_CHARSET", None, None, vector_values) if vector_values else ("UNSTRUCTURED", None, None, [])

    numeric = r"-?\d+(?:[.,]\d+)?"
    range_match = re.search(rf"(?:valores?\s+)?entre\s+({numeric})\s+e\s+({numeric})", values[0], flags=re.IGNORECASE)
    min_max_match = re.search(rf"Min\s*=\s*({numeric})\s+Max\s*=\s*({numeric})", values[0], flags=re.IGNORECASE)
    if len(values) == 1 and (range_match or min_max_match):
        match = range_match or min_max_match
        return "RANGE", match.group(1).replace(",", "."), match.group(2).replace(",", "."), []
    if len(values) == 1 and re.fullmatch(numeric, values[0]):
        return "FIXED", values[0].replace(",", "."), values[0].replace(",", "."), []

    parsed_values = []
    for value in values:
        match = re.fullmatch(r"([^.=]+?)\s*(?:=|\.)\s*(.+)", value)
        if not match:
            return "UNSTRUCTURED", None, None, []
        parsed_values.append((match.group(1).strip(), match.group(2).strip()))
    return "ENUM", None, None, parsed_values


def normalize_dictionary(rows: list[dict[str, str]], year: str, source_key: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    header_index = next((index for index, row in enumerate(rows) if "NOME" in {normalize_header(value) for value in row.values()}), None)
    if header_index is None:
        raise SilverTransformationError("DICIONÁRIO DE VARIÁVEIS header was not found.")
    header = {normalize_header(value): column for column, value in rows[header_index].items()}
    required_headers = {"NOME", "TIPO", "TAMANHO", "DESCRIÇÃO", "CATEGORIAS"}
    if not required_headers.issubset(header):
        raise SilverTransformationError("DICIONÁRIO DE VARIÁVEIS does not contain the expected columns.")

    variables: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for source_row, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        name = clean_text(row.get(header["NOME"], ""))
        data_type = clean_text(row.get(header["TIPO"], ""))
        description = clean_text(row.get(header["DESCRIÇÃO"], ""))
        category = clean_text(row.get(header["CATEGORIAS"], ""))
        if name and (data_type or description or category):
            size = clean_text(row.get(header["TAMANHO"], ""))
            current = {"year": year, "variable_name": name, "data_type": data_type or None, "max_length": int(size) if size.isdigit() else None, "description": description or None, "categories_raw": [], "source_row": source_row, "source_dictionary_key": source_key}
            variables.append(current)
        if current and category:
            current["categories_raw"].append(category)

    duplicate_names = {item["variable_name"] for item in variables if sum(row["variable_name"] == item["variable_name"] for row in variables) > 1}
    if duplicate_names:
        raise SilverTransformationError(f"Duplicate variable definitions: {sorted(duplicate_names)}")

    value_rows: list[dict[str, Any]] = []
    for variable in variables:
        rule_type, minimum, maximum, values = rule_from_categories(variable["variable_name"], variable["categories_raw"])
        variable.update(rule_type=rule_type, minimum_value=minimum, maximum_value=maximum)
        value_rows.extend({"year": year, "variable_name": variable["variable_name"], "value_code": code, "value_label": label, "source_dictionary_key": source_key} for code, label in values)
    return variables, value_rows


def bronze_txt_keys(s3_client: Any, bucket: str, bronze_prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{bronze_prefix}/"):
        for item in page.get("Contents", []):
            key = item["Key"]
            relative = PurePosixPath(key).relative_to(bronze_prefix)
            if len(relative.parts) == 1 and relative.suffix.lower() == ".txt":
                keys.append(key)
    return sorted(keys)


def clean_bronze_frame(frame: Any, source_file: str, processed_at: str) -> Any:
    columns = [normalize_column_name(column) for column in frame.columns]
    if not all(columns) or len(columns) != len(set(columns)):
        raise SilverTransformationError(f"Invalid or duplicated normalized columns in {source_file}: {columns}")
    cleaned = frame.toDF(*columns)
    for column in columns:
        value = F.trim(F.col(column).cast("string"))
        cleaned = cleaned.withColumn(column, F.when(value == "", F.lit(None).cast("string")).otherwise(value))

    if "NT_GER" in columns:
        raw_score = F.col("NT_GER")
        normalized_score = F.regexp_replace(raw_score, ",", ".")
        score = F.when(
            raw_score.isNull() | raw_score.isin("", "."),
            F.lit(None).cast(DecimalType(5, 2)),
        ).otherwise(normalized_score.cast(DecimalType(5, 2)))
        cleaned = cleaned.withColumn("_nt_ger_raw", raw_score).withColumn("NT_GER", score)

    return cleaned.withColumn("_source_file", F.lit(source_file)).withColumn("_silver_processed_at", F.lit(processed_at))


VARIABLE_SCHEMA = StructType([StructField("year", StringType(), False), StructField("variable_name", StringType(), False), StructField("data_type", StringType(), True), StructField("max_length", IntegerType(), True), StructField("description", StringType(), True), StructField("categories_raw", ArrayType(StringType()), False), StructField("rule_type", StringType(), False), StructField("minimum_value", StringType(), True), StructField("maximum_value", StringType(), True), StructField("source_row", IntegerType(), False), StructField("source_dictionary_key", StringType(), False)])
VALUE_SCHEMA = StructType([StructField("year", StringType(), False), StructField("variable_name", StringType(), False), StructField("value_code", StringType(), False), StructField("value_label", StringType(), False), StructField("source_dictionary_key", StringType(), False)])


def main() -> None:
    arguments = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET"])
    year = resolve_year()
    if not year.isdigit() or len(year) != 4:
        raise SilverTransformationError("YEAR must contain exactly four digits.")

    bucket = arguments["TARGET_BUCKET"]
    bronze_prefix = f"{year}/bronze"
    source_key = f"{bronze_prefix}/dictionary.xlsx"
    with tempfile.TemporaryDirectory(prefix="enade_dictionary_") as directory:
        dictionary_path = Path(directory) / "dictionary.xlsx"
        try:
            s3_client = boto3.client("s3")
            s3_client.download_file(bucket, source_key, str(dictionary_path))
            variables, values = normalize_dictionary(sheet_rows(dictionary_path), year, source_key)
            txt_keys = bronze_txt_keys(s3_client, bucket, bronze_prefix)
        except (BotoCoreError, ClientError) as error:
            raise SilverTransformationError(f"Could not read Bronze inputs: {error}") from error
    if not txt_keys:
        raise SilverTransformationError(f"No TXT files found in s3://{bucket}/{bronze_prefix}/")

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(arguments["JOB_NAME"], arguments)
    spark = glue_context.spark_session
    silver_prefix = f"s3://{bucket}/{year}/silver"
    spark.createDataFrame(variables, VARIABLE_SCHEMA).write.mode("overwrite").parquet(f"{silver_prefix}/dim_variable/")
    spark.createDataFrame(values, VALUE_SCHEMA).write.mode("overwrite").parquet(f"{silver_prefix}/dim_variable_value/")

    processed_at = datetime.now(timezone.utc).isoformat()
    for key in txt_keys:
        source_file = PurePosixPath(key).name
        dataset_name = PurePosixPath(key).stem
        bronze_frame = spark.read.option("header", "true").option("sep", ";").option("encoding", "ISO-8859-1").csv(f"s3://{bucket}/{key}")
        clean_bronze_frame(bronze_frame, source_file, processed_at).write.mode("overwrite").parquet(f"{silver_prefix}/{dataset_name}/")

    print(f"Published {len(txt_keys)} Silver datasets, {len(variables)} variables and {len(values)} dictionary values.")
    job.commit()


if __name__ == "__main__":
    main()
