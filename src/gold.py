"""Build course-grain Gold dimensions and performance fact from approved Silver data.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window


class GoldError(RuntimeError):
    pass


def optional_argument(name: str) -> str:
    flag = f"--{name}"
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else ""


def resolve_year() -> str:
    year = optional_argument("YEAR")
    if not year:
        raise GoldError("--YEAR is required.")
    return year


def required_columns(frame, columns: list[str], dataset: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise GoldError(f"{dataset} is missing required columns: {missing}")


def main() -> None:
    arguments = getResolvedOptions(sys.argv, ["JOB_NAME", "TARGET_BUCKET"])
    year = resolve_year()
    if not year.isdigit() or len(year) != 4:
        raise GoldError("YEAR must contain exactly four digits.")

    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    job = Job(glue_context)
    job.init(arguments["JOB_NAME"], arguments)
    spark = glue_context.spark_session
    bucket = arguments["TARGET_BUCKET"]
    silver = f"s3://{bucket}/{year}/silver"
    gold = f"s3://{bucket}/{year}/gold"
    attributes = spark.read.parquet(f"{silver}/microdados2023_arq1/")
    scores = spark.read.parquet(f"{silver}/microdados2023_arq3/")
    questionnaire_income = spark.read.parquet(f"{silver}/microdados2023_arq11/")
    questionnaire_perception = spark.read.parquet(f"{silver}/microdados2023_arq4/")
    ies_reference = spark.read.parquet(f"{silver}/dim_ies_reference/")
    dictionary_values = spark.read.parquet(f"{silver}/dim_variable_value/")
    attribute_columns = ["NU_ANO", "CO_CURSO", "CO_IES", "CO_GRUPO", "CO_MODALIDADE", "CO_UF_CURSO", "CO_REGIAO_CURSO"]
    required_columns(attributes, attribute_columns, "microdados2023_arq1")
    required_columns(scores, ["NU_ANO", "CO_CURSO", "NT_GER"], "microdados2023_arq3")
    required_columns(questionnaire_income, ["NU_ANO", "CO_CURSO", "QE_I05"], "microdados2023_arq11")
    required_columns(ies_reference, ["year", "co_ies", "no_ies", "sg_ies", "no_municipio", "sg_uf", "no_organizacao_academica", "no_categoria_administrativa", "situacao_ies"], "dim_ies_reference")
    required_columns(dictionary_values, ["year", "variable_name", "value_code", "value_label"], "dim_variable_value")

    course_source = attributes.select(*attribute_columns).dropDuplicates()
    conflicts = course_source.groupBy("CO_CURSO").count().where(F.col("count") > 1).limit(1).count()
    if conflicts:
        raise GoldError("Conflicting course attributes found for the same CO_CURSO.")

    dim_course = course_source.select(
        F.col("NU_ANO").alias("year"), F.col("CO_CURSO").alias("co_curso"),
        F.col("CO_IES").alias("co_ies"), F.col("CO_GRUPO").alias("co_grupo"),
        F.col("CO_MODALIDADE").alias("co_modalidade"), F.col("CO_UF_CURSO").alias("co_uf_curso"),
        F.col("CO_REGIAO_CURSO").alias("co_regiao_curso"),
    )
    ies_reference = ies_reference.select("year", "co_ies", "no_ies", "sg_ies", "no_mantenedora", "nu_cnpj_mantenedora", "no_municipio", "sg_uf", "no_organizacao_academica", "no_categoria_administrativa", "situacao_ies")
    if ies_reference.groupBy("year", "co_ies").count().where(F.col("count") > 1).limit(1).count():
        raise GoldError("IES reference contains duplicate CO_IES values.")
    dim_ies = dim_course.select("year", "co_ies").dropDuplicates().join(ies_reference, ["year", "co_ies"], "left")
    area_reference = dictionary_values.where(F.col("variable_name") == "CO_GRUPO").select(
        F.col("year"),
        F.col("value_code").alias("co_grupo"),
        F.col("value_label").alias("no_grupo"),
    )
    if area_reference.groupBy("year", "co_grupo").count().where(F.col("count") > 1).limit(1).count():
        raise GoldError("CO_GRUPO has duplicate labels in dim_variable_value.")
    dim_area = dim_course.select("year", "co_grupo").dropDuplicates().join(
        area_reference, ["year", "co_grupo"], "left"
    )
    modality_reference = dictionary_values.where(F.col("variable_name") == "CO_MODALIDADE").select(
        F.col("year"),
        F.col("value_code").alias("co_modalidade"),
        F.col("value_label").alias("no_modalidade"),
    )
    if modality_reference.groupBy("year", "co_modalidade").count().where(F.col("count") > 1).limit(1).count():
        raise GoldError("CO_MODALIDADE has duplicate labels in dim_variable_value.")
    dim_modalidade = dim_course.select("year", "co_modalidade").dropDuplicates().join(
        modality_reference, ["year", "co_modalidade"], "left"
    )

    score_value = F.col("NT_GER").cast(DecimalType(5, 2))
    fact = scores.groupBy(F.col("NU_ANO").alias("year"), F.col("CO_CURSO").alias("co_curso")).agg(
        F.count("*").alias("qtde_registros"),
        F.sum(F.when(score_value.isNotNull(), 1).otherwise(0)).cast("long").alias("qtde_notas_validas"),
        F.sum(F.when(score_value.isNull(), 1).otherwise(0)).cast("long").alias("qtde_notas_nulas"),
        F.sum(score_value).cast(DecimalType(18, 2)).alias("soma_nt_ger"),
        F.avg(score_value).cast(DecimalType(5, 2)).alias("media_nt_ger"),
    )
    processed_at = datetime.now(timezone.utc).isoformat()
    fact = fact.withColumn("_gold_processed_at", F.lit(processed_at))

    fact_ies_area = fact.join(
        dim_course.select("year", "co_curso", "co_ies", "co_grupo"),
        ["year", "co_curso"],
        "inner",
    ).groupBy("year", "co_ies", "co_grupo").agg(
        F.countDistinct("co_curso").alias("qtde_cursos"),
        F.sum("qtde_registros").alias("qtde_registros"),
        F.sum("qtde_notas_validas").alias("qtde_notas_validas"),
        F.sum("qtde_notas_nulas").alias("qtde_notas_nulas"),
        F.sum("soma_nt_ger").cast(DecimalType(18, 2)).alias("soma_nt_ger"),
    ).withColumn(
        "media_nt_ger",
        F.when(F.col("qtde_notas_validas") > 0, (F.col("soma_nt_ger") / F.col("qtde_notas_validas")).cast(DecimalType(5, 2))),
    )

    ies_area_named = fact_ies_area.join(
        dim_ies.select("year", "co_ies", "no_ies"),
        ["year", "co_ies"],
        "left",
    )
    ranking_window = Window.partitionBy("year", "co_grupo").orderBy(
        F.col("media_nt_ger").desc_nulls_last(),
        F.col("qtde_notas_validas").desc(),
        F.col("co_ies").asc(),
    )
    ranked_ies = ies_area_named.withColumn("ranking_area", F.row_number().over(ranking_window))
    best_ies = ranked_ies.where(F.col("ranking_area") == 1).select(
        "year", "co_grupo",
        F.col("co_ies").alias("melhor_co_ies"),
        F.col("no_ies").alias("melhor_no_ies"),
        F.col("media_nt_ger").alias("media_melhor_ies"),
    )
    unifor_scores = ies_area_named.where(F.col("co_ies") == "555").select(
        "year", "co_grupo",
        F.lit("555").alias("co_ies_unifor"),
        F.col("media_nt_ger").alias("media_unifor"),
    )
    fact_comparativo_ies_area = unifor_scores.join(best_ies, ["year", "co_grupo"], "left").withColumn(
        "diferenca_pontos",
        (F.col("media_melhor_ies") - F.col("media_unifor")).cast(DecimalType(5, 2)),
    ).select(
        "year", "co_grupo", "co_ies_unifor", "media_unifor",
        "melhor_co_ies", "melhor_no_ies", "media_melhor_ies", "diferenca_pontos",
    )

    income = questionnaire_income.select(
        F.col("NU_ANO").alias("year"), F.col("CO_CURSO").alias("co_curso"),
        F.lit("QE_I05").alias("variable_name"), F.col("QE_I05").cast("string").alias("response_code"),
    )
    perception_columns = [column for column in questionnaire_perception.columns if column.startswith("QE_I")]
    perception = None
    for column in perception_columns:
        current = questionnaire_perception.select(
            F.col("NU_ANO").alias("year"), F.col("CO_CURSO").alias("co_curso"),
            F.lit(column).alias("variable_name"), F.col(column).cast("string").alias("response_code"),
        )
        perception = current if perception is None else perception.unionByName(current)
    questionnaire = income.unionByName(perception) if perception is not None else income
    questionnaire = questionnaire.where(F.col("response_code").isNotNull() & (F.trim(F.col("response_code")) != ""))
    profile_counts = questionnaire.groupBy("year", "co_curso", "variable_name", "response_code").count().withColumnRenamed("count", "qtde_respostas")
    profile_window = Window.partitionBy("year", "co_curso", "variable_name")
    profile_fact = profile_counts.withColumn("qtde_respostas_variavel", F.sum("qtde_respostas").over(profile_window)).withColumn(
        "percentual_respostas", (F.col("qtde_respostas") / F.col("qtde_respostas_variavel")).cast(DecimalType(7, 4))
    ).join(
        dictionary_values.where(F.col("variable_name").isin(["QE_I05"] + perception_columns)).select(
            "year", "variable_name", F.col("value_code").alias("response_code"), F.col("value_label").alias("response_label")
        ),
        ["year", "variable_name", "response_code"],
        "left",
    ).join(
        fact.select("year", "co_curso", "media_nt_ger", "qtde_notas_validas"), ["year", "co_curso"], "left"
    ).join(
        dim_course.select("year", "co_curso", "co_ies", "co_grupo", "co_modalidade"), ["year", "co_curso"], "left"
    ).withColumn(
        "renda_faixa",
        F.when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "A"), 1)
        .when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "B"), 2)
        .when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "C"), 3)
        .when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "D"), 4)
        .when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "E"), 5)
        .when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "F"), 6)
        .when((F.col("variable_name") == "QE_I05") & (F.col("response_code") == "G"), 7)
        .otherwise(F.lit(None).cast(IntegerType()))
    ).select("year", "co_curso", "co_ies", "co_grupo", "co_modalidade", "variable_name", "response_code", "response_label", "renda_faixa", "qtde_respostas", "qtde_respostas_variavel", "percentual_respostas", "media_nt_ger", "qtde_notas_validas")

    fact_relacao_perfil_nota = profile_fact.groupBy(
        "year", "co_ies", "co_grupo", "co_modalidade", "variable_name", "response_code", "response_label", "renda_faixa"
    ).agg(
        F.countDistinct("co_curso").alias("qtde_cursos"),
        F.sum("qtde_respostas").alias("qtde_respostas"),
        F.sum("qtde_respostas_variavel").alias("qtde_respostas_variavel"),
        F.sum(F.when(F.col("media_nt_ger").isNotNull(), F.col("qtde_respostas")).otherwise(0)).alias("qtde_respostas_com_nota"),
        F.sum(F.when(F.col("media_nt_ger").isNotNull(), F.col("media_nt_ger") * F.col("qtde_respostas")).otherwise(0)).alias("soma_media_ponderada"),
    ).withColumn(
        "percentual_respostas",
        (F.col("qtde_respostas") / F.col("qtde_respostas_variavel")).cast(DecimalType(7, 4)),
    ).withColumn(
        "media_nt_ger_ponderada",
        F.when(F.col("qtde_respostas_com_nota") > 0, (F.col("soma_media_ponderada") / F.col("qtde_respostas_com_nota")).cast(DecimalType(5, 2))),
    ).drop("soma_media_ponderada")

    for name, frame in {"dim_course": dim_course, "dim_ies": dim_ies, "dim_area": dim_area, "dim_modalidade": dim_modalidade, "fato_desempenho_curso": fact, "fato_comparativo_ies_area": fact_comparativo_ies_area, "fato_perfil_nota_curso": fact_relacao_perfil_nota}.items():
        frame.write.mode("overwrite").parquet(f"{gold}/{name}/")
    print(f"Published Gold tables for {year}: {dim_course.count()} courses and {fact.count()} course performance rows.")
    job.commit()


if __name__ == "__main__":
    main()
