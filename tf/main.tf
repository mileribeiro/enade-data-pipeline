provider "aws" {
  region = var.aws_region
}

locals {
  common_tags = {
    Project   = var.project_name
    ManagedBy = "Terraform"
  }
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data"
  tags   = local.common_tags
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Glue needs a technical temporary location, removed after seven days.
resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "expire-glue-temporary-files"
    status = "Enabled"

    filter {
      prefix = "_temporary/"
    }

    expiration {
      days = 7
    }
  }
}

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "glue_bronze" {
  name               = "${var.project_name}-bronze-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_bronze.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_bronze_s3" {
  statement {
    sid       = "ListDataLakeBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.data_lake.arn]
  }

  statement {
    sid    = "ReadGlueScript"
    effect = "Allow"
    actions = [
      "s3:GetObject",
    ]
    resources = [
      format("%s/scripts/bronze.py", aws_s3_bucket.data_lake.arn),
      format("%s/scripts/bronze_quality.py", aws_s3_bucket.data_lake.arn),
    ]
  }

  statement {
    sid    = "ReadAndWriteBronzeData"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = [
      format("%s/*/bronze/*", aws_s3_bucket.data_lake.arn),
      format("%s/_temporary/*", aws_s3_bucket.data_lake.arn),
    ]
  }
}

resource "aws_iam_role_policy" "glue_bronze_s3" {
  name   = "${var.project_name}-bronze-s3-access"
  role   = aws_iam_role.glue_bronze.id
  policy = data.aws_iam_policy_document.glue_bronze_s3.json
}

resource "aws_s3_object" "bronze_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/bronze.py"
  source                 = "${path.module}/../src/bronze.py"
  etag                   = filemd5("${path.module}/../src/bronze.py")
  server_side_encryption = "AES256"
}

# Source files staged locally in data/ and uploaded to the Bronze archive.
resource "aws_s3_object" "bronze_source_archive" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "2023/bronze/archive/microdados_enade_2023.zip"
  source                 = "${path.module}/../data/microdados_enade_2023.zip"
  etag                   = filemd5("${path.module}/../data/microdados_enade_2023.zip")
  content_type           = "application/zip"
  server_side_encryption = "AES256"
}

# Public e-MEC reference used to identify institutions in the ENADE data.
resource "aws_s3_object" "ies_reference_archive" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "2023/bronze/archive/ies.xls"
  source                 = "${path.module}/../data/ies.xls"
  etag                   = filemd5("${path.module}/../data/ies.xls")
  content_type           = "text/html; charset=utf-8"
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "bronze" {
  name              = "${var.project_name}-bronze"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.bronze_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
    "--YEAR"                             = "2023"
    "--EXPECTED_TEXT_FILES"              = "32"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/_temporary/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_bronze_s3,
    aws_s3_object.bronze_script,
    aws_s3_object.bronze_source_archive,
    aws_s3_object.ies_reference_archive,
  ]
}

resource "aws_s3_object" "bronze_quality_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/bronze_quality.py"
  source                 = "${path.module}/../src/bronze_quality.py"
  etag                   = filemd5("${path.module}/../src/bronze_quality.py")
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "bronze_quality" {
  name         = "${var.project_name}-bronze-quality"
  role_arn     = aws_iam_role.glue_bronze.arn
  max_capacity = 0.0625
  max_retries  = 0
  timeout      = 30

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.bronze_quality_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
    "--YEAR"                             = "2023"
    "--KNOWN_MISSING_FILES"              = "microdados2023_arq33"
    "--KNOWN_SCHEMA_DIVERGENCES"         = "microdados2023_arq3"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_bronze_s3,
    aws_s3_object.bronze_quality_script,
  ]
}

resource "aws_glue_workflow" "pipeline" {
  name = "${var.project_name}-pipeline"

  tags = local.common_tags
}

resource "aws_glue_trigger" "start_bronze" {
  name          = "${var.project_name}-start-bronze"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.pipeline.name

  actions {
    job_name = aws_glue_job.bronze.name
  }
}

resource "aws_glue_trigger" "start_bronze_quality" {
  name              = "${var.project_name}-start-bronze-quality"
  type              = "CONDITIONAL"
  workflow_name     = aws_glue_workflow.pipeline.name
  start_on_creation = true

  actions {
    job_name = aws_glue_job.bronze_quality.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.bronze.name
      state    = "SUCCEEDED"
    }
  }
}


data "aws_iam_policy_document" "glue_silver" {
  statement {
    sid     = "ReadSilverScripts"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      format("%s/scripts/silver.py", aws_s3_bucket.data_lake.arn),
      format("%s/scripts/silver_quality.py", aws_s3_bucket.data_lake.arn),
    ]
  }

  statement {
    sid       = "ReadBronzeDictionaryAndFiles"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [format("%s/*/bronze/*", aws_s3_bucket.data_lake.arn)]
  }

  statement {
    sid    = "ReadAndWriteSilverLayer"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetObject",
      "s3:ListMultipartUploadParts",
      "s3:PutObject",
    ]
    resources = [format("%s/*/silver/*", aws_s3_bucket.data_lake.arn)]
  }
}

resource "aws_iam_role_policy" "glue_silver" {
  name   = "${var.project_name}-silver-access"
  role   = aws_iam_role.glue_bronze.id
  policy = data.aws_iam_policy_document.glue_silver.json
}

resource "aws_s3_object" "silver_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/silver.py"
  source                 = "${path.module}/../src/silver.py"
  etag                   = filemd5("${path.module}/../src/silver.py")
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "silver" {
  name              = "${var.project_name}-silver"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.silver_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
    "--YEAR"                             = "2023"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/_temporary/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_bronze_s3,
    aws_iam_role_policy.glue_silver,
    aws_s3_object.silver_script,
  ]
}

resource "aws_s3_object" "silver_quality_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/silver_quality.py"
  source                 = "${path.module}/../src/silver_quality.py"
  etag                   = filemd5("${path.module}/../src/silver_quality.py")
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "silver_quality" {
  name              = "${var.project_name}-silver-quality"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.silver_quality_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
    "--YEAR"                             = "2023"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/_temporary/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.common_tags

  depends_on = [
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_bronze_s3,
    aws_iam_role_policy.glue_silver,
    aws_s3_object.silver_quality_script,
  ]
}

resource "aws_glue_trigger" "start_silver" {
  name              = "${var.project_name}-start-silver"
  type              = "CONDITIONAL"
  workflow_name     = aws_glue_workflow.pipeline.name
  start_on_creation = true

  actions {
    job_name = aws_glue_job.silver.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.bronze_quality.name
      state    = "SUCCEEDED"
    }
  }
}

resource "aws_glue_trigger" "start_silver_quality" {
  name              = "${var.project_name}-start-silver-quality"
  type              = "CONDITIONAL"
  workflow_name     = aws_glue_workflow.pipeline.name
  start_on_creation = true

  actions {
    job_name = aws_glue_job.silver_quality.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.silver.name
      state    = "SUCCEEDED"
    }
  }
}


resource "aws_athena_workgroup" "analytics" {
  name = "${var.project_name}-analytics"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.common_tags
}

resource "aws_glue_catalog_database" "analytics" {
  name        = "${var.project_name}_analytics"
  description = "Athena catalog for the ENADE data pipeline."
}

resource "aws_glue_catalog_table" "dim_variable" {
  name          = "dim_variable"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/2023/silver/dim_variable/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "year"
      type = "string"
    }

    columns {
      name = "variable_name"
      type = "string"
    }

    columns {
      name = "data_type"
      type = "string"
    }

    columns {
      name = "max_length"
      type = "int"
    }

    columns {
      name = "description"
      type = "string"
    }

    columns {
      name = "categories_raw"
      type = "array<string>"
    }

    columns {
      name = "rule_type"
      type = "string"
    }

    columns {
      name = "minimum_value"
      type = "string"
    }

    columns {
      name = "maximum_value"
      type = "string"
    }

    columns {
      name = "source_row"
      type = "int"
    }

    columns {
      name = "source_dictionary_key"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_variable_value" {
  name          = "dim_variable_value"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/2023/silver/dim_variable_value/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "year"
      type = "string"
    }

    columns {
      name = "variable_name"
      type = "string"
    }

    columns {
      name = "value_code"
      type = "string"
    }

    columns {
      name = "value_label"
      type = "string"
    }

    columns {
      name = "source_dictionary_key"
      type = "string"
    }
  }
}


data "aws_iam_policy_document" "glue_gold" {
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [format("%s/scripts/gold.py", aws_s3_bucket.data_lake.arn), format("%s/scripts/gold_quality.py", aws_s3_bucket.data_lake.arn)]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [format("%s/*/silver/*", aws_s3_bucket.data_lake.arn)]
  }

  statement {
    effect  = "Allow"
    actions = ["s3:AbortMultipartUpload", "s3:DeleteObject", "s3:GetObject", "s3:ListMultipartUploadParts", "s3:PutObject"]
    resources = [
      format("%s/*/gold/*", aws_s3_bucket.data_lake.arn),
      format("%s/*/gold_$folder$", aws_s3_bucket.data_lake.arn),
    ]
  }
}

resource "aws_iam_role_policy" "glue_gold" {
  name   = "${var.project_name}-gold-access"
  role   = aws_iam_role.glue_bronze.id
  policy = data.aws_iam_policy_document.glue_gold.json
}

resource "aws_s3_object" "gold_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/gold.py"
  source                 = "${path.module}/../src/gold.py"
  etag                   = filemd5("${path.module}/../src/gold.py")
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "gold" {
  name              = "${var.project_name}-gold"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.gold_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
    "--YEAR"                             = "2023"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/_temporary/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property { max_concurrent_runs = 1 }
  tags       = local.common_tags
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy.glue_gold, aws_s3_object.gold_script]
}

resource "aws_s3_object" "gold_quality_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/gold_quality.py"
  source                 = "${path.module}/../src/gold_quality.py"
  etag                   = filemd5("${path.module}/../src/gold_quality.py")
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "gold_quality" {
  name              = "${var.project_name}-gold-quality"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 30

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.gold_quality_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
    "--YEAR"                             = "2023"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/_temporary/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  execution_property { max_concurrent_runs = 1 }
  tags       = local.common_tags
  depends_on = [aws_iam_role_policy_attachment.glue_service, aws_iam_role_policy.glue_gold, aws_s3_object.gold_quality_script]
}

resource "aws_glue_trigger" "start_gold" {
  name              = "${var.project_name}-start-gold"
  type              = "CONDITIONAL"
  workflow_name     = aws_glue_workflow.pipeline.name
  start_on_creation = true
  actions {
    job_name = aws_glue_job.gold.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.silver_quality.name
      state    = "SUCCEEDED"
    }
  }
}

resource "aws_glue_trigger" "start_gold_quality" {
  name              = "${var.project_name}-start-gold-quality"
  type              = "CONDITIONAL"
  workflow_name     = aws_glue_workflow.pipeline.name
  start_on_creation = true
  actions {
    job_name = aws_glue_job.gold_quality.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.gold.name
      state    = "SUCCEEDED"
    }
  }
}
locals {
  gold_catalog_tables = {
    dim_course = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_curso", type = "string" },
        { name = "co_ies", type = "string" },
        { name = "co_grupo", type = "string" },
        { name = "co_modalidade", type = "string" },
        { name = "co_uf_curso", type = "string" },
        { name = "co_regiao_curso", type = "string" },
      ]
    }

    dim_ies = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_ies", type = "string" },
        { name = "no_ies", type = "string" },
        { name = "sg_ies", type = "string" },
        { name = "no_mantenedora", type = "string" },
        { name = "nu_cnpj_mantenedora", type = "string" },
        { name = "no_municipio", type = "string" },
        { name = "sg_uf", type = "string" },
        { name = "no_organizacao_academica", type = "string" },
        { name = "no_categoria_administrativa", type = "string" },
        { name = "situacao_ies", type = "string" },
      ]
    }

    dim_area = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_grupo", type = "string" },
        { name = "no_grupo", type = "string" },
      ]
    }

    dim_modalidade = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_modalidade", type = "string" },
        { name = "no_modalidade", type = "string" },
      ]
    }

    fato_comparativo_ies_area = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_grupo", type = "string" },
        { name = "co_ies_unifor", type = "string" },
        { name = "media_unifor", type = "decimal(5,2)" },
        { name = "melhor_co_ies", type = "string" },
        { name = "melhor_no_ies", type = "string" },
        { name = "media_melhor_ies", type = "decimal(5,2)" },
        { name = "diferenca_pontos", type = "decimal(5,2)" },
      ]
    }

    fato_perfil_nota_curso = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_ies", type = "string" },
        { name = "co_grupo", type = "string" },
        { name = "co_modalidade", type = "string" },
        { name = "variable_name", type = "string" },
        { name = "response_code", type = "string" },
        { name = "response_label", type = "string" },
        { name = "renda_faixa", type = "int" },
        { name = "qtde_cursos", type = "bigint" },
        { name = "qtde_respostas", type = "bigint" },
        { name = "qtde_respostas_variavel", type = "bigint" },
        { name = "qtde_respostas_com_nota", type = "bigint" },
        { name = "percentual_respostas", type = "decimal(7,4)" },
        { name = "media_nt_ger_ponderada", type = "decimal(5,2)" },
      ]
    }

    fato_desempenho_curso = {
      columns = [
        { name = "year", type = "string" },
        { name = "co_curso", type = "string" },
        { name = "qtde_registros", type = "bigint" },
        { name = "qtde_notas_validas", type = "bigint" },
        { name = "qtde_notas_nulas", type = "bigint" },
        { name = "soma_nt_ger", type = "decimal(18,2)" },
        { name = "media_nt_ger", type = "decimal(5,2)" },
        { name = "_gold_processed_at", type = "string" },
      ]
    }
  }
}

resource "aws_glue_catalog_table" "gold" {
  for_each      = local.gold_catalog_tables
  name          = each.key
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL       = "TRUE"
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/2023/gold/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
