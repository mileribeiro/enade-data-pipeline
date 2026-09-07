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
      format("%s/scripts/bronze_ingestion.py", aws_s3_bucket.data_lake.arn),
      format("%s/scripts/bronze_quality.py", aws_s3_bucket.data_lake.arn),
    ]
  }

  statement {
    sid       = "ReadWorkflowRunProperties"
    effect    = "Allow"
    actions   = ["glue:GetWorkflowRunProperties"]
    resources = ["*"]
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

resource "aws_s3_object" "bronze_ingestion_script" {
  bucket                 = aws_s3_bucket.data_lake.id
  key                    = "scripts/bronze_ingestion.py"
  source                 = "${path.module}/../src/bronze_ingestion.py"
  etag                   = filemd5("${path.module}/../src/bronze_ingestion.py")
  server_side_encryption = "AES256"
}

resource "aws_glue_job" "bronze_ingestion" {
  name              = "${var.project_name}-bronze-ingestion"
  role_arn          = aws_iam_role.glue_bronze.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.data_lake.bucket}/${aws_s3_object.bronze_ingestion_script.key}"
  }

  default_arguments = {
    "--TARGET_BUCKET"                    = aws_s3_bucket.data_lake.bucket
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
    aws_s3_object.bronze_ingestion_script,
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

resource "aws_glue_trigger" "start_bronze_ingestion" {
  name          = "${var.project_name}-start-bronze-ingestion"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.pipeline.name

  actions {
    job_name = aws_glue_job.bronze_ingestion.name
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
      job_name = aws_glue_job.bronze_ingestion.name
      state    = "SUCCEEDED"
    }
  }
}
