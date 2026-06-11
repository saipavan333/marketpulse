# ============================================================================
# MarketPulse — AWS production reference architecture (Infrastructure as Code)
#
# Local-stack -> AWS mapping:
#   Kafka (compose)        -> Amazon MSK Serverless
#   MinIO                  -> Amazon S3 (+ lifecycle to Glacier)
#   Spark standalone       -> EMR Serverless (Spark 3.5)
#   Airflow (compose)      -> Amazon MWAA
#   Postgres warehouse     -> Amazon RDS Postgres (or Redshift/Snowflake at scale)
#
# This module is a reviewed, planned design — `terraform plan` is run in CI
# against a sandbox account; apply is gated behind manual approval.
# ============================================================================

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    # state bucket + dynamodb lock table created out-of-band (bootstrap)
    bucket         = "marketpulse-tfstate"
    key            = "platform/terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "marketpulse-tflock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = "marketpulse"
      managed_by = "terraform"
      env        = var.environment
    }
  }
}

# ------------------------------------------------------------------ S3 lake
resource "aws_s3_bucket" "lake" {
  bucket = "marketpulse-lake-${var.environment}"
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "aws:kms" }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "bronze-to-glacier"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ------------------------------------------------------------ MSK Serverless
resource "aws_msk_serverless_cluster" "events" {
  cluster_name = "marketpulse-${var.environment}"

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam { enabled = true }
    }
  }
}

resource "aws_security_group" "msk" {
  name_prefix = "marketpulse-msk-"
  vpc_id      = var.vpc_id
  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------------------------------------ EMR Serverless
resource "aws_emrserverless_application" "spark" {
  name          = "marketpulse-spark-${var.environment}"
  release_label = "emr-7.1.0" # Spark 3.5
  type          = "spark"

  maximum_capacity {
    cpu    = "64 vCPU"
    memory = "256 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

# --------------------------------------------------------------------- MWAA
resource "aws_mwaa_environment" "airflow" {
  name               = "marketpulse-${var.environment}"
  airflow_version    = "2.10.3"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa.arn
  source_bucket_arn  = aws_s3_bucket.lake.arn
  dag_s3_path        = "airflow/dags"

  network_configuration {
    security_group_ids = [aws_security_group.msk.id]
    subnet_ids         = slice(var.private_subnet_ids, 0, 2)
  }
}

resource "aws_iam_role" "mwaa" {
  name = "marketpulse-mwaa-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = ["airflow-env.amazonaws.com", "airflow.amazonaws.com"] }
    }]
  })
}

# ----------------------------------------------------------- RDS warehouse
resource "aws_db_instance" "warehouse" {
  identifier        = "marketpulse-warehouse-${var.environment}"
  engine            = "postgres"
  engine_version    = "16.3"
  instance_class    = var.warehouse_instance_class
  allocated_storage = 100
  storage_encrypted = true

  db_name                 = "marketpulse"
  username                = "marketpulse"
  manage_master_user_password = true # password lives in Secrets Manager

  multi_az                = var.environment == "prod"
  backup_retention_period = 7
  skip_final_snapshot     = var.environment != "prod"
  deletion_protection     = var.environment == "prod"
}
