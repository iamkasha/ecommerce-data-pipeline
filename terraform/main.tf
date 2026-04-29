terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "ecommerce-pipeline-tfstate"
    key    = "terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Data Lake ──────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-data-lake-${var.environment}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    id     = "archive-old-bronze"
    status = "Enabled"
    filter { prefix = "bronze/" }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
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

# ── Redshift Serverless ───────────────────────────────────────────────────────
resource "aws_redshiftserverless_namespace" "warehouse" {
  namespace_name      = "${var.project_name}-${var.environment}"
  admin_username      = var.redshift_admin_user
  admin_user_password = var.redshift_admin_password
  db_name             = "warehouse"
  tags                = local.common_tags
}

resource "aws_redshiftserverless_workgroup" "warehouse" {
  namespace_name = aws_redshiftserverless_namespace.warehouse.namespace_name
  workgroup_name = "${var.project_name}-${var.environment}"
  base_capacity  = 8   # RPUs — scale up for production
  tags           = local.common_tags
}

# ── MSK (Managed Kafka) ───────────────────────────────────────────────────────
resource "aws_msk_cluster" "kafka" {
  cluster_name           = "${var.project_name}-${var.environment}"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.msk.id]
    storage_info {
      ebs_storage_info { volume_size = 100 }
    }
  }

  encryption_info {
    encryption_in_transit { client_broker = "TLS_PLAINTEXT" }
  }

  tags = local.common_tags
}

# ── AWS Glue Data Catalog ─────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "bronze" {
  name = "${var.project_name}_bronze_${var.environment}"
}

resource "aws_glue_catalog_database" "silver" {
  name = "${var.project_name}_silver_${var.environment}"
}

resource "aws_glue_catalog_database" "gold" {
  name = "${var.project_name}_gold_${var.environment}"
}

# Glue Crawler — auto-discovers schema from S3
resource "aws_glue_crawler" "silver_orders" {
  name          = "${var.project_name}-silver-orders-${var.environment}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.silver.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.id}/silver/orders/"
  }

  schedule      = "cron(0 3 * * ? *)"
  tags          = local.common_tags
}

# ── IAM Roles ─────────────────────────────────────────────────────────────────
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-${var.environment}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "s3-data-lake-access"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.data_lake.arn,
        "${aws_s3_bucket.data_lake.arn}/*"
      ]
    }]
  })
}

# ── Security Groups ───────────────────────────────────────────────────────────
resource "aws_security_group" "msk" {
  name   = "${var.project_name}-msk-${var.environment}"
  vpc_id = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }
  tags = local.common_tags
}

# ── Locals ────────────────────────────────────────────────────────────────────
locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}
