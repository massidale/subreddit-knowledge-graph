terraform {
  required_version = ">= 1.9.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = var.project_name
      Env     = var.env
      Owner   = "team"
    }
  }
}

resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
}

resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
}

resource "aws_s3_bucket" "gold" {
  bucket = var.gold_bucket_name
}

resource "aws_s3_bucket_versioning" "bronze_versioning" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze_lifecycle" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "expire-noncurrent"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_iam_user" "pipeline" {
  name = "${var.project_name}-${var.env}-pipeline"
}

resource "aws_iam_access_key" "pipeline" {
  user = aws_iam_user.pipeline.name
}

data "aws_iam_policy_document" "pipeline_s3" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      "${aws_s3_bucket.bronze.arn}/*",
      aws_s3_bucket.silver.arn,
      "${aws_s3_bucket.silver.arn}/*",
      aws_s3_bucket.gold.arn,
      "${aws_s3_bucket.gold.arn}/*",
    ]
  }
}

resource "aws_iam_user_policy" "pipeline_s3" {
  user   = aws_iam_user.pipeline.name
  policy = data.aws_iam_policy_document.pipeline_s3.json
}
