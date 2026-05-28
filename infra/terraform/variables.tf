variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "reddit-kg"
}

variable "env" {
  type    = string
  default = "dev"
}

variable "bronze_bucket_name" {
  type        = string
  description = "Globally-unique S3 bucket name for bronze (e.g. reddit-kg-bronze-dev-abc)"
}

variable "silver_bucket_name" {
  type = string
}

variable "gold_bucket_name" {
  type = string
}
