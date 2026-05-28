output "bronze_bucket" {
  value = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  value = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  value = aws_s3_bucket.gold.bucket
}

output "pipeline_user_access_key" {
  value     = aws_iam_access_key.pipeline.id
  sensitive = true
}

output "pipeline_user_secret_key" {
  value     = aws_iam_access_key.pipeline.secret
  sensitive = true
}
