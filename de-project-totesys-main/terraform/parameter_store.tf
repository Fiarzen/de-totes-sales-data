resource "aws_ssm_parameter" "ingestion_bucket_name" {
  name  = "ingestion_bucket_name"
  type  = "String"
  value = aws_s3_bucket.ingestion_bucket.bucket
}

resource "aws_ssm_parameter" "lambda_last_run" {
  name  = "lambda_last_run"
  type  = "String"
  value = "None"
}

resource "aws_ssm_parameter" "processed_bucket_name" {
  name  = "processed_bucket_name"
  type  = "String"
  value = aws_s3_bucket.processing_bucket.bucket
}

resource "aws_ssm_parameter" "load_last_run" {
  name  = "load_last_run"
  type  = "String"
  value = "None"
}