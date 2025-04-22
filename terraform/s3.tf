resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket"
  tags = {
    Name = "Code Bucket"
  }
  
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = var.ingestion_bucket_prefix
  tags = {
    Name = "Ingestion Bucket"
  }
  force_destroy = true
}

resource "aws_s3_bucket_notification" "ingestion_bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id
  eventbridge = true
  lambda_function {
    lambda_function_arn = aws_lambda_function.workflow_tasks_transform.arn
    events              = ["s3:ObjectCreated:*"]
  }
  depends_on = [aws_lambda_permission.allow_ingestion_bucket]
}

resource "aws_s3_bucket" "processing_bucket" {
  bucket_prefix = var.processing_bucket_prefix
  tags = {
    Name = "Processing Bucket"
  }
  force_destroy = true
}

resource "aws_s3_bucket_notification" "processing_bucket_notification" {
  bucket = aws_s3_bucket.processing_bucket.id
  eventbridge = true
}


resource "aws_s3_object" "lambda_code" {
  for_each = toset([var.extract_lambda, var.transform_lambda, var.load_lambda])
  bucket   = aws_s3_bucket.code_bucket.bucket
  key      = "${each.key}/function.zip"
  source   = "${path.module}/../packages/${each.key}/function.zip"
  etag     = filemd5("${path.module}/../packages/${each.key}/function.zip")
}

resource "aws_s3_object" "lambda_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer/ingestion_layer.zip"
  source = data.archive_file.layer_code.output_path
  etag   = filemd5(data.archive_file.layer_code.output_path)
  depends_on = [ data.archive_file.layer_code ]
}

resource "aws_s3_object" "pyarrow_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer/ingestion_layer.zip"
  source = data.archive_file.pyarrow_files.output_path
  etag   = filemd5(data.archive_file.pyarrow_files.output_path)
  depends_on = [ data.archive_file.pyarrow_files ]
}