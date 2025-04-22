data "archive_file" "layer_code" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/layer.zip"
  source_dir  = "${path.module}/../dependencies"
}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name = "dependencies_library_layer"
  s3_bucket  = aws_s3_bucket.code_bucket.bucket
  s3_key     = aws_s3_object.lambda_layer.key
  }

data "archive_file" "pyarrow_files" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/pyarrow_layer.zip"
  source_dir  = "${path.module}/../pyarrow"
}

resource "aws_lambda_layer_version" "pyarrow" {
  layer_name = "pyarrow_layer"
  s3_bucket  = aws_s3_bucket.code_bucket.bucket
  s3_key     = aws_s3_object.pyarrow_layer.key
  }