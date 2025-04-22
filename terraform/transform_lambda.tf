data "archive_file" "transform_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/transform_lambda/function.zip"
  source_dir = "${path.module}/../src/transform_lambda"
}
resource "aws_lambda_function" "workflow_tasks_transform" {
  function_name    = var.transform_lambda
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.transform_lambda}/function.zip"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.default_timeout
  layers           = [aws_lambda_layer_version.dependencies.arn, aws_lambda_layer_version.pyarrow.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:13"]

  depends_on = [aws_s3_object.lambda_code, aws_s3_object.lambda_layer]
}

resource "aws_lambda_permission" "allow_ingestion_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.workflow_tasks_transform.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.ingestion_bucket.arn
}