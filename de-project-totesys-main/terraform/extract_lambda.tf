data "archive_file" "extract_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/extract_lambda/function.zip"
  source_dir = "${path.module}/../src/extract_lambda"
}
resource "aws_lambda_function" "workflow_tasks_extract" {
  function_name    = var.extract_lambda
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.extract_lambda}/function.zip"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_handler.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.default_timeout
  layers           = [aws_lambda_layer_version.dependencies.arn]
  environment {
    variables = {
      SECRETS_ARN = aws_secretsmanager_secret.db_credentials.arn
      USER = local.db_credentials["user"]
      PASSWORD = local.db_credentials["password"]
      HOST = local.db_credentials["host"]
      DATABASE = local.db_credentials["database"]
      PORT = local.db_credentials["port"]
    }
  }
  depends_on = [aws_s3_object.lambda_code, aws_s3_object.lambda_layer]
}