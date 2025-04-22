data "archive_file" "load_lambda" {
  type        = "zip"
  output_path = "${path.module}/../packages/load_lambda/function.zip"
  source_dir = "${path.module}/../src/load_lambda"
}
resource "aws_lambda_function" "workflow_tasks_loads" {
  function_name    = var.load_lambda
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  s3_bucket        = aws_s3_bucket.code_bucket.bucket
  s3_key           = "${var.load_lambda}/function.zip"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_handler.load_data"
  runtime          = "python3.12"
  timeout          = var.load_timeout
  layers           = [aws_lambda_layer_version.dependencies.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:13"]

  depends_on = [aws_s3_object.lambda_code, aws_s3_object.lambda_layer]
  environment {
    variables = {
      SECRETS_ARN = aws_secretsmanager_secret.warehouse_credentials.arn
      W_USER = local.warehouse_credentials["user"]
      W_PASSWORD = local.warehouse_credentials["password"]
      W_HOST = local.warehouse_credentials["host"]
      W_DATABASE = local.warehouse_credentials["database"]
      W_PORT = local.warehouse_credentials["port"]
    }
  }
}
