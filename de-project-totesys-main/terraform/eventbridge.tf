resource "aws_cloudwatch_event_rule" "run_extract_lambda" {
  name        = "run-extract-lambda"
  description = "runs extract function every 30 mins"
    schedule_expression = "rate(30 minutes)"
    
}

resource "aws_cloudwatch_event_target" "extract_lambda" {
  rule      = aws_cloudwatch_event_rule.run_extract_lambda.name
  target_id = "run-scheduled-task-every-30-mins"
  arn       = aws_lambda_function.workflow_tasks_extract.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
    statement_id = "AllowExecutionFromEventBridge"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.workflow_tasks_extract.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.run_extract_lambda.arn
}

resource "aws_cloudwatch_event_rule" "run_load_lambda" {
  name        = "run-load-lambda"
  description = "runs load function every 30 mins"
    schedule_expression = "rate(30 minutes)"
    
}

resource "aws_cloudwatch_event_target" "load_lambda" {
  rule      = aws_cloudwatch_event_rule.run_load_lambda.name
  target_id = "run-scheduled-task-every-30-mins"
  arn       = aws_lambda_function.workflow_tasks_loads.arn
}

resource "aws_lambda_permission" "allow_eventbridge_load" {
    statement_id = "AllowExecutionFromEventBridge"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.workflow_tasks_loads.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.run_load_lambda.arn
}
