resource "aws_sns_topic" "state_machine_alerts" {
    name = "state_machine_alerts"
}

resource "aws_sns_topic" "lambda_alerts" {
    name = "lambda_alerts"
}

resource "aws_sns_topic_subscription" "email_subscription_state_machine" {
    topic_arn = aws_sns_topic.state_machine_alerts.arn
    protocol = "email"
    endpoint = "jeremylam1995@gmail.com"
}

resource "aws_sns_topic_subscription" "email_subscription_lambda" {
    topic_arn = aws_sns_topic.lambda_alerts.arn
    protocol = "email"
    endpoint = "jeremylam1995@gmail.com"
}

resource "aws_cloudwatch_log_group" "sfn_log_group" {
  name              = "/aws/states/totes_state_machine"
  retention_in_days = 7
}

resource "aws_cloudwatch_metric_alarm" "lambda_alarm" {
  alarm_name                = "lambda_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = 2
  metric_name               = "Errors"
  namespace                 = "AWS/Lambda"
  period                    = 120
  statistic                 = "Sum"
  threshold                 = 1
  alarm_description         = "This metric monitors lambda errors"
  insufficient_data_actions = []
  alarm_actions = [aws_sns_topic.lambda_alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "state_machine_failure_alarm" {
  alarm_name          = "state_machine_failures_alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ExecutionsFailed"
  namespace           = "AWS/States"
  period              = 60
  statistic           = "Sum"
  threshold           = 1  # Trigger when there is at least one failure
  alarm_description   = "Alarm when State Machine has failed executions"

  alarm_actions = [aws_sns_topic.state_machine_alerts.arn]
}