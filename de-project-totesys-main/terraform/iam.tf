#lambda
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "role-totes-lambda"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}


resource "aws_iam_policy" "lambda_secret_access" {
  name = "lambda-secret-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = ["secretsmanager:GetSecretValue"],
      Effect   = "Allow",
      Resource = aws_secretsmanager_secret.db_credentials.arn
    }]
  })
}

resource "aws_iam_policy" "lambda_get_parameter" {
  name = "lambda-parameter-access"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action   = ["ssm:GetParameter", "ssm:PutParameter"],
      Effect   = "Allow",
      Resource = ["arn:aws:ssm:eu-west-2:216989110647:parameter/*"]
    }]
  })
}

resource "aws_iam_policy" "lambda_logging" {
  name = "lambda-logging"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        } 
    ]
} 
EOF
}


resource "aws_iam_role_policy_attachment" "attach_secret_access" { 
  role = aws_iam_role.lambda_role.name 
  policy_arn = aws_iam_policy.lambda_secret_access.arn 
  }

resource "aws_iam_role_policy_attachment" "attach_logging_access" { 
  role = aws_iam_role.lambda_role.name 
  policy_arn = aws_iam_policy.lambda_logging.arn 
  }

resource "aws_iam_role_policy_attachment" "attach_parameter_access" { 
  role = aws_iam_role.lambda_role.name 
  policy_arn = aws_iam_policy.lambda_get_parameter.arn 
  }


data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
    effect = "Allow"
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*","${aws_s3_bucket.processing_bucket.arn}/*","${aws_s3_bucket.ingestion_bucket.arn}",
      "${aws_s3_bucket.processing_bucket.arn}"
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-totes"
  policy      = data.aws_iam_policy_document.s3_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_policy.arn
}


resource "aws_iam_policy" "s3_permissions" {
  name   = "s3-policy"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "s3-object-lambda:*"
            ],
            "Resource": "*"
        }
    ]
}
    EOF
}

#state_machine
resource "aws_iam_role" "sfn_execution_role" {
  name = "sfn_execution_role"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Action": "sts:AssumeRole", 
        
        "Effect": "Allow",
        "Principal": {
          "Service": ["states.amazonaws.com", "scheduler.amazonaws.com"]
        }
      }
    ]
  })
}

resource "aws_iam_policy" "sfn_lambda_invoke_policy" {
  name        = "sfn_lambda_invoke_policy"
  description = "Allows Step Functions to invoke Lambda functions"

  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": "lambda:InvokeFunction",
        "Resource": [
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:*"
        ]
      }
    ]
  })
}
resource "aws_iam_role_policy_attachment" "attach_sfn_policy" {
  role       = aws_iam_role.sfn_execution_role.name
  policy_arn = aws_iam_policy.sfn_lambda_invoke_policy.arn
}


resource "aws_iam_policy" "sfn_logging_policy" {
  name        = "sfn_logging_policy"
  description = "IAM policy for Step Functions logging"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:CreateLogGroup"
        ],
        Resource = "${aws_cloudwatch_log_group.sfn_log_group.arn}:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sfn_logging_policy_attachment" {
  role       = aws_iam_role.sfn_execution_role.name
  policy_arn = aws_iam_policy.sfn_logging_policy.arn
}

#eventbridge
resource "aws_iam_role" "eventbridge_step_function_role" {
  name = "EventBridgeStepFunctionExecutionRole"

  assume_role_policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {
          "Service": "events.amazonaws.com"
        },
        "Action": "sts:AssumeRole"
      }
    ]
  })
}

# resource "aws_iam_role" "load_lambda" {
#   assume_role_policy = jsondecode({
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Effect": "Allow",
#       "Action": [
#         "secretsmanager:GetSecretValue",
#         "logs:CreateLogGroup",
#         "logs:CreateLogStream",
#         "logs:PutLogEvents",
#         "cloudwatch:PutMetricData"
#       ],
#       "Resource": "*"
#     }
#   ]
#   })
# }
