
resource "aws_secretsmanager_secret" "db_credentials" {
  name = "db_credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id     = aws_secretsmanager_secret.db_credentials.id
  secret_string = data.local_sensitive_file.db_credentials.content
}

data "local_sensitive_file" "db_credentials" {
  filename = "${path.module}/../db_credentials.json"
}

data "aws_secretsmanager_secret_version" "db_credentials"{
  secret_id = aws_secretsmanager_secret.db_credentials.id
  depends_on = [aws_secretsmanager_secret_version.db_credentials ]
}

locals {
  db_credentials= jsondecode(data.aws_secretsmanager_secret_version.db_credentials.secret_string)
}

resource "aws_secretsmanager_secret" "warehouse_credentials" {
  name = "warehouse_credentials"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "warehouse_credentials" {
  secret_id     = aws_secretsmanager_secret.warehouse_credentials.id
  secret_string = data.local_sensitive_file.warehouse_credentials.content
}

data "local_sensitive_file" "warehouse_credentials" {
  filename = "${path.module}/../warehouse_credentials.json"
}

data "aws_secretsmanager_secret_version" "warehouse_credentials"{
  secret_id = aws_secretsmanager_secret.warehouse_credentials.id
  depends_on = [aws_secretsmanager_secret_version.warehouse_credentials ]
}

locals {
  warehouse_credentials= jsondecode(data.aws_secretsmanager_secret_version.warehouse_credentials.secret_string)
}
