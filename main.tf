provider "aws" {
  region = var.aws_region
}

# Rol de ejecución de Lambda
resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda_exec_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Permiso básico para logs en CloudWatch
resource "aws_iam_policy_attachment" "lambda_basic" {
  name       = "lambda-basic-policy"
  roles      = [aws_iam_role.lambda_exec_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Recomendación 3: Política de acceso a S3 para Lambda
resource "aws_iam_policy" "lambda_s3_access" {
  name   = "lambda_s3_access"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        Resource = [
          "arn:aws:s3:::${var.input_bucket_name}",
          "arn:aws:s3:::${var.input_bucket_name}/*",
          "arn:aws:s3:::${var.output_bucket_name}",
          "arn:aws:s3:::${var.output_bucket_name}/*"
        ]
      }
    ]
  })
}

# Asociar esa política al rol de Lambda
resource "aws_iam_role_policy_attachment" "lambda_attach_s3" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_s3_access.arn
}

# Función Lambda
resource "aws_lambda_function" "csv_processor" {
  function_name = var.lambda_name
  handler       = "app.lambda_handler"
  runtime       = "python3.10"
  role          = aws_iam_role.lambda_exec_role.arn
  filename      = "lambda.zip"


  environment {
    variables = {
      OUTPUT_BUCKET = var.output_bucket_name
      RDS_HOST      = var.rds_host
      RDS_USER      = var.rds_user
      RDS_PASS      = var.rds_pass
      RDS_DB        = var.rds_db
    }
  }
}

# Trigger desde S3
resource "aws_s3_bucket_notification" "lambda_trigger" {
  bucket = var.input_bucket_name

  lambda_function {
    lambda_function_arn = aws_lambda_function.csv_processor.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3]
}

# Permiso para que S3 invoque la Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.csv_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::${var.input_bucket_name}"
}
