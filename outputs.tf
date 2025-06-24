output "lambda_name" {
  value = aws_lambda_function.csv_processor.function_name
}

output "lambda_arn" {
  value = aws_lambda_function.csv_processor.arn
}
