variable "aws_region" {
  default = "us-east-1"
}

variable "lambda_name" {
  default = "csv-lambda-processor"
}

variable "input_bucket_name" {}
variable "output_bucket_name" {}

variable "rds_host" {}
variable "rds_user" {}
variable "rds_pass" {
  sensitive = true
}
variable "rds_db" {}
