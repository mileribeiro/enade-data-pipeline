variable "aws_region" {
  description = "AWS region where the data lake resources will be created."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name used to identify project resources."
  type        = string
  default     = "enade"
}


