variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "ecommerce-pipeline"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod"
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "VPC ID for MSK and Redshift"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for MSK brokers"
  type        = list(string)
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access Kafka and Redshift"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "redshift_admin_user" {
  description = "Redshift admin username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_admin_password" {
  description = "Redshift admin password"
  type        = string
  sensitive   = true
}
