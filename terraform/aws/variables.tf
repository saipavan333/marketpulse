variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "environment" {
  type        = string
  description = "dev | staging | prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "vpc_id" {
  type = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "At least two private subnets across AZs"
}

variable "warehouse_instance_class" {
  type    = string
  default = "db.r6g.large"
}
