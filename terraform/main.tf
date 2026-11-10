# Example root. `terraform apply` costs money (NAT gateway, RDS, ElastiCache, ALB);
# run `terraform destroy` when done.
terraform {
  required_version = ">= 1.6"
}

provider "aws" {
  region = var.region
}

variable "region" {
  type    = string
  default = "us-east-1"
}
variable "image" { type = string }
variable "kafka_brokers" {
  type    = string
  default = ""
}

module "webhook" {
  source        = "./modules/webhook"
  region        = var.region
  image         = var.image
  kafka_brokers = var.kafka_brokers
}

output "url" { value = module.webhook.url }
