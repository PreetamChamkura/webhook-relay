variable "name" {
  type    = string
  default = "webhook-relay"
}
variable "region" { type = string }
variable "image" {
  description = "Container image for the service (e.g. ghcr.io/you/webhook-relay:sha)."
  type        = string
}
variable "desired_count" {
  type    = number
  default = 2
}
variable "db_instance_class" {
  type    = string
  default = "db.t4g.micro"
}
variable "cache_node_type" {
  type    = string
  default = "cache.t4g.micro"
}
variable "enable_msk" {
  description = "Provision MSK Serverless. MSK has a fixed hourly cost; leave false and point kafka_brokers at an external cluster to save money."
  type        = bool
  default     = false
}
variable "kafka_brokers" {
  description = "Used when enable_msk = false."
  type        = string
  default     = ""
}
