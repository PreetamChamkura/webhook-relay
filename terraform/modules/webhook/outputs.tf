output "url" { value = "http://${aws_lb.this.dns_name}" }
output "db_endpoint" { value = aws_db_instance.this.endpoint }
output "redis_endpoint" { value = aws_elasticache_cluster.this.cache_nodes[0].address }
output "kafka_brokers" { value = local.kafka_brokers }
