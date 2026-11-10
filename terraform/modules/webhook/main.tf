data "aws_availability_zones" "available" { state = "available" }

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name               = var.name
  cidr               = "10.40.0.0/16"
  azs                = local.azs
  public_subnets     = ["10.40.0.0/24", "10.40.1.0/24"]
  private_subnets    = ["10.40.10.0/24", "10.40.11.0/24"]
  enable_nat_gateway = true
  single_nat_gateway = true
}

# ---------- security groups ----------
resource "aws_security_group" "alb" {
  name   = "${var.name}-alb"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "app" {
  name   = "${var.name}-app"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "data" {
  name   = "${var.name}-data"
  vpc_id = module.vpc.vpc_id
  dynamic "ingress" {
    for_each = [5432, 6379, 9098]
    content {
      from_port       = ingress.value
      to_port         = ingress.value
      protocol        = "tcp"
      security_groups = [aws_security_group.app.id]
    }
  }
}

# ---------- Postgres ----------
resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "this" {
  name       = var.name
  subnet_ids = module.vpc.private_subnets
}

resource "aws_db_instance" "this" {
  identifier             = var.name
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = var.db_instance_class
  allocated_storage      = 20
  db_name                = "webhook"
  username               = "webhook"
  password               = random_password.db.result
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.data.id]
  skip_final_snapshot    = true
  deletion_protection    = false
}

# ---------- Redis ----------
resource "aws_elasticache_subnet_group" "this" {
  name       = var.name
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_cluster" "this" {
  cluster_id         = var.name
  engine             = "redis"
  node_type          = var.cache_node_type
  num_cache_nodes    = 1
  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.data.id]
}

# ---------- Kafka (optional) ----------
resource "aws_msk_serverless_cluster" "this" {
  count        = var.enable_msk ? 1 : 0
  cluster_name = var.name
  vpc_config {
    subnet_ids         = module.vpc.private_subnets
    security_group_ids = [aws_security_group.data.id]
  }
  client_authentication {
    sasl {
      iam { enabled = true }
    }
  }
}

locals {
  kafka_brokers = var.enable_msk ? aws_msk_serverless_cluster.this[0].bootstrap_brokers_sasl_iam : var.kafka_brokers
}

# ---------- secrets ----------
resource "aws_secretsmanager_secret" "database_url" {
  name                    = "${var.name}/database-url"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "database_url" {
  secret_id     = aws_secretsmanager_secret.database_url.id
  secret_string = "postgres://webhook:${random_password.db.result}@${aws_db_instance.this.endpoint}/webhook?sslmode=require"
}

# ---------- ECS Fargate ----------
resource "aws_ecs_cluster" "this" {
  name = var.name
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/ecs/${var.name}"
  retention_in_days = 7
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${var.name}-execution"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "read_secret" {
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["secretsmanager:GetSecretValue"]
      Resource = [aws_secretsmanager_secret.database_url.arn]
    }]
  })
}

resource "aws_ecs_task_definition" "this" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 512
  memory                   = 1024
  execution_role_arn       = aws_iam_role.execution.arn
  container_definitions = jsonencode([{
    name         = "app"
    image        = var.image
    essential    = true
    portMappings = [{ containerPort = 8080 }, { containerPort = 9090 }]
    stopTimeout  = 30
    environment = [
      { name = "REDIS_URL", value = "redis://${aws_elasticache_cluster.this.cache_nodes[0].address}:6379/0" },
      { name = "KAFKA_BROKERS", value = local.kafka_brokers },
      { name = "SHUTDOWN_TIMEOUT", value = "25s" },
    ]
    secrets = [{ name = "DATABASE_URL", valueFrom = aws_secretsmanager_secret.database_url.arn }]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.this.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "app"
      }
    }
  }])
}

resource "aws_lb" "this" {
  name               = var.name
  load_balancer_type = "application"
  subnets            = module.vpc.public_subnets
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "this" {
  name        = var.name
  port        = 8080
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = module.vpc.vpc_id
  health_check { path = "/readyz" }
  deregistration_delay = 30
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.this.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.this.arn
  }
}

resource "aws_ecs_service" "this" {
  name            = var.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.this.arn
  desired_count   = var.desired_count
  launch_type     = "FARGATE"
  network_configuration {
    subnets         = module.vpc.private_subnets
    security_groups = [aws_security_group.app.id]
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.this.arn
    container_name   = "app"
    container_port   = 8080
  }
  depends_on = [aws_lb_listener.http]
}
