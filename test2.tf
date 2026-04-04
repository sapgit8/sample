Step 2 — Add ALB + Route53 + ACM (full replacement code)
hcl####################################################################################
# ALB SECURITY GROUP
####################################################################################

resource "aws_security_group" "alb_sg" {
  name        = "${var.infra_tags["paloName"]}-alb-sg"
  description = "Security group for Application Load Balancer"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTP from internet"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS from internet"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.required_tags
}

####################################################################################
# APPLICATION LOAD BALANCER
####################################################################################

resource "aws_lb" "palo_alto_alb" {
  name               = "${var.infra_tags["paloName"]}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb_sg.id]
  subnets            = [for subnet in aws_subnet.public_subnets : subnet.id]

  enable_deletion_protection = true
  enable_http2               = true

  tags = var.required_tags
}

####################################################################################
# ALB TARGET GROUP
####################################################################################

resource "aws_lb_target_group" "palo_alto_alb_tg" {
  name        = "${var.infra_tags["paloName"]}-alb-tg"
  port        = 443
  protocol    = "HTTPS"
  vpc_id      = aws_vpc.main.id
  target_type = "instance"

  health_check {
    enabled             = true
    path                = "/php/login.php"   # PaloAlto management UI path
    protocol            = "HTTPS"
    port                = "traffic-port"
    healthy_threshold   = 3
    unhealthy_threshold = 3
    timeout             = 10
    interval            = 30
    matcher             = "200"
  }

  stickiness {
    type            = "lb_cookie"
    cookie_duration = 86400
    enabled         = true
  }

  tags = var.required_tags
}

####################################################################################
# ALB LISTENERS
####################################################################################

resource "aws_lb_listener" "http_redirect" {
  load_balancer_arn = aws_lb.palo_alto_alb.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type = "redirect"
    redirect {
      port        = "443"
      protocol    = "HTTPS"
      status_code = "HTTP_301"
    }
  }
}

resource "aws_lb_listener" "https" {
  load_balancer_arn = aws_lb.palo_alto_alb.arn
  port              = 443
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS13-1-2-2021-06"
  certificate_arn   = aws_acm_certificate_validation.palo_alto.certificate_arn

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.palo_alto_alb_tg.arn
  }
}

####################################################################################
# ASG ATTACHMENT (replaces asg_attachment_hot)
####################################################################################

resource "aws_autoscaling_attachment" "asg_alb_attachment" {
  autoscaling_group_name = aws_autoscaling_group.palo_alto_asg.id
  lb_target_group_arn    = aws_lb_target_group.palo_alto_alb_tg.arn
}

####################################################################################
# ACM CERTIFICATE
####################################################################################

resource "aws_acm_certificate" "palo_alto" {
  domain_name       = var.subdomain
  validation_method = "DNS"

  tags = var.required_tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert_validation" {
  for_each = {
    for dvo in aws_acm_certificate.palo_alto.domain_validation_options : dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  }

  zone_id = data.aws_route53_zone.main.zone_id
  name    = each.value.name
  type    = each.value.type
  records = [each.value.record]
  ttl     = 60
}

resource "aws_acm_certificate_validation" "palo_alto" {
  certificate_arn         = aws_acm_certificate.palo_alto.arn
  validation_record_fqdns = [for record in aws_route53_record.cert_validation : record.fqdn]
}

####################################################################################
# ROUTE53
####################################################################################

data "aws_route53_zone" "main" {
  name         = var.domain_name
  private_zone = false
}

resource "aws_route53_record" "palo_alto_alb" {
  zone_id = data.aws_route53_zone.main.zone_id
  name    = var.subdomain
  type    = "A"

  alias {
    name                   = aws_lb.palo_alto_alb.dns_name
    zone_id                = aws_lb.palo_alto_alb.zone_id
    evaluate_target_health = true
  }
}

Step 3 — Update your ASG to remove the commented-out target group
In your existing aws_autoscaling_group, you already have this commented out — just make sure it stays empty:
hclresource "aws_autoscaling_group" "palo_alto_asg" {
  # ...
  target_group_arns = []   # keep empty — attachment handled by aws_autoscaling_attachment above

  lifecycle {
    ignore_changes = [load_balancers]   # keep this
  }
}

Step 4 — New variables needed
hclvariable "domain_name" {
  description = "Root hosted zone in Route53"
  type        = string
  # e.g. "example.com"
}

variable "subdomain" {
  description = "Full subdomain for the ALB"
  type        = string
  # e.g. "firewall.example.com"
}

Migration order when you apply
Run in this order to avoid dependency issues:
bash# 1. Destroy GWLB attachment first (so ASG isn't blocked)
terraform destroy -target=aws_autoscaling_attachment.asg_attachment_hot

# 2. Destroy GWLB resources
terraform destroy -target=aws_lb_target_group.palo_gwlb_tg
terraform destroy -target=aws_lb.palo_gwlb

# 3. Apply everything new
terraform apply
