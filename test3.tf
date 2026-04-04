hcl####################################################################################
# NLB (REQUIRED WRAPPER FOR ENDPOINT SERVICE)
####################################################################################

# NOTE: VPC Endpoint Service does NOT support ALB directly.
# AWS requires a Network Load Balancer (NLB) in front of the ALB.
# Traffic flow: Client → NLB → ALB → PaloAlto VMs

resource "aws_lb" "palo_alto_nlb" {
  name               = "${var.infra_tags["paloName"]}-nlb"
  internal           = true
  load_balancer_type = "network"
  subnets            = [for subnet in aws_subnet.private_subnets : subnet.id]

  enable_deletion_protection       = true
  enable_cross_zone_load_balancing = true

  tags = var.required_tags
}

####################################################################################
# NLB TARGET GROUP → POINTS TO ALB
####################################################################################

resource "aws_lb_target_group" "nlb_to_alb_tg" {
  name        = "${var.infra_tags["paloName"]}-nlb-alb-tg"
  port        = 443
  protocol    = "TCP"
  vpc_id      = aws_vpc.main.id
  target_type = "alb"          # NLB targets the ALB directly

  health_check {
    enabled             = true
    protocol            = "HTTPS"
    port                = "traffic-port"
    path                = "/php/login.php"
    healthy_threshold   = 3
    unhealthy_threshold = 3
    interval            = 30
  }

  tags = var.required_tags
}

####################################################################################
# ATTACH ALB AS TARGET TO NLB TARGET GROUP
####################################################################################

resource "aws_lb_target_group_attachment" "nlb_to_alb" {
  target_group_arn = aws_lb_target_group.nlb_to_alb_tg.arn
  target_id        = aws_lb.palo_alto_alb.arn    # ALB ARN as target
  port             = 443
}

####################################################################################
# NLB LISTENER → FORWARDS TO ALB TARGET GROUP
####################################################################################

resource "aws_lb_listener" "nlb_https" {
  load_balancer_arn = aws_lb.palo_alto_nlb.arn
  port              = 443
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.nlb_to_alb_tg.arn
  }
}

resource "aws_lb_listener" "nlb_http" {
  load_balancer_arn = aws_lb.palo_alto_nlb.arn
  port              = 80
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.nlb_to_alb_tg.arn
  }
}

####################################################################################
# VPC ENDPOINT SERVICE → POINTS TO NLB
####################################################################################

resource "aws_vpc_endpoint_service" "palo_alto" {
  acceptance_required        = true    # manually approve endpoint connection requests
  network_load_balancer_arns = [aws_lb.palo_alto_nlb.arn]

  # optional: allow specific AWS accounts to connect
  allowed_principals = var.endpoint_allowed_principals

  tags = merge(var.required_tags, {
    Name = "${var.infra_tags["paloName"]}-endpoint-service"
  })
}

####################################################################################
# ENDPOINT SERVICE NOTIFICATIONS (OPTIONAL)
####################################################################################

resource "aws_vpc_endpoint_service_allowed_principal" "allowed" {
  for_each = toset(var.endpoint_allowed_principals)

  vpc_endpoint_service_id = aws_vpc_endpoint_service.palo_alto.id
  principal_arn           = each.value
}

resource "aws_sns_topic" "endpoint_notifications" {
  name = "${var.infra_tags["paloName"]}-endpoint-notifications"
  tags = var.required_tags
}

resource "aws_vpc_endpoint_connection_notification" "palo_alto" {
  vpc_endpoint_service_id     = aws_vpc_endpoint_service.palo_alto.id
  connection_notification_arn = aws_sns_topic.endpoint_notifications.arn
  connection_events           = ["Accept", "Reject", "Connect", "Delete"]
}

New variables needed
hclvariable "endpoint_allowed_principals" {
  description = "List of AWS account ARNs allowed to create endpoints to this service"
  type        = list(string)
  default     = []
  # e.g. ["arn:aws:iam::123456789012:root", "arn:aws:iam::987654321098:root"]
}

How consumers in other VPCs connect to your service
hcl# In the CONSUMER account / VPC:

resource "aws_vpc_endpoint" "connect_to_palo" {
  vpc_id              = var.consumer_vpc_id
  service_name        = data.aws_vpc_endpoint_service.palo_alto.service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.consumer_subnet_ids
  security_group_ids  = [aws_security_group.endpoint_sg.id]
  private_dns_enabled = true

  tags = var.required_tags
}

# Look up the service name from the provider account
data "aws_vpc_endpoint_service" "palo_alto" {
  service_name = "com.amazonaws.vpce.us-east-1.vpce-svc-xxxxxxxxxx"
}

Full traffic flow
Consumer VPC
    ↓
VPC Endpoint (Interface)
    ↓
VPC Endpoint Service
    ↓
NLB (internal) ← required by AWS, ALB not supported directly
    ↓
ALB (application) ← TLS termination, HTTP routing
    ↓
ALB Target Group
    ↓
PaloAlto VMs (ASG) ← ENIs attached via Lambda lifecycle hook
