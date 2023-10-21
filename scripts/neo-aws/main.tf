terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.20.0"
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "ap-east-1"
}

variable "num-replica" {
  type    = number
  default = 67
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

resource "aws_vpc" "neo" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "neo" {
  vpc_id                  = resource.aws_vpc.neo.id
  cidr_block              = "10.0.0.0/16"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "neo" {
  vpc_id = resource.aws_vpc.neo.id
}

resource "aws_route_table" "neo" {
  vpc_id = resource.aws_vpc.neo.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = resource.aws_internet_gateway.neo.id
  }
}

resource "aws_route_table_association" "_1" {
  route_table_id = resource.aws_route_table.neo.id
  subnet_id      = resource.aws_subnet.neo.id
}

# resource "aws_ec2_transit_gateway" "neo" {
#   multicast_support = "enable"
# }

# resource "aws_ec2_transit_gateway_vpc_attachment" "neo" {
#   subnet_ids         = [aws_subnet.neo.id]
#   transit_gateway_id = aws_ec2_transit_gateway.neo.id
#   vpc_id             = aws_vpc.neo.id
# }

# resource "aws_ec2_transit_gateway_multicast_domain" "neo" {
#   transit_gateway_id     = aws_ec2_transit_gateway.neo.id
#   static_sources_support = "enable"
# }

# resource "aws_ec2_transit_gateway_multicast_domain_association" "neo" {
#   subnet_id                           = aws_subnet.neo.id
#   transit_gateway_attachment_id       = aws_ec2_transit_gateway_vpc_attachment.neo.id
#   transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain.neo.id
# }

resource "aws_security_group" "neo" {
  vpc_id = resource.aws_vpc.neo.id

  ingress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_instance" "clients" {
  count = 100

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c5a.large"
  subnet_id              = resource.aws_subnet.neo.id
  vpc_security_group_ids = [resource.aws_security_group.neo.id]
  key_name               = "Ephemeral"
}

resource "aws_instance" "replicas" {
  count = var.num-replica

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c5a.xlarge"
  subnet_id              = resource.aws_subnet.neo.id
  vpc_security_group_ids = [resource.aws_security_group.neo.id]
  key_name               = "Ephemeral"
}

resource "aws_instance" "sequencer" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c5a.4xlarge"
  subnet_id              = resource.aws_subnet.neo.id
  vpc_security_group_ids = [resource.aws_security_group.neo.id]
  key_name               = "Ephemeral"
}

resource "aws_instance" "relays" {
  count = 6

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c5a.4xlarge"
  subnet_id              = resource.aws_subnet.neo.id
  vpc_security_group_ids = [resource.aws_security_group.neo.id]
  key_name               = "Ephemeral"
}

# resource "aws_ec2_transit_gateway_multicast_group_source" "source" {
#   group_ip_address                    = "224.0.0.1"
#   network_interface_id                = aws_instance.sequencer.primary_network_interface_id
#   transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain_association.neo.transit_gateway_multicast_domain_id
# }

# resource "aws_ec2_transit_gateway_multicast_group_member" "members" {
#   count = var.num-replica

#   group_ip_address                    = "224.0.0.1"
#   network_interface_id                = aws_instance.replicas[count.index].primary_network_interface_id
#   transit_gateway_multicast_domain_id = aws_ec2_transit_gateway_multicast_domain_association.neo.transit_gateway_multicast_domain_id
# }

output "client-hosts" {
  value = aws_instance.clients[*].public_dns
}

output "client-ips" {
  value = aws_instance.clients[*].private_ip
}

output "replica-hosts" {
  value = aws_instance.replicas[*].public_dns
}

output "replica-ips" {
  value = aws_instance.replicas[*].private_ip
}

output "sequencer-host" {
  value = aws_instance.sequencer.public_dns
}

output "sequencer-ip" {
  value = aws_instance.sequencer.private_ip
}

output "relay-hosts" {
  value = aws_instance.relays[*].public_dns
}

output "relay-ips" {
  value = aws_instance.relays[*].private_ip
}
