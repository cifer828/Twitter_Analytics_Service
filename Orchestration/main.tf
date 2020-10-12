# Provides a resource to manage EC2 Fleets.
#
# Usage:
# Configure the credentials first with `aws configure`
# Create a file named `terraform.tfvars` and set the values of the variables defined in `variables.tf`
#
# terraform init      Initialize a Terraform working directory
# terraform validate  Validates the Terraform files
# terraform fmt       Rewrites config files to canonical format
# terraform plan      Generate and show an execution plan
# terraform apply     Builds or changes infrastructure
# terraform destroy   Destroy Terraform-managed infrastructure

provider "aws" {
  region = "us-east-1"
}

# Create a new GitLab Lightsail Instance
resource "aws_lightsail_instance" "large-1" {
  name              = "QC-1"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.large2_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "large-2" {
  name              = "QC-2"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.large2_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}


resource "aws_lightsail_instance" "medium-1" {
  name              = "QC-3"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.large_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "medium-2" {
  name              = "QC-4"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.large_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "medium-3" {
  name              = "HY-1"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.large_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "medium-4" {
  name              = "HY-2"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.medium_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "medium-5" {
  name              = "HY-3"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.medium_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "medium-6" {
  name              = "XC-1"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.medium_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}


resource "aws_lightsail_instance" "medium-7" {
  name              = "XC-2"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.medium_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}

resource "aws_lightsail_instance" "medium-8" {
  name              = "XC-3"
  availability_zone = "us-east-1b"
  blueprint_id      = "ubuntu_18_04"
  bundle_id         = var.medium_lightsail
  tags = {
    project = "phase3"
  }

  user_data = var.shell
}
