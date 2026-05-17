terraform {
  required_providers {
    docker = { source = "kreuzwerker/docker", version = "~> 3.0" }
  }
}

provider "docker" {}

resource "docker_container" "minio" {
  image   = "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
  name    = "minio"
  command = ["server", "/data", "--console-address", ":9001"]

  ports {
    internal = 9000
    external = 9000
  }

  ports {
    internal = 9001
    external = 9001
  }

  env = [
    "MINIO_ROOT_USER=${var.minio_root_user}",
    "MINIO_ROOT_PASSWORD=${var.minio_root_password}",
  ]
}

variable "minio_root_user" {
  description = "MinIO root username"
  type        = string
  sensitive   = true
}

variable "minio_root_password" {
  description = "MinIO root password"
  type        = string
  sensitive   = true
}
