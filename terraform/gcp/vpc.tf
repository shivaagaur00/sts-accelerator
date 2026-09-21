resource "google_compute_network" "migration_vpc" {
  name                    = "migration-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "migration_subnet" {
  name          = "migration-subnet"
  ip_cidr_range = "10.10.0.0/24"
  region        = var.region
  network       = google_compute_network.migration_vpc.id
}

# Cross-Cloud Interconnect VLAN attachment and the internal load balancer
# for the private-network transfer path go here once that's needed —
# see docs/Migration_Accelerator_Complete_Guide.md Phase 4 for the
# manual console steps; codifying them in Terraform is a later step
# once the interconnect itself is provisioned (that part isn't
# Terraform-manageable until the physical connection exists).
