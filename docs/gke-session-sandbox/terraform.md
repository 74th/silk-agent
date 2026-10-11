# Dedicated GKE test environment

Terraform under `terraform/` owns only resources named for this test environment. It uses a local state file in that directory and does not import or read the state in `../common` or neighboring tests. Keep `terraform.tfstate`, plan files, and credentials out of Git.

## Prepare inputs

Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and set an explicit project ID and the operator's public CIDR. The defaults use `192.168.0.0/20`, `192.168.32.0/19`, `192.168.64.0/20`, and `172.20.0.0/28`; confirm them against every VPC subnet and GKE control-plane range in the project. When another copy of this environment will exist in the same project, change both name prefixes, all ranges, and the local state path together.

The defaults name the cluster `research-gke-agent-sandbox-with-sandboxd`, VPC and subnet with `-vpc` and `-subnet`, node pools from `research-gke-as-sbd` with `-system` and `-gvisor`, the node service account with `-node`, and the registry with `-images`. Terraform outputs the complete physical-name mapping.

## Plan and apply

Run these commands from `terraform/` after installing the Terraform version in `.terraform-version`:

```sh
terraform init
terraform fmt -check -recursive
terraform validate
terraform plan -var-file=terraform.tfvars -out=environment.tfplan
terraform show environment.tfplan
```

Review the project ID, region, every resource name, network ranges, node counts, and authorized control-plane CIDRs in the saved plan. Apply only the reviewed plan:

```sh
terraform apply environment.tfplan
terraform output
```

Configure `kubectl` with the explicit `get_credentials_command` output. The Terraform outputs contain identifiers and commands, not credentials or Secret values.

## Destroy

Review the target before deleting the dedicated environment:

```sh
terraform plan -destroy -var-file=terraform.tfvars -out=destroy.tfplan
terraform show destroy.tfplan
```

The plan should contain only this environment's VPC, subnet, router/NAT, GKE cluster and node pools, node service account and its member-level role grants, registry, and API service resources. API resources set `disable_on_destroy = false`, so destroying this state does not disable project APIs. Apply the reviewed destroy plan, then inspect Cloud resources and persistent disks / images for resources the Kubernetes controller or container service may retain outside Terraform ownership.

Do not delete the local state until the destroy result and remaining resources have been recorded. A second environment requires a separate state file and non-overlapping names and CIDRs.
