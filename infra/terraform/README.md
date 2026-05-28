# Terraform AWS Infrastructure

Minimum infra for the bronze/silver/gold S3 buckets + a dedicated IAM
user with R/W permissions on those buckets only.

## Apply

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with globally-unique bucket names

terraform init
terraform plan
terraform apply
```

Take the outputs and add them to your `.env`:

```bash
terraform output -json | jq -r '.pipeline_user_access_key.value, .pipeline_user_secret_key.value'
```

## Destroy

```bash
terraform destroy
```

NOTE: this only removes infra. Bucket contents are deleted if buckets are non-empty (see `force_destroy` in the bucket resource if needed for dev).
