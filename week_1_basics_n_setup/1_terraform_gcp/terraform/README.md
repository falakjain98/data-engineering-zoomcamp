### Concepts
* [Terraform_overview](../1_terraform_overview.md)
* [Audio](https://drive.google.com/file/d/1IqMRDwJV-m0v9_le_i2HA_UbM_sIWgWx/view?usp=sharing)

### Execution

```shell
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Can also use:
# export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/ny-rides.json 
# gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

# Initialize state file (.tfstate)
terraform init

# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
```

```shell
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```

```shell
# Delete infra after your work, to avoid costs on any running services
terraform destroy
```
