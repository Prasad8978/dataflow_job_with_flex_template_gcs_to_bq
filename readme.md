In this document, you use the following billable components of Google Cloud:

Dataflow -- run access
Cloud Storage -- template store
Artifact Registry -- docker images repository 
Cloud Build -- jobs build 


1. Enable the Dataflow, Compute Engine, Logging, Cloud Storage, Cloud Storage JSON, Resource Manager, Artifact Registry, and Cloud Build API:

gcloud services enable dataflow compute_component logging storage_component storage_api cloudresourcemanager.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com


2. Granting roles to your user account:

gcloud projects add-iam-policy-binding rare-tome-458105-n0 --member="user:ellapunagaprasad4@gmail.com" --role=roles/iam.serviceAccountUser

3. Grant roles to your compute engine service account:

gcloud projects add-iam-policy-binding rare-tome-458105-n0 --member="serviceAccount:693932477273-compute@developer.gserviceaccount.com" --role=roles/dataflow.admin


gcloud projects add-iam-policy-binding rare-tome-458105-n0 --member="serviceAccount:693932477273-compute@developer.gserviceaccount.com" --role=roles/dataflow.worker

gcloud projects add-iam-policy-binding rare-tome-458105-n0 --member="serviceAccount:693932477273-compute@developer.gserviceaccount.com" --role=roles/storage.objectAdmin

gcloud projects add-iam-policy-binding rare-tome-458105-n0 --member="serviceAccount:693932477273-compute@developer.gserviceaccount.com" --role=roles/artifactregistry.writer


4. local shell preparation:

folder setup done by us locally in VS code

5. bucket creation:

gsutil mb -l us-central1 gs://my-flex-template-bucket2/

6. repo creation:

gcloud artifacts repositories create dataflowflextemplaterepo `
 --repository-format=docker `
 --location=us-central1

Note: To list out the repositories in the artifact registry:

gcloud artifacts repositories list


7. Docker authentication requests for Artifact registry:

gcloud auth configure-docker us-central1-docker.pkg.dev

8. build:

gcloud dataflow flex-template build gs://my-flex-template-bucket2/beam_python_script.json `
 --image-gcr-path "us-central1-docker.pkg.dev/rare-tome-458105-n0/dataflowflextemplaterepo/beam-gcstobq-script:latest" `
 --sdk-language PYTHON `
 --flex-template-base-image PYTHON3 `
 --metadata-file "metadata.json" `
 --py-path "." `
 --env "FLEX_TEMPLATE_PYTHON_PY_FILE=beam_python_script.py" `
 --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt"

9.  CLI command for dataflow job creation

gcloud dataflow flex-template run job-gcs-to-gcs4 `
  --project=rare-tome-458105-n0 `
  --region=us-central1 `
  --template-file-gcs-location=gs://my-flex-template-bucket1/beam_gcstobq_script.json `
  --parameters output=gs://my-flex-template-bucket1/output 






