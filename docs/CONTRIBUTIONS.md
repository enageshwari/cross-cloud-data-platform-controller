# Contributions Guide
## Cross-Cloud Data Platform Controller

---

## 1. Setup — Local Development

### Prerequisites

```bash
go 1.25+           # go version
kubectl            # brew install kubernetes-cli
eksctl             # brew install eksctl
helm               # brew install helm
gcloud CLI         # brew install --cask google-cloud-sdk
aws CLI            # brew install awscli
```

### Clone and build

```bash
git clone https://github.com/enageshwari/cross-cloud-data-platform-controller.git
cd cross-cloud-data-platform-controller/api
go mod download    # populate module cache — subsequent builds use no network
go build -o /tmp/control-api ./cmd/server
echo "build ok"
```

### Configure credentials (local only)

Create `api/.env` — this file is `.gitignore`-covered and never committed:

```env
# AWS S3 presigner
AWS_PRESIGNER_ACCESS_KEY_ID=<key id from aws-presigner-creds>
AWS_PRESIGNER_SECRET_ACCESS_KEY=<secret>
AWS_PRESIGNER_REGION=us-west-1
AWS_OUTPUT_BUCKET=cross-cloud-data-platform-controller-us-west-1
PRESIGN_TTL_MINUTES=15

# GCS signer
GCS_PRESIGNER_SA=gcs-presigner@project-965bb0cf-caa0-458d-ba9.iam.gserviceaccount.com
GCS_OUTPUT_BUCKET=cross-cloud-data-platform-controller-us-west2
GCS_PRESIGN_TTL_MINUTES=15

# EKS Spark dispatch
EKS_CONTEXT=arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane
SPARK_IMAGE=docker.io/apache/spark:3.5.3
```

Set up GCP ADC (one-time, browser login):
```bash
gcloud auth application-default login
gcloud config set project project-965bb0cf-caa0-458d-ba9
```

### Run the API locally

```bash
# Kill anything on port 8080 first
lsof -ti :8080 | xargs kill -9 2>/dev/null; sleep 1

# Build and start
cd api && go build -o /tmp/control-api ./cmd/server
/tmp/control-api &
SERVER_PID=$!

# Verify both presigners loaded
curl -s http://localhost:8080/healthz  # → {"status":"ok"}

# Clean shutdown
kill $SERVER_PID
```

---

## 2. Setup — Cloud Clusters

### GKE control plane

```bash
# Auth
gcloud auth login
gcloud config set project project-965bb0cf-caa0-458d-ba9
gcloud config set compute/region us-west2

# Create cluster (2 nodes, single zone, 30GB SSD — stays within quota)
gcloud container clusters create cross-cloud-control-plane \
  --project=project-965bb0cf-caa0-458d-ba9 \
  --zone=us-west2-a \
  --num-nodes=2 \
  --machine-type=e2-standard-2 \
  --disk-size=30 \
  --disk-type=pd-ssd \
  --workload-pool=project-965bb0cf-caa0-458d-ba9.svc.id.goog \
  --quiet

gcloud container clusters get-credentials cross-cloud-control-plane \
  --zone=us-west2-a --project=project-965bb0cf-caa0-458d-ba9

# Apply manifests (in order)
kubectl apply -f deploy/gke-control-plane/namespace.yaml
kubectl apply -f policy/templates/data-residency-template.yaml
kubectl apply -f policy/constraints/data-residency-constraint.yaml
kubectl apply -f scheduler/kueue/resource-flavors.yaml
kubectl apply -f scheduler/kueue/cluster-queue.yaml
kubectl apply -f scheduler/kueue/local-queues.yaml
kubectl apply -f deploy/gke-control-plane/control-api-deployment.yaml
kubectl apply -f deploy/gke-control-plane/spark-crd-creator-rbac.yaml
```

### EKS data plane

```bash
# Create cluster
eksctl create cluster -f deploy/aws-eks/cluster-config.yaml

# Scale up nodes for work, scale to zero when idle
aws eks update-nodegroup-config \
  --cluster-name cross-cloud-data-plane \
  --nodegroup-name spot-data-workers \
  --region us-west-1 \
  --scaling-config minSize=0,maxSize=4,desiredSize=2

# Get kubeconfig
aws eks update-kubeconfig --region us-west-1 --name cross-cloud-data-plane

# Install operators (using correct namespace config)
kubectl apply -f https://raw.githubusercontent.com/open-policy-agent/gatekeeper/v3.16.0/deploy/gatekeeper.yaml
kubectl apply --server-side -f https://github.com/kubernetes-sigs/kueue/releases/download/v0.9.1/manifests.yaml

# Fix Kueue kube-rbac-proxy image (gcr.io image was removed)
kubectl set image deployment/kueue-controller-manager \
  kube-rbac-proxy=quay.io/brancz/kube-rbac-proxy:v0.18.0 -n kueue-system

# Install Spark Operator — specify namespace explicitly
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set webhook.enable=true \
  --set batchScheduler.enable=false \
  --set "jobNamespaces[0]=data-workloads" \
  --wait

# Apply queue and policy configs
kubectl apply -f scheduler/kueue/resource-flavors.yaml
kubectl apply -f scheduler/kueue/cluster-queue.yaml
kubectl apply -f scheduler/kueue/local-queues.yaml
kubectl apply -f policy/templates/data-residency-template.yaml
kubectl apply -f policy/constraints/data-residency-constraint.yaml

# Create IRSA for Spark executor pods
eksctl create iamserviceaccount \
  --cluster=cross-cloud-data-plane \
  --region=us-west-1 \
  --namespace=data-workloads \
  --name=spark-job-sa \
  --attach-policy-arn=arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess \
  --attach-policy-arn=arn:aws:iam::080147880517:policy/spark-s3-output-write \
  --approve --override-existing-serviceaccounts

kubectl create serviceaccount spark-driver -n data-workloads
kubectl create clusterrolebinding spark-driver-binding \
  --clusterrole=edit --serviceaccount=data-workloads:spark-driver

# RBAC for CRD dispatch from control plane (SparkApplication, AppWrapper, FlinkDeployment)
kubectl apply -f deploy/gke-control-plane/spark-crd-creator-rbac.yaml
kubectl create clusterrolebinding spark-driver-crd-creator \
  --clusterrole=spark-crd-creator --serviceaccount=data-workloads:spark-driver
```

### Deploy API to GKE

```bash
# Build and push image via Cloud Build (no local Docker needed)
IMAGE="us-west2-docker.pkg.dev/project-965bb0cf-caa0-458d-ba9/cross-cloud-api/control-api:latest"
gcloud builds submit api/ --tag=$IMAGE --project=project-965bb0cf-caa0-458d-ba9

# Create AWS creds secret in GKE
kubectl apply -f deploy/gke-control-plane/namespace.yaml
kubectl create secret generic aws-presigner-creds \
  --namespace=data-workloads \
  --from-literal=access-key-id=<KEY_ID> \
  --from-literal=secret-access-key=<SECRET>

# Create cross-cluster kubeconfig secret (static SA tokens — see §4.3 for rotation)
# Generate /tmp/static-kubeconfig.yaml first using python3 /tmp/gen_kubeconfig.py
kubectl create secret generic cross-cloud-kubeconfig \
  --from-file=kubeconfig=/tmp/static-kubeconfig.yaml \
  -n data-workloads

# Apply deployment
kubectl apply -f deploy/gke-control-plane/control-api-deployment.yaml

# Verify
kubectl get pods -n data-workloads
kubectl port-forward svc/control-plane-api-svc 9090:80 -n data-workloads &
curl http://localhost:9090/healthz
```

---

## 3. Cost Management

```bash
# EKS: scale nodes to zero when not needed (saves ~$0.06-0.08/hr)
aws eks update-nodegroup-config \
  --cluster-name cross-cloud-data-plane \
  --nodegroup-name spot-data-workers \
  --region us-west-1 \
  --scaling-config minSize=0,maxSize=4,desiredSize=0

# EKS: restore for demos
aws eks update-nodegroup-config \
  --cluster-name cross-cloud-data-plane \
  --nodegroup-name spot-data-workers \
  --region us-west-1 \
  --scaling-config minSize=0,maxSize=4,desiredSize=2

# GKE: scale control plane to zero
gcloud container clusters resize cross-cloud-control-plane \
  --zone=us-west2-a --node-pool=default-pool --num-nodes=0 --quiet

# GKE: restore
gcloud container clusters resize cross-cloud-control-plane \
  --zone=us-west2-a --node-pool=default-pool --num-nodes=2 --quiet

# Delete clusters entirely when done
eksctl delete cluster -f deploy/aws-eks/cluster-config.yaml
gcloud container clusters delete cross-cloud-control-plane --zone=us-west2-a --quiet
```

---

## 4. Future Enhancements

### 4.1 Azure Integration

- Add `azure` as a third `target_cloud` value
- Points of change:
  - `api/internal/validator/job_validator.go` — add `"azure"` to `validClouds`
  - New package `api/internal/presigner/adls_presigner.go` — Azure Blob SAS token generation using `github.com/Azure/azure-sdk-for-go/sdk/storage/azblob`
  - `api/internal/handler/jobs.go` — add `case "azure":` to the cloud switch
  - `deploy/azure-aks/` — AKS cluster config, RBAC, Workload Identity
  - OPA policy: add `azure` cloud-storage affinity rule for `wasbs://` or `abfs://` paths
  - Considerations: Azure Workload Identity (AAD Pod Identity replacement), AKS OIDC issuer for IRSA-equivalent

### 4.2 Automatic Preemption Notification

Currently the API is fire-and-forget after 202. Options for production notification:

- **Webhook callback:** accept `callback_url` in job payload; post `{"job_id":"...", "status":"preempted"}` on eviction
- **Event bus:** publish preemption events to SNS (AWS) or Pub/Sub (GCP); callers subscribe and resubmit
- **Kueue workload finalizer:** custom controller watches `Workload` objects for `Evicted` condition and calls configured webhook
- **Status polling endpoint:** add `GET /api/v1/jobs/{job_id}` backed by a K8s informer watching Kueue Workloads and SparkApplication CRDs

### 4.3 Kubeconfig Token Rotation for Cross-Cluster Dispatch

The `cross-cloud-kubeconfig` Secret mounted in the control plane API pod uses static ServiceAccount tokens to dispatch CRDs to EKS and GKE. EKS caps token TTL at 24 hours; GKE tokens are bounded by the cluster's token review policy. These tokens expire and must be rotated.

Options for production:

- **CronJob-based rotation:** a Kubernetes CronJob (or Cloud Scheduler trigger) runs daily, generates fresh tokens via `kubectl create token`, rebuilds the kubeconfig, and patches the Secret. The pod picks up the updated Secret automatically via volume mount refresh.
- **Workload Identity Federation (recommended):** instead of static tokens, use GCP Workload Identity to allow the control plane SA to impersonate an AWS IAM role (via OIDC). This eliminates token expiry entirely — no Secret, no rotation, no TTL.
- **External Secrets Operator:** store the kubeconfig in GCP Secret Manager or AWS Secrets Manager, managed externally; ESO syncs it into the K8s Secret on a schedule.

Points of change for CronJob rotation:
  - New manifest: `deploy/gke-control-plane/kubeconfig-rotator-cronjob.yaml`
  - ServiceAccount with permission to create tokens on both clusters and patch the Secret
  - Script: `scripts/rotate-kubeconfig.sh` — wraps `kubectl create token` + `kubectl create secret --dry-run -o yaml | kubectl apply`

---

## 5. Reference Docs

| Topic | File |
|---|---|
| Troubleshooting (Spark Operator, Kueue, OPA, API access, throttling) | [`docs/TROUBLESHOOTING.md`](TROUBLESHOOTING.md) |
| Operational design decisions (HA, security, cost, spot instances) | [`docs/DESIGN.md §5`](DESIGN.md#5-operational-design-decisions) |
| Batch preemption mechanics and resubmission | [`docs/DESIGN.md §2.8`](DESIGN.md#28-batch-preemption--mechanics) |
