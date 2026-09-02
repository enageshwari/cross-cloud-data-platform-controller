# Cross-Cloud Data Platform Controller

A GKE-hosted control plane for submitting and governing Spark and Flink batch workloads across AWS EKS and GCP GKE data planes. A single REST API handles job submission for both clouds — OPA Gatekeeper enforces regional data residency at admission time, Kueue manages priority scheduling and preemption, and Cluster Autoscaler eliminates idle compute cost.

## Quick Start — Local (no cloud required)

```
api/                               Go control plane
  cmd/server/main.go               Entry point — initialises all 6 components on startup
  internal/handler/jobs.go         HTTP handler — routes by (engine, target_cloud)
  internal/spark/submitter.go      SparkApplication CRD submitter (dynamic K8s client)
  internal/flink/submitter.go      AppWrapper CRD submitter (dynamic K8s client)
  internal/presigner/              S3 presigner (AWS SDK v2) + GCS signer (IAM SignBlob)
  internal/validator/              Schema validation
  internal/model/job.go            Request/response structs
  Dockerfile                       Multi-stage distroless build

deploy/
  gke-control-plane/
    control-api-deployment.yaml    Deployment, Service, ServiceAccount, PDB
    namespace.yaml                 data-workloads namespace
    spark-crd-creator-rbac.yaml    ClusterRole + bindings for CRD dispatch
  aws-eks/
    cluster-config.yaml            eksctl cluster definition
    spot-nodegroup.yaml            Spot node group config
    cluster-autoscaler.yaml        CA deployment
    cluster-autoscaler-policy.json IAM policy for Cluster Autoscaler
    spark-test-job.yaml            Sample SparkApplication for OPA testing
  gcp-gke/
    preemptible-nodepool.yaml      Future preemptible node pool (not yet provisioned)

policy/
  templates/                       OPA ConstraintTemplate — DataResidency (Rego)
  constraints/                     DataResidency constraint (enforce mode)

scheduler/kueue/
  resource-flavors.yaml            gcp-standard-flavor, aws-spot-flavor, on-demand-flavor
  cluster-queue.yaml               multi-cloud-cluster-queue with cohort + preemption
  local-queues.yaml                batch-data-queue, interactive-data-queue
  priority-classes.yaml            batch-low (100), interactive-high (1000)
  prometheus-scrape-config.yaml    Manual Prometheus scrape config snippet
  prometheus-scrape.yaml           Prometheus Operator ServiceMonitor + RBAC

scripts/
  *.sh                             Test and ops shell scripts (see docs/TESTING.md)
  assemble_snapshot.py             Snapshot assembler (used by metrics-snapshot.sh)
  parse_nodes.py                   Node metrics parser
  parse_workloads.py               Kueue workload parser
  scrape_kueue.py                  Kueue /metrics scraper
  capture-autoscale-evidence.py    CA log and node state collector

test-result-snapshots/
  20260901T*.json                  Metrics snapshots from load/preemption tests
  20260902T190212Z-post-gcp*.json  Post-GCP-verification snapshot
  autoscaling-preemption-test-evidence.txt  CA scale events + preemption event log

docs/
  DESIGN.md                        HLD diagrams, LLD, component inventory, observability
  REQUIREMENTS.md                  Functional and non-functional requirements
  TESTING.md                       Test methodology, verified results for all 4 paths
  CONTRIBUTIONS.md                 Setup guide, future enhancements
  TROUBLESHOOTING.md               Known issues and fixes
```

## Quick Start — Local (no cloud required)

```bash
cd api
go build -o /tmp/control-api ./cmd/server

# Create api/.env with credentials (see docs/CONTRIBUTIONS.md §1)
/tmp/control-api &

curl -s http://localhost:8080/healthz
# → {"status":"ok"}

# Submit a job (generates presigned URL; CRD dispatch requires clusters up)
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "etl-test",
    "engine":       "spark",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "main_class":   "org.apache.spark.examples.SparkPi",
    "artifact_uri": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority":     "batch-low"
  }' | python3 -m json.tool
```

## Deploy to GKE

```bash
# 1. Build and push image
gcloud builds submit api/ \
  --tag=us-west2-docker.pkg.dev/<PROJECT>/cross-cloud-api/control-api:latest

# 2. Apply manifests (order matters)
kubectl apply -f deploy/gke-control-plane/namespace.yaml
kubectl apply -f policy/templates/data-residency-template.yaml
kubectl apply -f policy/constraints/data-residency-constraint.yaml
kubectl apply -f scheduler/kueue/resource-flavors.yaml
kubectl apply -f scheduler/kueue/cluster-queue.yaml
kubectl apply -f scheduler/kueue/local-queues.yaml
kubectl apply -f scheduler/kueue/priority-classes.yaml
kubectl apply -f deploy/gke-control-plane/control-api-deployment.yaml
kubectl apply -f deploy/gke-control-plane/spark-crd-creator-rbac.yaml

# 3. Create required secrets
kubectl create secret generic aws-presigner-creds \
  --namespace=data-workloads \
  --from-literal=access-key-id=<KEY_ID> \
  --from-literal=secret-access-key=<SECRET>

kubectl create secret generic cross-cloud-kubeconfig \
  --from-file=kubeconfig=<path-to-static-kubeconfig> \
  -n data-workloads
```

> **Prerequisites:** OPA Gatekeeper, Kueue, Spark Operator, and Flink Operator (with AppWrapper) must be installed on each target cluster. See `docs/CONTRIBUTIONS.md §2` for full cluster setup.

## Architecture

[`docs/DESIGN.md`](docs/DESIGN.md) — hub-and-spoke HLD, end-to-end job lifecycle LLD, Kueue admission detail (AppWrapper vs native), observability design, security architecture, and operational design decisions.

## Test Results

All 4 dispatch paths verified end-to-end from the deployed GKE pod (2026-09-02):

| Path | Job ID | Result |
|---|---|---|
| AWS Spark | `aws-spark-verify-17883733260108` | COMPLETED |
| AWS Flink | `aws-flink-verify-17883733269713` | FINISHED/STABLE |
| GCP Spark | `gcp-spark-test-17883723573466` | COMPLETED |
| GCP Flink | `gcp-flink-test-17883726767214` | FINISHED/STABLE |

See [`docs/TESTING.md`](docs/TESTING.md) for full test methodology, OPA enforcement evidence, preemption test results, and autoscale evidence.
