# Cross-Cloud Data Platform Controller

A GKE-hosted control plane for submitting and governing Spark and Flink batch workloads across AWS EKS and GCP GKE data planes. A single REST API handles job submission for both clouds — OPA Gatekeeper enforces regional data residency at admission time, Kueue manages priority scheduling and preemption, and Cluster Autoscaler eliminates idle compute cost.

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

## Project Layout

```
api/internal/          Go — handler, spark/flink submitters, S3/GCS presigners, validator
policy/templates/      OPA ConstraintTemplate (Rego)
scheduler/kueue/       Kueue ClusterQueue, LocalQueues, ResourceFlavors
deploy/                GKE, EKS, and GCP cluster manifests + RBAC
scripts/demo-run.sh    Runs all 7 demo test cases end-to-end
docs/                  DESIGN, REQUIREMENTS, TESTING, CONTRIBUTIONS, TROUBLESHOOTING
```

## Architecture

[`docs/DESIGN.md`](docs/DESIGN.md) — hub-and-spoke HLD, end-to-end job lifecycle LLD, Kueue admission detail (AppWrapper vs native), observability design, security architecture, and operational design decisions.

## Test Results

All 4 dispatch paths verified end-to-end from the deployed GKE pod (2026-09-02):

| Path | Job ID | Result |
|---|---|---|
| AWS Spark | `aws-spark-verify-17883733260108` | ✅ Verified |
| AWS Flink | `aws-flink-verify-17883733269713` | ✅ Verified |
| GCP Spark | `gcp-spark-test-17883723573466` | ✅ Verified |
| GCP Flink | `gcp-flink-test-17883726767214` | ✅ Verified |

See [`docs/TESTING.md`](docs/TESTING.md) for full test methodology, OPA enforcement evidence, preemption test results, and autoscale evidence.
