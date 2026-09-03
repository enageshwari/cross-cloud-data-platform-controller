# Design Specification
## Cross-Cloud Data Platform Controller

---

## 1. High-Level Design (HLD)

### 1.1 Architectural Overview

**Hub-and-spoke** means one central cluster (the hub) makes all decisions — policy enforcement, scheduling, credential generation — and then sends work out to one or more worker clusters (the spokes) that only run jobs. The spokes never talk to each other and never hold policy logic. This keeps compliance and security controls in one place even when execution is spread across multiple clouds.

In this system: GKE (`cross-cloud-control-plane`) is the hub. AWS EKS (`cross-cloud-data-plane`) is the current spoke. A GCP GKE data plane can be added as a second spoke without changing the control plane.

A single GKE management cluster houses the Go control plane, OPA policy engine, and Kueue scheduler. It dispatches signed output credentials and SparkApplication CRDs to downstream data planes (AWS EKS and GCP GKE).

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    CENTRAL MANAGEMENT CLUSTER — GKE                        │
│                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  Go Control API                                                      │  │
│  │  POST /api/v1/jobs                                                   │  │
│  │  • Schema validation           • S3 presigner (AWS SDK v2)           │  │
│  │  • GCS signer (IAM SignBlob)   • SparkApplication / AppWrapper CRD   │  │
│  │  • cross-cloud-kubeconfig Secret (static SA tokens, 24h TTL)         │  │
│  └────────────────────────────────-─┬───────────────────────────────────┘  │
└─────────────────────────────────────┼──────────────────────────────────────┘
                                      │  Dispatch CRD + signed credential
                     ┌────────────────┴────────────────┐
                     │                                 │
                     ▼                                 ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│  AWS EKS DATA PLANE       │   │  GCP GKE DATA PLANE       │
│  us-west-1 (spot nodes)   │   │  us-west2-a (std nodes)   │
│                           │   │                           │
│  OPA Gatekeeper           │   │  OPA Gatekeeper           │
│  (validates CRD at        │   │  (validates CRD at        │
│   K8s admission)          │   │   K8s admission)          │
│                           │   │                           │
│  Kueue                    │   │  Kueue                    │
│  (queues + preempts)      │   │  (queues + preempts)      │
│                           │   │                           │
│  Spark Operator           │   │  Spark Operator           │
│  Flink Operator           │   │  Flink Operator           │
│  AppWrapper controller    │   │  AppWrapper controller    │
│  Cluster Autoscaler       │   │  Workload Identity        │
└─────────────┬─────────────┘   └─────────────┬─────────────┘
              │ IRSA                          │ Workload Identity
              │ (no credentials in pod)       │ (no credentials in pod)
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│  AWS S3                 │     │  GCP GCS                │
│  us-west-1  (private)   │     │  us-west2   (private)   │
└─────────────────────────┘     └─────────────────────────┘
```

### 1.2 Data Flow Diagram

```
Developer
    │
    │  POST /api/v1/jobs
    │  { engine, target_cloud, region, priority, input_path, output_path }
    ▼
Go Control API (GKE pod)
    │
    ├─ 1. Schema validation (engine, cloud, region, priority)
    │      → 422 if invalid — fast feedback before any cloud call
    │
    ├─ 2. Credential generation
    │      aws → pre-signed S3 PUT URL (15 min TTL, S3PresignClient)
    │      gcp → V4 signed GCS PUT URL (15 min TTL, IAM SignBlob via ADC)
    │
    ├─ 3a. AWS job → build SparkApplication or AppWrapper CRD
    │       └─ write to EKS via cross-cloud-kubeconfig Secret token
    │
    ├─ 3b. GCP job → build SparkApplication or AppWrapper CRD
    │       └─ write to GKE data plane via cross-cloud-kubeconfig Secret token
    │
    └─ 4. Return 202: { job_id, status, output_credential, workload_name }
           Note: 202 means "submitted to K8s" — OPA admission happens next

EKS / GKE data plane (after CRD lands in etcd)
    │
    ├─ 5. OPA Gatekeeper validating webhook intercepts at K8s admission
    │      → hard DENY if outputPath violates region or cloud-storage rule
    │      → CRD never persisted to etcd if denied
    │
    ├─ 6. Kueue intercepts admitted CRD — queues by priority (batch / interactive)
    │
    ├─ 7. Cluster Autoscaler scales up nodes if pods are unschedulable
    │
    └─ 8. Engine operator creates pods → write output via IRSA / Workload Identity
```

### 1.3 User Interaction Diagram

#### AWS job (target_cloud=aws)

```
┌──────────┐  ┌────────────────┐  ┌───────────────────┐  ┌──────────┐
│Developer │  │  Control API   │  │  EKS / Kueue      │  │  AWS S3  │
└────┬─────┘  └───────┬────────┘  └─────────┬─────────┘  └───-─┬────┘
     │                │                     │                  │
     │─ POST /jobs ──▶│                     │                  │
     │                │─ OPA admit          │                  │
     │                │─ presign S3 URL     │                  │
     │                │─ SparkApplication ─▶│                  │
     │◀─ 202 + cred ──│                     │                  │
     │                │                     │─ Kueue enqueue   │
     │                │                     │─ CA scale up     │
     │                │                     │─ pod scheduled   │
     │                │                     │─ write S3 ──────▶│
     │                │                     │─ pod completes   │
```

#### GCP job (target_cloud=gcp)

```
┌──────────┐  ┌────────────────┐  ┌───────────────────┐  ┌──────────┐
│Developer │  │  Control API   │  │  GKE / Kueue      │  │  GCS     │
└────┬─────┘  └───────┬────────┘  └─────────┬─────────┘  └─-───┬────┘
     │                │                     │                  │
     │─ POST /jobs ──▶│                     │                  │
     │                │─ OPA admit          │                  │
     │                │─ sign GCS URL       │                  │
     │                │─ AppWrapper ───────▶│                  │
     │◀─ 202 + cred ──│                     │                  │
     │                │                     │─ Kueue enqueue   │
     │                │                     │─ pod scheduled   │
     │                │                     │─ write GCS ─────▶│
     │                │                     │─ pod completes   │
```

---

## 2. Low-Level Design (LLD)

### 2.1 Job Lifecycle — End-to-End Flow Diagram

#### Full pipeline: submit → validate → transform → admit → execute

```
  CLIENT
    │
    │  POST /api/v1/jobs
    │  { job_name, engine, target_cloud, region,
    │    artifact_uri, input_path, output_path, priority }
    │
    ▼
┌─────────────────────────────────────────────────────────────┐
│  Go Control API                                             │
│                                                             │
│  1. SCHEMA VALIDATION                                       │
│     • engine ∈ {spark, flink}                               │
│     • target_cloud ∈ {aws, gcp}                             │
│     • priority ∈ {batch-low, interactive-high}              │
│     → 422 if any check fails                                │
│                                                             │
│  2. CREDENTIAL GENERATION                                   │
│     • aws → pre-signed S3 PUT URL  (15 min TTL)             │
│     • gcp → V4 signed GCS PUT URL  (15 min TTL)             │
│                                                             │
│  3. CRD CONSTRUCTION                                        │
│     spark → SparkApplication                                │
│     flink → AppWrapper (wrapping FlinkDeployment)           │
│     • queue label injected (batch / interactive)            │
│     • cloud-specific storage config injected                │
│                                                             │
│  4. WRITE CRD to target cluster via dynamic K8s client      │
│     → 202 Accepted + output_credential returned to caller   │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  KUBERNETES API SERVER                                      │
│                                                             │
│  5. VALIDATING WEBHOOK — OPA Gatekeeper                     │
│     • outputPath must contain region token                  │
│     • aws → s3://, gcp → gs://                              │
│     → hard DENY if either rule fails                        │
│     → object never persisted to etcd if denied              │
│                                                             │
│  6. CRD persisted to etcd (only if OPA admits)              │
└──────────────────────────┬──────────────────────────────────┘
                           │
          ┌────────────────┴────────────────┐
          │ engine=spark                    │ engine=flink
          ▼                                 ▼
  SparkApplication CRD              AppWrapper CRD
  (Kueue-native)                    (Kueue envelope)
          │                                 │
          └────────────────┬────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  KUEUE ADMISSION LAYER                                      │
│                                                             │
│  7. Kueue webhook intercepts CRD on CREATE                  │
│     • reads queue-name label → suspends workload            │
│     • creates Workload object for quota tracking            │
│                                                             │
│  8. ClusterQueue evaluates available quota                  │
│     spot-instance-flavor (primary) / on-demand (overflow)   │
│                                                             │
│  9. Priority-based admission                                │
│     interactive-high preempts batch-low if quota exhausted  │
│     → admitted: workload unsuspended                        │
└──────────────────────────┬──────────────────────────────────┘
                           │  workload admitted
          ┌────────────────┴────────────────┐
          │ engine=spark                    │ engine=flink
          ▼                                 ▼
  Spark Operator reconciles        AppWrapper controller
  SparkApplication directly        extracts FlinkDeployment
  → driver pod                     → Flink Operator reconciles
  → executor pod(s)                → JobManager pod
                                   → TaskManager pod(s)
          │                                 │
          └────────────────┬────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  EXECUTION                                                  │
│                                                             │
│  10. Pods scheduled on spot nodes                           │
│      Cluster Autoscaler scales up if unschedulable          │
│      Spark: writes via IRSA (no key in pod)                 │
│      Flink: writes via SA annotation (no key in pod)        │
│                                                             │
│  11. Job completes → pods deleted                           │
│      CRD remains (delete manually or via TTL controller)    │
│      Kueue Workload transitions to Finished                 │
│      Cluster Autoscaler scales down after idle window       │
└─────────────────────────────────────────────────────────────┘
```

#### Kueue admission layer — AppWrapper vs native CRD

```
                    CRD written to K8s API server
                              │
              ┌───────────────┴───────────────┐
              │ engine=spark                  │ engine=flink
              │ SparkApplication              │ AppWrapper
              │ (Kueue-native integration)    │ (CodeFlare envelope)
              ▼                               ▼
  ┌───────────────────────┐       ┌───────────────────────────┐
  │ Kueue mutating webhook│       │ Kueue mutating webhook    │
  │ recognises            │       │ recognises AppWrapper     │
  │ SparkApplication as   │       │ as a managed workload     │
  │ a managed workload    │       └────────────┬──────────────┘
  └──────────┬────────────┘                    │
             │                                 │
             ▼                                 ▼
  Workload object created         Workload object created
  podSets inferred from           podSets declared explicitly
  driver/executor spec            in AppWrapper spec
             │                                 │
             └───────────────┬─────────────────┘
                             ▼
                  ClusterQueue evaluates quota
                  (spot-instance-flavor first,
                   on-demand-flavor as overflow)
                             │
                    ┌────────┴────────┐
                    │ quota available │ quota exhausted
                    ▼                 ▼
               admit workload    hold in queue
                                 (preempt lower-priority
                                  if interactive-high)
                    │
                    ▼
         ┌──────────────────────────────────┐
         │ engine=spark                     │
         │ Spark Operator reconciles        │
         │ SparkApplication directly        │
         │ → driver pod                     │
         │ → executor pod(s)                │
         └──────────────────────────────────┘

         ┌──────────────────────────────────┐
         │ engine=flink                     │
         │ AppWrapper controller extracts   │
         │ inner FlinkDeployment template   │
         │ → creates FlinkDeployment CRD    │
         │ Flink Operator reconciles        │
         │ → JobManager pod                 │
         │ → TaskManager pod(s)             │
         └──────────────────────────────────┘
```

> **Key distinction:** For Spark, the operator acts directly on the admission-layer CRD (`SparkApplication`). For Flink, the admission-layer CRD (`AppWrapper`) is just a wrapper — the AppWrapper controller extracts and materialises the inner `FlinkDeployment`, and the Flink Operator then acts on that. Two reconcile loops for Flink vs one for Spark.

---

### 2.2 Go API — Cloud-Agnostic Job Submission Layer

```go
type JobSubmission struct {
    JobName     string `json:"job_name"`
    Engine      string `json:"engine"`       // wired end-to-end: spark, flink
    TargetCloud string `json:"target_cloud"` // aws | gcp
    Region      string `json:"region"`       // us-west-1 | us-west2
    ArtifactURI string `json:"artifact_uri"`
    InputPath   string `json:"input_path"`   // s3:// or gs://
    OutputPath  string `json:"output_path"`  // region-locked bucket
    Priority    string `json:"priority"`     // batch-low | interactive-high
}

type JobResponse struct {
    JobID            string            `json:"job_id"`
    Status           string            `json:"status"`
    Message          string            `json:"message"`
    OutputCred       *OutputCredential `json:"output_credential,omitempty"`
    SparkApplication string            `json:"spark_application,omitempty"`
}

type OutputCredential struct {
    Type      string `json:"type"`       // presigned_put_url | signed_put_url
    URL       string `json:"url"`
    ObjectKey string `json:"object_key"`
    Bucket    string `json:"bucket"`
    Region    string `json:"region"`
    ExpiresAt string `json:"expires_at"` // RFC3339
}
```

**Design choice — dynamic Kubernetes client for SparkApplication:**
Rather than importing the full Spark Operator Go module (which adds ~300 MB of transitive deps), we use `k8s.io/client-go/dynamic` and construct the SparkApplication as `unstructured.Unstructured`. This keeps the binary lean and avoids version-pinning the Spark Operator API types.

**Handler routing — how (engine, target_cloud) maps to a submitter:**

```
POST /api/v1/jobs
        │
        ├─ target_cloud = "aws"
        │       │
        │       ├─ engine = "spark"  → sparkSubAWS.Submit()  → SparkApplication CRD → EKS
        │       └─ engine = "flink"  → flinkSubAWS.Submit()  → AppWrapper CRD       → EKS
        │
        └─ target_cloud = "gcp"
                │
                ├─ engine = "spark"  → sparkSubGCP.Submit()  → SparkApplication CRD → GKE
                └─ engine = "flink"  → flinkSubGCP.Submit()  → AppWrapper CRD       → GKE

Each submitter uses:
  - KUBECONFIG=/etc/kubeconfig/kubeconfig (mounted from cross-cloud-kubeconfig Secret)
  - KubeContext = EKS_CONTEXT or GKE_CONTEXT env var
  - dynamic.Interface.Resource(GVR).Namespace("data-workloads").Create()

If a submitter is nil (context missing / token expired):
  - Presigned URL is still returned (credential generation always runs)
  - spark_application field is absent from 202 response
  - WARN log: "submitter not available"
```

### 2.3 OPA Gatekeeper — Automated Compliance Enforcement

Two rules in one ConstraintTemplate handle both validation concerns atomically. If either rule fires, the entire admission is denied — there is no partial allow.

The constraint is scoped to the `data-workloads` namespace and covers **both `SparkApplication` and `AppWrapper` CRDs** — enforcement applies regardless of whether the job was submitted via the Go API or directly via `kubectl apply`.

```rego
package datacontroller.residency

# Rule 1: Region affinity — output path must contain region token
violation[{"msg": msg}] {
    input.review.kind.kind == "SparkApplication"
    spec := input.review.object.spec
    not contains(spec.outputPath, spec.region)
    msg := sprintf("residency violation: %v not in %v", [spec.region, spec.outputPath])
}

# Rule 2: Cloud-storage affinity — aws→s3://, gcp→gs://
violation[{"msg": msg}] {
    input.review.kind.kind == "SparkApplication"
    spec := input.review.object.spec
    spec.targetCloud == "aws"
    not startswith(spec.outputPath, "s3://")
    msg := sprintf("cloud-storage affinity: aws jobs must use s3://, got: %v", [spec.outputPath])
}

violation[{"msg": msg}] {
    input.review.kind.kind == "SparkApplication"
    spec := input.review.object.spec
    spec.targetCloud == "gcp"
    not startswith(spec.outputPath, "gs://")
    msg := sprintf("cloud-storage affinity: gcp jobs must use gs://, got: %v", [spec.outputPath])
}
```

**Design choice — admission-time enforcement vs API-time:**
OPA runs at K8s admission to catch any job submitted outside the Go API (e.g. direct `kubectl apply`). The Go API does an early validation for fast feedback, but OPA is the authoritative enforcement point.

### 2.4 Kueue — Dynamic Resource Scheduling and Preemption

```
ClusterQueue: multi-cloud-cluster-queue
└── Cohort: cross-cloud-cohort
    ├── Flavor: gcp-standard-flavor  (GCP standard nodes, no nodeSelector)
    ├── Flavor: aws-spot-flavor      (EKS spot nodes, eks.amazonaws.com/capacityType=SPOT)
    └── Flavor: on-demand-flavor     (overflow, node.kubernetes.io/instance-type=on-demand)

LocalQueue: batch-data-queue         → priority class: batch-low (100)
LocalQueue: interactive-data-queue   → priority class: interactive-high (1000)
```

**Design choice — cohort-based preemption:**
Putting both queues in the same cohort enables `interactive-data-queue` to reclaim quota from `batch-data-queue` via Kueue's `reclaimWithinCohort: LowerPriority` policy. Without a cohort, cross-queue preemption is not possible. Verified in testing: interactive-high (1000) preempts batch-low (100) within seconds.

**Design choice — Kueue v0.9.1 over v0.7.0:**
v0.7.0 ships with `gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0` which was removed from GCR. v0.9.1 uses `quay.io/brancz/kube-rbac-proxy:v0.18.0`. Applied post-install patch to fix the image.

### 2.5 Credential Injection — Improved Security via Short-Lived Tokens

#### AWS job from GKE control plane (pre-signed URL pattern)

```
GKE API pod
  │
  ├── AWS SDK v2 S3PresignClient
  │     credentials: static (AKIARFKJPTJCWA2EJN7W via K8s Secret)
  │     signs: PUT s3://...us-west-1.../jobs/<id>/output
  │
  └── Returns URL to caller (15 min TTL)
       Worker uses plain HTTPS PUT — no AWS SDK needed in worker
```

#### GCP job from GKE control plane (IAM SignBlob pattern)

```
GKE API pod
  │
  ├── ADC (Application Default Credentials via Workload Identity)
  │     impersonates: gcs-presigner@project-965bb0cf-caa0-458d-ba9.iam.gserviceaccount.com
  │     calls: iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/.../signBlob
  │     base64-encodes input, decodes response
  │
  └── Returns V4 signed URL (15 min TTL)
       Worker uses plain HTTPS PUT — no GCP SDK needed
```

#### Spark executor on EKS (IRSA — no credentials)

```
EKS node → IRSA → spark-job-sa ServiceAccount
  → eks.amazonaws.com/role-arn annotation
  → short-lived STS token via OIDC
  → S3AFileSystem reads token from IMDS
  → no access key in pod spec, env vars, or mounted files
```

### 2.6 Observability Design — Metrics and Logs

#### Prometheus metrics (Kueue)

Kueue exposes a Prometheus-compatible `/metrics` endpoint on port `8443` of the `kueue-controller-manager` pod in the `kueue-system` namespace. Key metrics emitted:

| Metric | Description |
|---|---|
| `kueue_pending_workloads` | Number of workloads waiting to be admitted, labelled by `queue` and `status` |
| `kueue_admitted_active_workloads` | Currently running workloads per ClusterQueue |
| `kueue_evicted_workloads_total` | Cumulative preemption count, labelled by `preemption_reason` |
| `kueue_admitted_workloads_total` | Total admissions since controller start |

**Scraping:** in a cluster with Prometheus Operator, add a `ServiceMonitor` targeting `kueue-system/kueue-controller-manager-metrics-service` on port `https`. Without Prometheus Operator, use a manual scrape job:

```yaml
scrape_configs:
  - job_name: kueue
    scheme: https
    tls_config:
      insecure_skip_verify: true
    static_configs:
      - targets: ['kueue-controller-manager.kueue-system.svc:8443']
```

For a quick point-in-time snapshot without Prometheus infrastructure:

```bash
# Port-forward then curl — grabs current metric values
kubectl port-forward svc/kueue-controller-manager-metrics-service \
  8444:8443 -n kueue-system &
curl -sk https://localhost:8444/metrics | grep kueue_pending
```

Captured snapshots from the running system are in `test-result-snapshots/` (5 JSON files covering queue depth under load, preemption events, and admission counts).

#### Where metrics are stored and accessed

| Environment | Storage | Access |
|---|---|---|
| Local / demo | `test-result-snapshots/*.json` (point-in-time curl output) | `cat test-result-snapshots/<file>.json` |
| GKE (production) | Cloud Monitoring via GKE Metrics Server + Prometheus sidecar | GCP Console → Monitoring → Metrics Explorer → filter `kueue_*` |
| EKS (production) | Amazon Managed Prometheus (AMP) + ADOT collector | AWS Console → AMP workspace → PromQL query |

#### Structured API logs (GCP Cloud Logging)

The Go control API logs every request as a single JSON line on stdout. When deployed to GKE, these are automatically ingested into Cloud Logging:

```
GKE pod stdout
  → Cloud Logging agent (DaemonSet)
  → Log sink: projects/project-965bb0cf-caa0-458d-ba9/logs/control-api
```

**Sample log line (AWS Spark job):**
```json
{"time":"2026-09-02T18:43:11Z","level":"INFO","method":"POST","path":"/api/v1/jobs",
 "job_id":"job-20250902184311-abc1","engine":"spark","target_cloud":"aws",
 "region":"us-west-1","priority":"batch-low","status":202,"latency_ms":312}
```

**Sample log line (GCP signed URL job):**
```json
{"time":"2026-09-02T18:51:04Z","level":"INFO","method":"POST","path":"/api/v1/jobs",
 "job_id":"job-20250902185104-def2","engine":"flink","target_cloud":"gcp",
 "region":"us-west2","priority":"interactive-high","status":202,"latency_ms":198}
```

Query in Cloud Logging:
```
resource.type="k8s_container"
resource.labels.namespace_name="data-workloads"
jsonPayload.path="/api/v1/jobs"
```

#### Cluster Autoscaler logs (EKS)

CA logs scale-up and scale-down decisions to the pod log. Captured evidence is in `test-result-snapshots/autoscaling-preemption-test-evidence.txt`.

```bash
# Live CA decisions
kubectl logs -f deployment/cluster-autoscaler -n kube-system | grep -E "scale|node"

# Scale-up triggered by pending pods
# Expected output: "Scale-up: setting group ... to 2"

# Scale-down after inactivity
# Expected output: "Scale-down: removing node ... utilization 0.12 < 0.50"
```

### 2.7 Spark Operator Integration — Operational Notes

> The operational troubleshooting details for Spark Operator (namespace config, `batchScheduler` flag, app name derivation) have been moved to **`docs/TROUBLESHOOTING.md`** (Spark Operator — namespace and scheduler configuration). Design decisions are captured in §2.2 above (dynamic Kubernetes client choice).

### 2.8 Batch Preemption — Mechanics

**What happens:** When an `interactive-high` job triggers preemption, Kueue evicts the lowest-priority batch workload. The evicted `Workload` object transitions to `Evicted` status and its pods are deleted. The `SparkApplication` CRD remains in the cluster but driver/executor pods are gone — the job does not auto-restart.

**Detection:** The `spark_application` name is returned in the original 202 response. Callers can poll it:

```bash
# Check workload eviction status
kubectl get workloads -n data-workloads
# Look for: ADMITTED=False, reason=Preempted

# Watch preemption events live
kubectl get events -n data-workloads --field-selector reason=Preempted -w

# Check SparkApplication state
kubectl get sparkapplication <name> -n data-workloads
# STATUS will be blank or FAILING after preemption
```

**Resubmission (required — no auto-retry):**

```bash
# 1. Delete the failed CRD
kubectl delete sparkapplication <name> -n data-workloads

# 2. Resubmit via the same API payload
curl -s -X POST http://localhost:9090/api/v1/jobs -H "Content-Type: application/json" -d '{...}'
```

The pre-signed S3/GCS output URL from the original response remains valid for 15 minutes and can be reused if resubmission happens within the TTL window.

**Production path:** see `docs/CONTRIBUTIONS.md §4.2` for webhook callback, event bus, and status polling endpoint options.

---

## 3. Component Inventory

| Component | Language / Tool | Version | Location |
|---|---|---|---|
| Control plane API | Go | 1.25 | `api/` |
| S3 presigner | Go (AWS SDK v2) | v1.110+ | `api/internal/presigner/s3_presigner.go` |
| GCS signer | Go (cloud.google.com/go/storage) | v1.66 | `api/internal/presigner/gcs_presigner.go` |
| Spark CRD submitter | Go (k8s.io/client-go dynamic) | v0.31.0 | `api/internal/spark/submitter.go` |
| Flink CRD submitter | Go (k8s.io/client-go dynamic) | v0.31.0 | `api/internal/flink/submitter.go` |
| OPA policy | Rego | Gatekeeper v3.16 | `policy/` |
| Kueue config | YAML | v0.9.1 | `scheduler/kueue/` |
| Resource flavors | YAML | — | `scheduler/kueue/resource-flavors.yaml` |
| Priority classes | YAML | — | `scheduler/kueue/priority-classes.yaml` |
| Prometheus scrape | YAML | — | `scheduler/kueue/prometheus-scrape.yaml` |
| GKE manifests | YAML | K8s 1.35 | `deploy/gke-control-plane/` |
| CRD creator RBAC | YAML | — | `deploy/gke-control-plane/spark-crd-creator-rbac.yaml` |
| EKS config | eksctl YAML | K8s 1.31 | `deploy/aws-eks/` |
| Container image | Dockerfile (distroless) | Go 1.25-alpine | `api/Dockerfile` |
| CI/CD | Cloud Build (GCP) | — | triggered manually |
| AppWrapper | CRD (CodeFlare) | v1beta2 | installed on each data plane |
| Spark Operator | Helm (kubeflow) | v2.5.2 | installed on each data plane |
| Flink Operator | Helm | v1beta1 | installed on each data plane |

---

## 4. Security Architecture

```
No secrets in Git
       │
       ├── AWS creds  → K8s Secret aws-presigner-creds (not in repo)
       ├── GCS creds  → ADC / Workload Identity (no key file)
       ├── IRSA       → OIDC token (short-lived, auto-rotated)
       └── .env file  → .gitignore covers it

Cross-cluster dispatch (control plane → EKS / GKE data planes)
       │
       └── cross-cloud-kubeconfig Secret
             → static ServiceAccount tokens (EKS: 24h TTL, GKE: cluster policy)
             → scoped to CRD create in data-workloads namespace only
             → NOT in git — created imperatively via kubectl create secret
             → known operational limitation: tokens expire and must be rotated
             → production path: Workload Identity Federation (see CONTRIBUTIONS.md §4.3)

Network boundaries
       │
       ├── Control API: ClusterIP (no external exposure; port-forward for local test)
       ├── OPA webhook: cluster-internal admission webhook
       └── EKS→S3:     VPC endpoint (optional) or public S3 HTTPS
```

---

## 5. Operational Design Decisions

These are the concrete choices made during implementation that affect how the system behaves in production — not conventions, but decisions with specific reasoning.

### Kueue Applicability — Job-Shaped vs Cluster-Shaped Engines

Kueue is designed for **job-shaped workloads** — compute tasks with a defined start, run, and end. Both current engines fit this model:

| Engine | Shape | Kueue path |
|---|---|---|
| Spark | Job (driver + executors, terminates) | Native Kueue integration via Spark Operator |
| Flink | Job (JobManager + TaskManagers, terminates) | AppWrapper bridge (no native Kueue support in Flink Operator) |

**Why not AppWrapper for Spark too?** Spark has native Kueue support via the Spark Operator's `batchScheduler` hook — no envelope needed. Using AppWrapper would add a second reconcile loop for no gain. The split (native for Spark, AppWrapper for Flink) reflects what upstream operators actually support.

### High Availability

- **2 API replicas with hard `podAntiAffinity`** (`topologyKey: kubernetes.io/hostname`) — replicas are forced onto separate nodes by the scheduler. A single node failure cannot take down the API.
- **`PodDisruptionBudget minAvailable: 1`** — node drains and rolling upgrades cannot evict both replicas simultaneously. At least one is always serving.
- **EKS: 2 spot nodes across `us-west-1a` and `us-west-1c`** — separate AZs mean an AZ-level spot interruption event is unlikely to affect both nodes at once.

### Security

- **No credentials in Git** — AWS presigner keys live in a K8s Secret (`aws-presigner-creds`), GCS signing uses ADC/Workload Identity, Spark executor pods use IRSA (OIDC short-lived tokens). Nothing long-lived is stored in the repo or in container images.
- **`.gitignore` covers `*.env`, `*.pem`, `*.key`, `kubeconfig`** — defence-in-depth; a misconfigured `git add .` cannot accidentally commit credentials.
- **OPA webhook is the authoritative enforcement point** — cloud-storage affinity and region residency cannot be bypassed by direct `kubectl apply`. The Go API does an early validation for fast feedback, but the webhook is the hard gate.
- **S3 bucket:** public access blocked, AES-256 encryption at rest, no versioning (versioning adds cost without benefit for this workload pattern).
- **GCS bucket:** uniform bucket-level access, public access prevention enforced at the bucket IAM level.
- **Cross-cluster dispatch credential — known operational limitation:** the `cross-cloud-kubeconfig` Secret uses static ServiceAccount tokens to dispatch CRDs from the GKE control plane to EKS and GKE data planes. EKS caps these tokens at 24h. The Secret is not stored in Git and is scoped to CRD create in `data-workloads` only — not cluster admin. This is an accepted tradeoff for a showcase system. Production path: Workload Identity Federation eliminates the token entirely (see `docs/CONTRIBUTIONS.md §4.3`).

### Cloud-Agnostic Interface Layer

- **Identical job payload for AWS and GCP** — only `target_cloud` and `region` differ. The caller never needs to know which cloud SDK is in use.
- **Storage scheme enforced by OPA, not by the API** — `s3://` vs `gs://` is a policy rule, not an API validation rule. This means the same API version can accept a new cloud target (e.g. Azure `wasbs://`) by adding an OPA rule, without an API version bump.
- **Dynamic K8s client (`unstructured.Unstructured`) for CRD submission** — avoids importing the Spark Operator or Flink Operator Go modules (~300 MB of transitive deps). The binary stays lean and version-pinning the operator API types is not required.

### Auto-Scale and Cost

- **`minSize=0` on EKS node group** — full scale-to-zero when idle. No nodes running means no node cost. Cluster Autoscaler scales up within ~90 seconds when a new job arrives.
- **Mixed spot instance types (`t3.xlarge` + `t3a.xlarge`)** — EKS picks whichever has available spot capacity. Two types reduces the probability that both are interrupted simultaneously (~70% cheaper than on-demand for this workload).
- **Kueue cohort-based preemption** — both queues share the same cohort (`cross-cloud-cohort`), which is required for cross-queue preemption. Without a cohort, an interactive job cannot reclaim quota from a batch job even if nodes are available.
- **`batchScheduler.enable=false` on Spark Operator** — the `batchScheduler` integration suspends all jobs by default until an external scheduler unsuspends them. Disabled until Kueue `externalFrameworks` is properly configured; Kueue admission via labels is sufficient for this system.

### Resource Sizing Rationale

| Component | Request | Reasoning |
|---|---|---|
| Control plane API | 100m CPU / 128Mi | Handles HTTP + AWS/GCS SDK calls; no compute-heavy work. Fits alongside system pods on e2-standard-2. |
| Spark driver | 1 core / 512Mi | Demo workload (SparkPi). Scale up `coreRequest` for real ETL. |
| Spark executor | 1 core / 512Mi | 1 executor instance. Increase `instances` for parallelism. |
| Flink JobManager | 0.5 core / 1024Mi | Coordinator role — lightweight relative to task processing. |
| Flink TaskManager | 0.5 core / 1024Mi | 1 slot. Increase `taskmanager.numberOfTaskSlots` for parallelism. |
| Kueue spot quota | 16 CPU / 64Gi per flavor | gcp-standard-flavor covers GKE e2-standard-2 nodes; aws-spot-flavor covers EKS t3.xlarge (4 vCPU / 16Gi each). |
