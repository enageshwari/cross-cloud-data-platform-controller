# Requirements Specification
## Cross-Cloud Data Platform Controller

---

## 1. Problem Statement

Data engineering teams submitting batch and streaming workloads to cloud infrastructure face three compounding challenges: managing per-cloud credentials at the workload level, preventing data residency violations across regions and cloud boundaries, and avoiding idle over-provisioning of expensive compute. Teams need a single, cloud-agnostic control plane that enforces compliance automatically and optimizes cost without requiring per-team infrastructure knowledge.

---

## 2. Goals

- Single API surface for submitting jobs to AWS EKS or GCP GKE
- Automated enforcement of regional data residency at admission time
- Cost minimization through spot/preemptible instances and queue-based scheduling
- No long-lived credentials stored in workload pods
- Cloud-agnostic interface layer — same job payload works across clouds

---

## 3. Functional Requirements

### 3.1 Unified Job Submission API

| # | Requirement |
|---|---|
| F1 | `POST /api/v1/jobs` accepts a JSON payload specifying `engine`, `target_cloud`, `region`, `artifact_uri`, `input_path`, `output_path`, `priority` |
| F2 | Supported engines (end-to-end wired): `spark`, `flink` |
| F3 | Supported clouds: `aws`, `gcp` |
| F4 | Valid priorities: `batch-low`, `interactive-high` |
| F5 | API returns 202 Accepted with `job_id`, `status`, and `output_credential` |
| F6 | For AWS jobs: response includes a pre-signed S3 PUT URL (15 min TTL) — worker writes output without holding credentials |
| F7 | For GCP jobs: response includes a V4 signed GCS PUT URL (15 min TTL) — signed via IAM SignBlob API |
| F8 | For `spark` engine: API dispatches a `SparkApplication` CRD to the target data plane (EKS or GKE) via Kubernetes dynamic client. For `flink` engine: API dispatches an `AppWrapper` CRD (wrapping a `FlinkDeployment`) to the target data plane |
| F9 | All requests produce structured JSON logs (timestamp, level, method, path, job_id, engine, target_cloud, region, priority) |
| F10 | `GET /healthz` returns `{"status":"ok"}` for liveness/readiness probes |

### 3.2 Hybrid Multi-Cloud Orchestration

| # | Requirement |
|---|---|
| F11 | GKE management cluster acts as central control plane; dispatches to AWS EKS and GCP GKE data planes |
| F12 | SparkApplication CRDs are submitted to EKS `data-workloads` namespace via the Spark Operator |
| F13 | Kueue manages job queuing across both clouds with two local queues: `batch-data-queue` and `interactive-data-queue` |
| F14 | OPA Gatekeeper admission webhook runs on both EKS and GKE clusters |

### 3.3 Data Residency & Compliance

| # | Requirement |
|---|---|
| F15 | OPA ConstraintTemplate `DataResidency` enforces two rules at K8s admission time |
| F16 | Rule 1 — Region affinity: `outputPath` must contain the declared execution region token |
| F17 | Rule 2 — Cloud-storage affinity: AWS jobs must write to `s3://` paths; GCP jobs must write to `gs://` paths |
| F18 | Violations result in hard admission deny (`enforcementAction: deny`) |
| F19 | OPA constraint is scoped to `data-workloads` namespace on both EKS and GKE data planes |

---

## 4. Non-Functional Requirements

### 4.1 Cost Optimization

| # | Requirement |
|---|---|
| NF1 | Batch workloads run on spot/preemptible instances — significantly cheaper than on-demand, with automatic fallback to alternate instance types if capacity is unavailable |
| NF2 | Compute clusters support scale-to-zero — no nodes running (and no node cost) when no jobs are queued |
| NF3 | Cluster scales up automatically when new jobs arrive, and scales back down after a period of inactivity |
| NF4 | Interactive and batch workloads are isolated into separate priority queues to prevent resource contention |

### 4.2 Priority Scheduling & Preemption

| # | Requirement |
|---|---|
| NF5 | Interactive jobs are guaranteed fast start — they preempt lower-priority batch jobs when resources are fully utilised |
| NF6 | Preemption is automated and policy-driven, requiring no manual intervention |
| NF7 | Preemption events are auditable — observable via cluster event logs |

> **Note for batch submitters:** When a batch job is preempted it must be resubmitted — no auto-retry. See `docs/DESIGN.md §2.8` for detection and resubmission steps, and `docs/CONTRIBUTIONS.md §4.2` for automated notification options.

### 4.3 High Availability

| # | Requirement |
|---|---|
| NF8 | The control plane API is highly available — multiple instances run concurrently, spread across separate compute nodes so a single node failure does not cause downtime |
| NF9 | Rolling updates and node maintenance do not interrupt service |

### 4.4 Security & Credential Management

| # | Requirement |
|---|---|
| NF10 | Improved security via short-lived, automatically-rotated credentials — no long-lived keys, tokens, or secrets are stored in workload pod specs, environment variables, or container images |
| NF11 | Each cloud provider component uses its native identity mechanism (AWS IRSA, GCP Workload Identity) — credentials are never copied or distributed manually |
| NF12 | Output storage buckets are private by default — no public access, data encrypted at rest |
| NF13 | Worker pods write output via time-limited signed URLs — they never hold permanent storage credentials |

### 4.5 Observability

| # | Requirement |
|---|---|
| NF14 | All API activity produces structured, machine-readable logs including job metadata, enabling audit trails and downstream log aggregation |
| NF15 | Queue depth, node counts, and workload admission status are capturable as point-in-time snapshots for capacity planning and incident review |
| NF16 | Kueue exposes scheduler metrics (pending workloads, admitted workloads, preemption counts, queue depth per LocalQueue) via a Prometheus-compatible `/metrics` endpoint; these metrics must be scrapeable and storable for trend analysis |
| NF17 | Cluster Autoscaler scale-up and scale-down events must be observable via cluster event logs, enabling confirmation that nodes were added/removed in response to workload demand |

**Captured metrics and logs** — representative snapshots from the running system are in `test-result-snapshots/` (Kueue queue depth and admission metrics) and `test-result-snapshots/autoscaling-preemption-test-evidence.txt` (CA scale events). See `docs/DESIGN.md §2.5` for where metrics are stored and how to access them in Prometheus/Cloud Logging.

---

## 5. Out of Scope

- EKS as control plane (GKE is always the hub)
- Multi-region replication or cross-region data movement
- Azure integration (future enhancement)
- Trino, Jupyter, Druid end-to-end job execution — these are cluster-shaped engines (long-lived, persistent infrastructure) outside Kueue's job-shaped scheduling model; not in scope for this system
- Authentication/authorization on the job submission API itself — the API is a ClusterIP service, reachable only via `kubectl port-forward` or from inside the cluster. No API key or OAuth gate. Production path: ingress with mTLS or API gateway. See `docs/TROUBLESHOOTING.md` ("API not reachable from outside the cluster") for access steps and expected output.

---

## 6. Supported Environments

| Environment | Cluster | Region | Nodes |
|---|---|---|---|
| GKE control plane | `cross-cloud-control-plane` | `us-west2-a` (GCP) | 2× e2-standard-2 |
| EKS data plane | `cross-cloud-data-plane` | `us-west-1` (AWS) | 2× t3/t3a.xlarge spot |
| S3 output bucket | `cross-cloud-data-platform-controller-us-west-1` | `us-west-1` | — |
| GCS output bucket | `cross-cloud-data-platform-controller-us-west2` | `us-west2` | — |
| Local development | binary + `.env` | — | single process |
