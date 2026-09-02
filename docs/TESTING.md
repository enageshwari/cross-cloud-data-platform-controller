# Testing Methodologies
## Cross-Cloud Data Platform Controller

---

## 1. Overview

Testing is organised by concern rather than environment. Each category has a clear scope and can be run independently.

| Category | Scope | Scripts |
|---|---|---|
| API Tests | Schema validation, credential generation, cross-cloud dispatch, logging | `scripts/e2e-job.sh`, `scripts/gcp-job-test.sh`, `scripts/custom-job-test.sh`, `scripts/flink-*.sh` |
| OPA / Policy Tests | Admission webhook enforcement — region affinity, cloud-storage affinity | `kubectl apply` with test manifests |
| Priority & Preemption | Kueue queue behaviour under load, interactive preempts batch | `scripts/load-test.sh`, `scripts/preemption-test.sh` |
| Autoscale | Cluster Autoscaler scale-up and scale-down | `scripts/metrics-snapshot.sh`, `scripts/capture-autoscale-evidence.py` |
| Non-Functional | HA, security, cost, credential hygiene | `kubectl`, `aws s3api`, `gcloud storage` |

### Script Reference

| Script | What it tests | Cloud required |
|---|---|---|
| `scripts/e2e-job.sh` | Single Spark job → AWS, verifies 202 + presigned S3 URL | Yes (AWS) |
| `scripts/gcp-job-test.sh` | Single Spark job → GCP, verifies 202 + signed GCS URL | Yes (GCP) |
| `scripts/flink-aws-test.sh` | Flink job → AWS via AppWrapper | Yes (AWS) |
| `scripts/flink-gcp-test.sh` | Flink job → GCP via AppWrapper | Yes (GCP) |
| `scripts/custom-job-test.sh` | All 4 paths (Spark+Flink × AWS+GCP) with custom JAR references | Yes (both) |
| `scripts/load-test.sh` | 3 batch + 1 interactive, polls Kueue admission, shows queue wait times | Yes (AWS EKS) |
| `scripts/preemption-test.sh` | 7 batch jobs saturate quota, interactive job triggers preemption | Yes (AWS EKS) |
| `scripts/metrics-snapshot.sh` | Point-in-time Kueue + node metrics snapshot to `test-result-snapshots/` | Yes (both) |
| `scripts/capture-autoscale-evidence.py` | Pulls CA logs, node state, preemption events, snapshot diff | Yes (both) |

All tests follow the **build-once, single-script** strategy — binary compiled once, all cases run in a single invocation to avoid throttling.

---

## 2. Local Tests (No Cloud Required)

These tests run against the local binary only. No AWS or GCP access needed — the presigners generate real URLs (requires `.env` with credentials) but CRD dispatch is a no-op when the cluster context is not reachable.

### 2.1 Setup

```bash
cd api
go build -o /tmp/control-api ./cmd/server

# Kill any existing process on 8080
lsof -ti :8080 | xargs kill -9 2>/dev/null; sleep 1

# Start with .env loaded
/tmp/control-api > /tmp/control-api.log 2>&1 &
echo $! > /tmp/control-api.pid
sleep 2

# Confirm startup
curl -s http://localhost:8080/healthz
# → {"status":"ok"}

# Confirm both presigners loaded
grep -E "presigner ready" /tmp/control-api.log
# S3 presigner ready  bucket=cross-cloud-data-platform-controller-us-west-1
# GCS presigner ready bucket=cross-cloud-data-platform-controller-us-west2
```

### 2.2 Schema Validation

| # | Test | Input | Expected |
|---|---|---|---|
| 1 | Health check | `GET /healthz` | `200 {"status":"ok"}` |
| 2 | Valid AWS Spark job | `engine=spark, cloud=aws, region=us-west-1` | `202` + presigned S3 URL + spark_application name |
| 3 | Valid GCP job | `engine=spark, cloud=gcp, region=us-west2` | `202` + GCS signed URL |
| 4 | Invalid engine | `engine=hadoop` | `422` engine not supported |
| 5 | Empty job_name | `job_name=""` | `422` job_name is required |
| 6 | Unsupported cloud | `target_cloud=azure` | `422` target_cloud not supported |
| 7 | Malformed JSON | non-JSON body | `400` parse error |

Run all cases in one script:

```bash
chmod +x /tmp/test_api.sh && /tmp/test_api.sh
```

**Verified output (2026-09-01):**

```
=== 1. Health check ===
{"status":"ok"}

=== 2. Valid AWS Spark job — expect 202 + presigned URL ===
{"job_id":"job-1788281271476238000","status":"accepted",
 "output_credential":{"type":"presigned_put_url",
   "url":"https://cross-cloud-data-platform-controller-us-west-1.s3.us-west-1.amazonaws.com/...",
   "bucket":"cross-cloud-data-platform-controller-us-west-1","region":"us-west-1",
   "expires_at":"2026-09-01T17:02:51Z"},
 "spark_application":"daily-etl-788281271476"}

=== 3. PUT via presigned URL ===
S3 PUT status: 200

=== 4. Verify object in S3 ===
{"ContentLength":38,"ServerSideEncryption":"AES256",...}

=== 5. Valid GCP job ===
{"job_id":"job-1788281272532353000","status":"accepted",
 "output_credential":{"type":"signed_put_url",
   "url":"https://storage.googleapis.com/cross-cloud-data-platform-controller-us-west2/...",
   "bucket":"cross-cloud-data-platform-controller-us-west2","region":"us-west2",
   "expires_at":"2026-09-01T17:02:53Z"}}
GCS PUT status: 200

=== 6. Verify object in GCS ===
name: cross-cloud-data-platform-controller-us-west2/jobs/job-1788281272532353000/.../output
size: 38
storageClass: STANDARD
location: US-WEST2
contentType: application/octet-stream
created: 2026-09-01T17:02:54Z

=== 7. Invalid engine — expect 422 ===
{"code":422,"message":"engine \"hadoop\" is not supported"}

=== ALL TESTS PASSED ===
```

---

## 3. Cloud Tests (AWS EKS + GCP GKE Required)

These tests require live clusters, port-forward to the deployed GKE pod, and both `EKS_CONTEXT` and `GKE_CONTEXT` configured. Run `kubectl port-forward svc/control-plane-api-svc 9090:80 -n data-workloads` before running any test in this section.

### Running all cases end-to-end

```bash
# From repo root
cd ~/nagelan/cross-cloud-data-platform-controller

# Kill any existing port-forward on 9090
lsof -ti :9090 | xargs kill -9 2>/dev/null

# Start port-forward
kubectl port-forward svc/control-plane-api-svc 9090:80 -n data-workloads &

# Run all 7 cases
./scripts/demo-run.sh

# Or with explicit URL
./scripts/demo-run.sh http://localhost:9090

# Start from a specific case (e.g. skip cases 1-2, start from GCP Spark)
./scripts/demo-run.sh http://localhost:9090 3
```

### 3.1 Cross-Cloud Dispatch — Verified (all 4 paths)

All four dispatch paths (Spark+Flink × AWS+GCP) verified from the deployed GKE pod via port-forward on 2026-09-02.

**AWS Spark — COMPLETED:**
```json
{"job_id":"job-1788373326010880333","status":"accepted",
 "output_credential":{"type":"presigned_put_url","bucket":"cross-cloud-data-platform-controller-us-west-1","region":"us-west-1"},
 "spark_application":"aws-spark-verify-17883733260108"}
```
```bash
kubectl get sparkapplication aws-spark-verify-17883733260108 -n data-workloads
# SUSPEND=false  STATUS=COMPLETED  START=18:22:06Z  FINISH=18:23:47Z
```

**AWS Flink — FINISHED/STABLE:**
```json
{"job_id":"job-1788373326971343801","status":"accepted",
 "output_credential":{"type":"presigned_put_url","bucket":"cross-cloud-data-platform-controller-us-west-1","region":"us-west-1"},
 "spark_application":"aws-flink-verify-17883733269713"}
```
```bash
kubectl get flinkdeployment aws-flink-verify-17883733269713 -n data-workloads
# JOB STATUS=FINISHED  LIFECYCLE STATE=STABLE
```

**GCP Spark — COMPLETED** (see §2.6 for full output)

**GCP Flink — FINISHED/STABLE** (see §2.6 for full output)

> **Note on RBAC:** CRD dispatch from the GKE pod requires `spark-crd-creator` ClusterRole applied on each target cluster with the SA used in the static kubeconfig. Applied to both GKE (`control-plane-sa`) and EKS (`spark-driver` SA). See `deploy/gke-control-plane/spark-crd-creator-rbac.yaml`.

To run all 4 paths with custom JARs:
```bash
./scripts/custom-job-test.sh http://localhost:9090

# Edit variables at the top for real JARs:
SPARK_AWS_JAR="s3://your-bucket/jars/my-etl-1.0.jar"
SPARK_AWS_CLASS="com.example.MyETLJob"
```

### 3.2 GKE Deployment Functional Test

Verified live on GKE cluster:

```bash
kubectl port-forward svc/control-plane-api-svc 9090:80 -n data-workloads &

curl -s -X POST http://localhost:9090/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"gke-spark-test","engine":"spark","target_cloud":"aws",
       "region":"us-west-1","artifact_uri":"apache/spark:3.5.3",
       "input_path":"s3://.../input/","output_path":"s3://.../results/",
       "priority":"batch-low"}'
# → 202 + presigned S3 URL generated from inside GKE cluster
# Pod logs confirm: "S3 presigner ready" + "SparkApplication dispatched"
```

**Pod anti-affinity verified:**

```
NAME                                 READY   STATUS    NODE
control-plane-api-86dddb9bbc-7gjsd   1/1     Running   ...hpb3  ← node 1
control-plane-api-86dddb9bbc-hb626   1/1     Running   ...nxxh  ← node 2
```

Both pods on different nodes — hard `podAntiAffinity` working as designed.

### 3.3 Structured Logging

Every request produces a single structured JSON log line. Actual output captured from the deployed API (`kubectl logs -n data-workloads -l app=control-plane-api`):

```json
{"time":"2026-09-01T09:44:07Z","level":"INFO","msg":"S3 presigner ready","bucket":"cross-cloud-data-platform-controller-us-west-1","region":"us-west-1","ttl_minutes":15}
{"time":"2026-09-01T09:44:07Z","level":"INFO","msg":"GCS presigner ready","bucket":"cross-cloud-data-platform-controller-us-west2","ttl_minutes":15}
{"time":"2026-09-01T09:44:07Z","level":"INFO","msg":"control plane API starting","port":"8080"}
{"time":"2026-09-01T16:46:53Z","level":"INFO","msg":"job accepted","job_id":"job-1788284774776896161","job_name":"gke-spark-test","engine":"spark","target_cloud":"aws","region":"us-west-1","priority":"batch-low"}
{"time":"2026-09-01T16:46:53Z","level":"INFO","msg":"S3 presigned URL generated","job_id":"job-1788284774776896161","object_key":"jobs/job-1788284774776896161/gke-spark-test/output"}
{"time":"2026-09-01T16:46:53Z","level":"INFO","msg":"SparkApplication dispatched","job_id":"job-1788284774776896161","spark_app":"gke-spark-test-788284774776"}
{"time":"2026-09-01T12:54:05Z","level":"WARN","msg":"job rejected","status":422,"reason":"engine \"hadoop\" is not supported; valid values: spark, flink"}
{"time":"2026-09-01T12:54:05Z","level":"WARN","msg":"job rejected","status":422,"reason":"target_cloud \"azure\" is not supported; valid values: aws, gcp"}
```

Valid jobs → `INFO` with full metadata. Invalid jobs → `WARN` with reason. No interpolated strings.

### 3.4 GCP Spark/Flink Dispatch — Verified Output

With `GKE_CONTEXT` and the `cross-cloud-kubeconfig` Secret mounted, GCP jobs dispatch CRDs to the GKE data plane in addition to generating the signed GCS URL.

**Startup logs confirming all 6 components ready (captured 2026-09-02):**
```json
{"level":"INFO","msg":"S3 presigner ready","bucket":"cross-cloud-data-platform-controller-us-west-1","region":"us-west-1","ttl_minutes":15}
{"level":"INFO","msg":"GCS presigner ready","bucket":"cross-cloud-data-platform-controller-us-west2","ttl_minutes":15}
{"level":"INFO","msg":"AWS Spark submitter ready","context":"arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"}
{"level":"INFO","msg":"GCP Spark submitter ready","context":"gke_project-965bb0cf-caa0-458d-ba9_us-west2-a_cross-cloud-control-plane"}
{"level":"INFO","msg":"AWS Flink submitter ready","context":"arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"}
{"level":"INFO","msg":"GCP Flink submitter ready","context":"gke_project-965bb0cf-caa0-458d-ba9_us-west2-a_cross-cloud-control-plane"}
{"level":"INFO","msg":"control plane API starting","port":"8080"}
```

**GCP Spark job — verified 202 response (2026-09-02T18:05Z):**
```bash
curl -s -X POST http://localhost:9090/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"gcp-spark-test","engine":"spark","target_cloud":"gcp",
       "region":"us-west2","main_class":"org.apache.spark.examples.SparkPi",
       "artifact_uri":"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
       "input_path":"gs://cross-cloud-data-platform-controller-us-west2/input/",
       "output_path":"gs://cross-cloud-data-platform-controller-us-west2/output/",
       "priority":"batch-low"}' | python3 -m json.tool
```

```json
{
    "job_id": "job-1788372357346625569",
    "status": "accepted",
    "message": "Job queued for scheduling. OPA admission and Kueue placement will proceed asynchronously.",
    "output_credential": {
        "type": "signed_put_url",
        "url": "https://storage.googleapis.com/cross-cloud-data-platform-controller-us-west2/...",
        "bucket": "cross-cloud-data-platform-controller-us-west2",
        "region": "us-west2",
        "expires_at": "2026-09-02T18:20:57Z"
    },
    "spark_application": "gcp-spark-test-17883723573466"
}
```

**SparkApplication admitted and completed on GKE (verified):**
```bash
kubectl get sparkapplication gcp-spark-test-17883723573466 -n data-workloads
# NAME                            SUSPEND   STATUS      ATTEMPTS   START                  FINISH                 AGE
# gcp-spark-test-17883723573466   false     COMPLETED   1          2026-09-02T18:05:57Z   2026-09-02T18:07:13Z   101s
```

Kueue admitted the workload via `multi-cloud-cluster-queue / batch-data-queue`. Driver pod ran and completed in 76 seconds. ✓

**GCP Flink job — verified (2026-09-02T18:11Z):**
```bash
curl -s -X POST http://localhost:9090/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"gcp-flink-test","engine":"flink","target_cloud":"gcp",
       "region":"us-west2",
       "artifact_uri":"local:///opt/flink/examples/streaming/WordCount.jar",
       "input_path":"gs://cross-cloud-data-platform-controller-us-west2/input/",
       "output_path":"gs://cross-cloud-data-platform-controller-us-west2/output/",
       "priority":"interactive-high"}' | python3 -m json.tool
```

```json
{
    "job_id": "job-1788372676721446838",
    "status": "accepted",
    "output_credential": {"type": "signed_put_url", "bucket": "cross-cloud-data-platform-controller-us-west2", "region": "us-west2", ...},
    "spark_application": "gcp-flink-test-17883726767214"
}
```

**AppWrapper admitted via interactive-data-queue, FlinkDeployment completed (verified):**
```bash
kubectl get appwrapper gcp-flink-test-17883726767214 -n data-workloads
# NAME                            STATUS    QUOTA RESERVED   RESOURCES DEPLOYED   UNHEALTHY
# gcp-flink-test-17883726767214   Running   True             True                 False

kubectl get flinkdeployment gcp-flink-test-17883726767214 -n data-workloads
# NAME                            JOB STATUS   LIFECYCLE STATE
# gcp-flink-test-17883726767214   FINISHED     STABLE
```

JobManager + TaskManager pods ran on GKE. AppWrapper admitted via `interactive-data-queue` (priority 1000). FlinkDeployment `JOB STATUS: FINISHED`. ✓

### 3.5 GCP Submitter Nil-Path Behaviour

When `GKE_CONTEXT` is not set (e.g. deployment without the env var), the GCP submitter initialises to `nil` and the API degrades gracefully:

```bash
# Submit a GCP Spark job with GKE_CONTEXT unset
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"no-gke-test","engine":"spark","target_cloud":"gcp",
       "region":"us-west2","main_class":"org.apache.spark.examples.SparkPi",
       "artifact_uri":"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
       "input_path":"gs://.../input/","output_path":"gs://.../output/",
       "priority":"batch-low"}' | jq .
```

**Expected response — presigned URL generated, no CRD dispatch:**
```json
{
  "job_id": "job-...",
  "status": "accepted",
  "message": "Job queued for scheduling. OPA admission and Kueue placement will proceed asynchronously.",
  "output_credential": {
    "type": "signed_put_url",
    "url": "https://storage.googleapis.com/...",
    "expires_at": "..."
  }
}
```

Note: `spark_application` field is absent — no CRD was submitted. Pod log shows:
```json
{"level":"WARN","msg":"GCP Spark submitter not available — GCP Spark jobs won't be dispatched to GKE"}
```

This is expected degraded behaviour — GCS credential generation still works, CRD dispatch requires `GKE_CONTEXT` to be set.

---

## 4. OPA / Policy Tests

These tests verify that regional data residency is enforced **automatically at admission time**. The `kubectl apply` commands here are the *test tool* — they intentionally bypass the Go API to prove OPA is the authoritative enforcement point, not just a secondary check backed by the API.

### 4.1 Setup

Verify the constraint is active before running tests:

```bash
kubectl get constrainttemplate dataresidency
# NAME            AGE
# dataresidency   ...

kubectl get dataresidency
# NAME                              ENFORCEMENT-ACTION   TOTAL-VIOLATIONS
# enforce-regional-data-residency   deny
```

### 4.2 TC-OPA-1: Cross-region output path — DENIED

**Input:** `region: us-west-1`, `outputPath` contains `us-east-1`

**Expected:**
```
Error from server (Forbidden): admission webhook "validation.gatekeeper.sh" denied the request:
[enforce-regional-data-residency] Data residency violation: output path
s3://.../us-east-1/... does not contain target region us-west-1
```

**Verified:** ✓ Workload never reaches scheduler or nodes.

### 4.3 TC-OPA-2: GCP job writing to S3 — DENIED

**Input:** `targetCloud: gcp`, `outputPath: s3://...`

**Expected:**
```
[enforce-regional-data-residency] Cloud-storage affinity violation:
GCP jobs must write to gs:// paths, got: s3://...
```

**Verified:** ✓ Cross-cloud output path blocked at admission.

### 4.4 TC-OPA-3: AWS job writing to GCS — DENIED

**Input:** `targetCloud: aws`, `outputPath: gs://...`

**Expected:**
```
[enforce-regional-data-residency] Cloud-storage affinity violation:
AWS jobs must write to s3:// paths, got: gs://...
```

**Verified:** ✓ Blocked.

### 4.5 TC-OPA-4: AWS compliant job — ADMITTED

```bash
kubectl apply -f deploy/aws-eks/spark-test-job.yaml
# sparkapplication.sparkoperator.k8s.io/spark-pi-test created
```

**Verified:** ✓ No webhook error — `outputPath` contains `us-west-1` and uses `s3://`.

### 4.6 TC-OPA-6: GCP compliant job — ADMITTED

```bash
kubectl apply -f - <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: opa-test-gcp-compliant
  namespace: data-workloads
spec:
  region: us-west2
  targetCloud: gcp
  outputPath: gs://cross-cloud-data-platform-controller-us-west2/output/
  type: Scala
  mode: cluster
  image: apache/spark:3.5.3
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar
EOF
# sparkapplication.sparkoperator.k8s.io/opa-test-gcp-compliant created
```

**Verified:** ✓ No webhook error — `outputPath` contains `us-west2` and uses `gs://`. Confirmed via GCP Spark end-to-end test (2026-09-02): `gcp-spark-test-17883723573466` was admitted by OPA and completed successfully.

### 4.7 TC-OPA-5: API bypass attempt — OPA backstop holds

The API validates schema but OPA is the hard gate. A job with a valid API payload but mismatched `output_path` is still blocked at the K8s layer:

```bash
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"bypass-test","engine":"spark","target_cloud":"aws",
       "region":"us-west-1","output_path":"s3://wrong-region-us-east-1/output/",
       "priority":"batch-low"}'
# API returns 202 (presigned URL generated)
# BUT SparkApplication CRD is denied by OPA admission webhook
# kubectl get sparkapplication -n data-workloads → no resource created
```

This demonstrates defence-in-depth: the pre-signed URL is issued (worker credential is valid), but the compute job is blocked from running in the wrong region.

### 4.8 Enforcement Summary

| Test | Violation | OPA action | Result |
|---|---|---|---|
| TC-OPA-1 | Output region ≠ declared region | `deny` | ✓ Blocked |
| TC-OPA-2 | GCP job → S3 path | `deny` | ✓ Blocked |
| TC-OPA-3 | AWS job → GCS path | `deny` | ✓ Blocked |
| TC-OPA-4 | AWS compliant job | `allow` | ✓ Admitted |
| TC-OPA-5 | API bypass via `kubectl apply` | `deny` at K8s layer | ✓ CRD never created |
| TC-OPA-6 | GCP compliant job | `allow` | ✓ Admitted |

Enforcement covers both `SparkApplication` and `AppWrapper` (Flink) CRDs in the `data-workloads` namespace and cannot be bypassed by any submission path.

---

## 5. Priority & Preemption Tests

### 5.1 Script

```bash
# Requires: API running (port-forward or local), kubectl configured for EKS
./scripts/load-test.sh http://localhost:8080
```

**What it does:**
1. Submits 3 `batch-low` Spark jobs concurrently — fills Kueue batch queue
2. Waits 10 s, submits 1 `interactive-high` Flink job
3. Polls Kueue Workload admission every 5 s for up to 5 min
4. Records submit time and admission time per workload
5. Prints summary table with queue wait per job

**Expected output:**

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
JOB NAME                  PRIORITY      WAIT(s)    STATUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
load-test-batch-1         batch-low     12         ADMITTED
load-test-batch-2         batch-low     12         ADMITTED
load-test-batch-3         batch-low     12         ADMITTED
load-test-interactive-1   interactive   4          ADMITTED  ← preempted batch
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### 5.2 Verified Preemption Event

**Captured from live run, 2026-09-01T23:46 UTC:**

```
LAST SEEN   TYPE     REASON     OBJECT                          MESSAGE
6m51s       Normal   Preempted  workload/sparkapplication-...   Preempted to accommodate
            a workload due to reclamation within the cohort;
            preemptor effective priority: 1000;
            preemptee effective priority: 100
```

Interactive job (priority 1000) preempted batch job (priority 100) within seconds via Kueue cohort reclamation. Full event log in `test-result-snapshots/autoscaling-preemption-test-evidence.txt`.

---

## 6. Autoscale Tests

### 6.1 Scripts

```bash
# Point-in-time metrics snapshot
./scripts/metrics-snapshot.sh

# Full autoscale evidence capture
python3 /tmp/capture_autoscale_evidence.py
```

### 6.2 Scale-Up Evidence

**Captured from live run, 2026-09-01:**

```
── EKS SCALE-UP ──
Unschedulable pods detected → CA triggered
Node ip-192-168-24-6 joined at 23:41:03 UTC

── SNAPSHOT DIFF: before-load-test vs after-preemption-test ──
  EKS nodes:      1 → 2   (autoscale-up triggered)
  EKS workloads:  0 → 8   (8 jobs admitted including 1 interactive)
  GKE nodes:      2 → 2   (unchanged — control plane stable)
```

### 6.3 Scale-Down Evidence

Node `ip-192-168-28-115` tracked as unneeded and removed after 60 s idle:

```
23:33:31 → unneeded (0s)
23:33:41 → unneeded (10s)
...
23:34:32 → unneeded (1m0s) → SCALE DOWN
```

Full logs in `test-result-snapshots/autoscaling-preemption-test-evidence.txt`.

### 6.4 Metrics Snapshots

| File | When captured | EKS nodes | EKS workloads |
|---|---|---|---|
| `20260901T233535Z-baseline-.json` | Before any jobs | 1 | 0 |
| `20260901T234016Z-before-load-test.json` | Before load test | 1 | 0 |
| `20260901T234109Z-after-load-test.json` | After batch jobs submitted | 2 | 4 |
| `20260901T234514Z-after-preemption-test.json` | After interactive job + preemption | 2 | 8 |
| `20260901T235126Z-final-settled-state.json` | After scale-down | 2 | 0 |

```bash
# Capture a fresh snapshot
./scripts/metrics-snapshot.sh
# Output → test-result-snapshots/$(date -u +%Y%m%dT%H%M%SZ)-<label>.json
```

---

## 7. Non-Functional Test Summary

| Requirement | Test method | Result |
|---|---|---|
| HA — 2 replicas on different nodes | `kubectl get pods -o wide` | ✓ pods on hpb3 and nxxh |
| PDB prevents downtime | `kubectl get pdb -n data-workloads` | ✓ minAvailable=1, allowedDisruptions=1 |
| S3 output encrypted | `aws s3api head-object` | ✓ ServerSideEncryption=AES256 |
| GCS output public-access blocked | `gcloud storage buckets describe` | ✓ publicAccessPrevention=enforced |
| No credentials in pod spec | `kubectl get secret` / pod env inspection | ✓ Secret reference only, no literal values |
| OPA blocks cross-region output | TC-OPA-1 | ✓ admission webhook denies |
| OPA blocks cross-cloud storage | TC-OPA-2, TC-OPA-3 | ✓ admission webhook denies |
| Kueue preempts batch for interactive | `scripts/load-test.sh` + events | ✓ priority 1000 preempts 100 |
| EKS scale-up on unschedulable pods | `scripts/metrics-snapshot.sh` diff | ✓ 1→2 nodes at 23:41 UTC |
| EKS scale-down when idle | CA logs + node list | ✓ node removed after 60s idle |
| Spot nodes across two AZs | `kubectl get nodes` labels | ✓ us-west-1a + us-west-1c |
| Pre-signed URL works end-to-end | `scripts/custom-job-test.sh` | ✓ S3 PUT 200, GCS PUT 200 |
| SparkApplication dispatched to EKS | Job response `spark_application` field | ✓ CRD created in data-workloads |
| AWS Spark end-to-end from GKE pod | `aws-spark-verify-17883733260108` | ✓ COMPLETED 2026-09-02T18:23Z |
| AWS Flink end-to-end from GKE pod | `aws-flink-verify-17883733269713` | ✓ FINISHED/STABLE 2026-09-02T18:24Z |
| GCP Spark end-to-end from GKE pod | `gcp-spark-test-17883723573466` | ✓ COMPLETED 2026-09-02T18:07Z |
| GCP Flink end-to-end from GKE pod | `gcp-flink-test-17883726767214` | ✓ FINISHED/STABLE 2026-09-02T18:14Z |
| Reproducible builds | `git log` + `go mod verify` | ✓ go.sum committed |

---

## 8. Screenshots / External Evidence

Screenshots from GCP and AWS consoles provide independent proof beyond what code and logs show.

### 8.1 GCP Console

| What to capture | Console path | Why it matters |
|---|---|---|
| Cloud Build history | Cloud Build → History | Shows container image build (~3m44s), no local Docker needed |
| API deployment | Kubernetes Engine → Workloads → control-plane-api | 2/2 pods Running, liveness probe green |
| Pod spread across nodes | Workloads → control-plane-api → Managed pods | pod-1 on `hpb3`, pod-2 on `nxxh` — anti-affinity working |
| GCS bucket | Cloud Storage → Buckets | Objects written by signed URLs, public access blocked |
| GCS object metadata | Click any object → Details | Content-type, size, creation time — signed PUT worked end-to-end |
| Service account | IAM & Admin → Service Accounts → gcs-presigner | TokenCreator binding, no key files — keyless signing |
| Artifact Registry | Artifact Registry → cross-cloud-api | Image digest, size, push timestamp |

### 8.2 AWS Console

| What to capture | Console path | Why it matters |
|---|---|---|
| EKS cluster overview | EKS → Clusters → cross-cloud-data-plane | OIDC provider enabled, status ACTIVE |
| Spot nodes across AZs | EKS → Compute → spot-data-workers | One node in us-west-1a, one in us-west-1c — AZ spread |
| Node capacity type | EC2 → Instances (filter by cluster tag) | Lifecycle: Spot — cost optimization proof |
| S3 bucket | S3 → cross-cloud-data-platform-controller-us-west-1 | Output objects, AES256 column, 0 public access |
| S3 object properties | Click any object → Properties | SSE-S3 (AES256), size, last modified |
| IAM user policy | IAM → Users → cross-cloud-presigner → Permissions | Scoped PutObject/GetObject on one bucket — least privilege |
| IRSA role trust | IAM → Roles → eksctl-...-Role1 | OIDC trust policy — pod identity without keys |

### 8.3 Key Terminal Captures

```bash
# Pods on different nodes (anti-affinity proof)
kubectl get pods -n data-workloads -o wide

# Spot node labels
kubectl get nodes --show-labels | grep capacityType
# eks.amazonaws.com/capacityType=SPOT

# Nodes in different AZs
kubectl get nodes -L topology.kubernetes.io/zone

# Preemption event
kubectl get events -n data-workloads --field-selector reason=Preempted

# Queue state after load test
kubectl get clusterqueue,localqueue -n data-workloads

# OPA constraint active
kubectl get dataresidency

# Full job response (GKE → EKS dispatch)
curl -s http://localhost:9090/api/v1/jobs ... | python3 -m json.tool
# "spark_application": "gke-spark-test-788284774776"
```

---

## Appendix A: Direct CRD Submission

The Go API is the intended entry point for job submission. The examples below show that OPA enforcement holds even when jobs are submitted directly via `kubectl apply`, without going through the API. This is the mechanism tested in TC-OPA-1 through TC-OPA-6.

### A.1 TC-OPA-1 — Cross-region output path

```bash
kubectl apply -f - <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: opa-test-wrong-region
  namespace: data-workloads
spec:
  region: us-west-1
  targetCloud: aws
  outputPath: s3://cross-cloud-data-platform-controller-us-east-1/output/
  type: Scala
  mode: cluster
  image: apache/spark:3.5.3
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar
EOF
# Error from server (Forbidden): admission webhook denied:
# [enforce-regional-data-residency] output path does not contain region us-west-1
```

### A.2 TC-OPA-2 — GCP job writing to S3

```bash
kubectl apply -f - <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: opa-test-wrong-storage
  namespace: data-workloads
spec:
  region: us-west2
  targetCloud: gcp
  outputPath: s3://cross-cloud-data-platform-controller-us-west-1/output/
  type: Scala
  mode: cluster
  image: apache/spark:3.5.3
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar
EOF
# Error: GCP jobs must write to gs:// paths, got: s3://...
```

### A.3 TC-OPA-4 — Compliant job admitted

```bash
kubectl apply -f deploy/aws-eks/spark-test-job.yaml
# sparkapplication.sparkoperator.k8s.io/spark-pi-test created
# No webhook error — outputPath contains us-west-1 and uses s3://
```
