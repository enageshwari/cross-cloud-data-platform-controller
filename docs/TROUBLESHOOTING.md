# Troubleshooting Guide
## Cross-Cloud Data Platform Controller

---

## Spark Operator — namespace and scheduler configuration

### Problem: Spark job submitted but no pods appear

**Symptom:** `kubectl get sparkapplication -n data-workloads` shows the CRD with blank STATUS and no driver/executor pods.

**Root cause:** Spark Operator v2.5.2 Helm chart defaults `--namespaces=default`. Jobs submitted to `data-workloads` are silently ignored — the operator never picks them up.

**Fix:** Install (or reinstall) with the namespace set explicitly. The correct Helm value is `jobNamespaces`, not `controller.namespaces`:

```bash
# Verify what namespace the operator is currently watching
kubectl describe deployment spark-operator-controller -n spark-operator | grep "\-\-namespaces"
# Should show: --namespaces=data-workloads

# If it shows "default" or nothing, reinstall
helm uninstall spark-operator -n spark-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace \
  --set webhook.enable=true \
  --set batchScheduler.enable=false \
  --set "jobNamespaces[0]=data-workloads" \
  --wait
```

### Problem: All jobs stay in PENDING / SUBMITTED indefinitely

**Root cause:** `batchScheduler.enable=true` (the Helm default) causes Spark Operator to treat every SparkApplication as suspended until an external batch scheduler unsuspends it. If no external scheduler is configured, jobs never start.

**Fix:** Disable until proper Kueue `externalFrameworks` integration is configured:

```bash
helm upgrade spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --set batchScheduler.enable=false \
  --reuse-values
```

### Problem: Stale Kueue API services block admission

**Symptom:** Spark jobs admitted but immediately fail with API discovery errors.

**Fix:**
```bash
kubectl delete apiservice v1alpha1.visibility.kueue.x-k8s.io \
  v1beta1.visibility.kueue.x-k8s.io 2>/dev/null
```

### SparkApplication name derivation

Names are constructed as `<job_name>-<14-char-timestamp-suffix>` (stripping the `job-` prefix from the job ID). This keeps names under the 63-character Kubernetes label limit while remaining unique per submission:

```go
appName := spec.JobName + "-" + spec.JobID[4:18]
```

---

## Kueue controller ImagePullBackOff

**Symptom:** `kueue-controller-manager` pod stuck in ImagePullBackOff.

**Root cause:** `gcr.io/kubebuilder/kube-rbac-proxy:v0.8.0` was removed from GCR.

**Fix:**
```bash
kubectl set image deployment/kueue-controller-manager \
  kube-rbac-proxy=quay.io/brancz/kube-rbac-proxy:v0.18.0 -n kueue-system
kubectl rollout status deployment/kueue-controller-manager -n kueue-system
```

---

## GCS signed URL returns 401 CREDENTIALS_MISSING

**Symptom:** API returns 500 with `IAM SignBlob failed: CREDENTIALS_MISSING`.

**Root cause:** ADC not set up locally, or Workload Identity binding missing.

```bash
# Local fix: run browser-based login
gcloud auth application-default login

# Verify ADC was created
ls ~/.config/gcloud/application_default_credentials.json

# Verify SA has token creator permission on gcs-presigner SA
gcloud iam service-accounts get-iam-policy \
  gcs-presigner@project-965bb0cf-caa0-458d-ba9.iam.gserviceaccount.com
# Should show user:tech.nageshwari@gmail.com with roles/iam.serviceAccountTokenCreator
```

---

## OPA blocks job with residency violation

**Symptom:** SparkApplication rejected: `"data residency violation: output path X does not contain region Y"`.

**Fix:** Ensure `output_path` contains the region token:
- AWS `us-west-1` → `s3://cross-cloud-data-platform-controller-us-west-1/...`
- GCP `us-west2` → `gs://cross-cloud-data-platform-controller-us-west2/...`

---

## ClusterQueue admission fails — borrowingLimit error

**Symptom:** `kubectl apply -f scheduler/kueue/cluster-queue.yaml` returns:
`ClusterQueue is invalid: borrowingLimit must be nil when cohort is empty`

**Fix:** Remove `borrowingLimit` from resource flavors unless a cohort is defined. Already fixed in `scheduler/kueue/cluster-queue.yaml` as of commit `3d0e700`.

---

## API not reachable from outside the cluster

**Symptom:** Any `curl` attempt to the API from a machine that doesn't have an active `port-forward` session returns:

```
curl: (7) Failed to connect to localhost port 8080 after 0 ms: Connection refused
```

Or, if targeting the pod IP directly from outside the cluster:

```
curl: (28) Failed to connect to <pod-ip> port 8080 after 21038 ms: Operation timed out
```

**Root cause:** The control API is exposed as a `ClusterIP` service — it has no external load balancer or ingress. It is intentionally reachable only from inside the cluster network (or via an authenticated `kubectl port-forward` tunnel). This is the current security boundary in place of an API gateway.

**How to access the API (required step every session):**

```bash
# Terminal 1 — open the tunnel (keep this running)
kubectl port-forward svc/control-plane-api-svc 9090:80 -n data-workloads

# Terminal 2 — verify the tunnel is alive
curl http://localhost:9090/healthz
# Expected: {"status":"ok"}

# Submit a job through the tunnel
curl -s -X POST http://localhost:9090/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "residency-test",
    "engine":       "spark",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "artifact_uri": "s3://cross-cloud-data-platform-controller-us-west-1/jobs/spark-pi.jar",
    "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority":     "batch-low"
  }' | jq .
```

**Expected successful response:**
```json
{
  "job_id": "job-1788373326010880333",
  "status": "accepted",
  "message": "Job queued for scheduling. OPA admission and Kueue placement will proceed asynchronously.",
  "output_credential": {
    "type": "presigned_put_url",
    "url": "https://cross-cloud-data-platform-controller-us-west-1.s3.us-west-1.amazonaws.com/...",
    "expires_at": "2026-09-02T19:13:11Z"
  },
  "spark_application": "residency-test-17883733260108"
}
```

**If you get `Connection refused` and the port-forward is running:** check the namespace — the service is in `data-workloads`, not `default`.

**Production hardening note:** for production use, front the ClusterIP service with an ingress controller and mTLS, or an API gateway (e.g. Kong, Apigee) with OAuth2/JWT validation. The port-forward pattern is intentional for this showcase to keep the attack surface minimal.

---

## "Too Many Requests" / throttling

**Prevention strategy:**
- Build binary once (`go build -o /tmp/control-api`), never use `go run` for repeated restarts
- Batch all test requests into a single script — never send one-by-one
- Add `sleep 2` between consecutive GCP/AWS API calls
- If throttled: `sleep 30` then retry once — never loop immediately
- Run `go mod download` once; subsequent builds use local cache (no network)

---

## EKS node group CREATE_FAILED (quota exceeded)

**Symptom:** `GCE_QUOTA_EXCEEDED: SSD_TOTAL_GB` or similar quota error during node group creation.

**Fix:** Reduce disk size. The EKS config uses `disk-size: 30` (30 GB × 2 nodes = 60 GB — well within 250 GB quota in `us-west-1`).

---

## API accepts jobs (202) but `spark_application` field is missing / jobs never appear on cluster

**Symptom:** `POST /api/v1/jobs` returns 202 with a presigned URL but no `spark_application` field, or the field is present but `kubectl get sparkapplication -n data-workloads` shows nothing.

**Root cause:** The `cross-cloud-kubeconfig` Secret contains static ServiceAccount tokens that expire. EKS caps token TTL at 24 hours. When the token expires the submitter silently initialises to `nil` on pod restart (logs `WARN: AWS/GCP Spark submitter not available`), or the dispatch call returns a 403 Forbidden from the K8s API server.

**Check pod startup logs for the warning:**
```bash
kubectl logs -n data-workloads -l app=control-plane-api | grep -E "submitter|WARN"
# If expired: "WARN ... submitter not available"
# If healthy:  "INFO ... AWS Spark submitter ready"
#              "INFO ... GCP Spark submitter ready"
```

**Fix — regenerate the kubeconfig Secret:**
```bash
# Run the kubeconfig generation script
python3 /tmp/gen_kubeconfig.py   # or recreate manually with fresh tokens

# Replace the secret
kubectl delete secret cross-cloud-kubeconfig -n data-workloads
kubectl create secret generic cross-cloud-kubeconfig \
  --from-file=kubeconfig=/tmp/static-kubeconfig.yaml \
  -n data-workloads

# Restart pods to pick up new secret
kubectl rollout restart deployment/control-plane-api -n data-workloads
kubectl rollout status deployment/control-plane-api -n data-workloads

# Verify all submitters ready
kubectl logs -n data-workloads -l app=control-plane-api | grep "submitter ready"
```

**Why this happens:** The current cross-cluster dispatch mechanism uses static SA tokens — an operational tradeoff acceptable for a showcase system. The production fix is Workload Identity Federation (no token, no expiry). See `docs/CONTRIBUTIONS.md §4.7` for the options.
