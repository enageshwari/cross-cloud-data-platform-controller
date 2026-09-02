#!/bin/zsh
# demo-run.sh — Cross-Cloud Data Platform Controller
#
# Runs all demo test cases in sequence with clear section headers.
# Each case pauses for confirmation before proceeding.
#
# Prerequisites:
#   1. Port-forward must be active:
#      kubectl port-forward svc/control-plane-api-svc 9090:80 -n data-workloads &
#   2. Both clusters must have nodes up (EKS desiredSize >= 1)
#
# Usage:
#   cd ~/nagelan/cross-cloud-data-platform-controller
#   ./scripts/demo-run.sh

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
API="${1:-http://localhost:9090}"
EKS_CTX="arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"
GKE_CTX="gke_project-965bb0cf-caa0-458d-ba9_us-west2-a_cross-cloud-control-plane"
NS="data-workloads"

BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

header() { echo "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  $1\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"; }
ok()     { echo "${GREEN}✓ $1${NC}"; }
info()   { echo "${YELLOW}▶ $1${NC}"; }
pause()  { echo "\n${BOLD}Press Enter to continue...${NC}"; read; }

# ── Preflight ──────────────────────────────────────────────────────────────
header "PREFLIGHT CHECKS"

info "Checking API health..."
HEALTH=$(curl -s $API/healthz 2>/dev/null || echo "FAIL")
if [[ "$HEALTH" != '{"status":"ok"}' ]]; then
  echo "${RED}✗ API not reachable at $API${NC}"
  echo "  Run: kubectl port-forward svc/control-plane-api-svc 9090:80 -n $NS &"
  exit 1
fi
ok "API healthy: $HEALTH"

info "Checking API startup logs (all 6 components)..."
kubectl logs -n $NS -l app=control-plane-api --context $GKE_CTX 2>/dev/null \
  | grep -E "submitter ready|presigner ready|API starting" | tail -7 || true

pause

# ── Case 1: AWS Spark ──────────────────────────────────────────────────────
header "CASE 1: AWS Spark — SparkApplication → EKS → S3"
info "Submitting Spark job to AWS..."

RESP=$(curl -s -X POST $API/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "demo-spark-aws",
    "engine":       "spark",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "main_class":   "org.apache.spark.examples.SparkPi",
    "artifact_uri": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority":     "batch-low"
  }')
echo "$RESP" | python3 -m json.tool
SPARK_APP=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('spark_application',''))")
ok "spark_application: $SPARK_APP"

info "Watching SparkApplication on EKS (Ctrl-C to stop)..."
kubectl get sparkapplication $SPARK_APP -n $NS --context $EKS_CTX -w &
WATCH_PID=$!
sleep 30
kill $WATCH_PID 2>/dev/null

info "Final status:"
kubectl get sparkapplication $SPARK_APP -n $NS --context $EKS_CTX 2>/dev/null || echo "(may have completed)"
pause

# ── Case 2: AWS Flink ──────────────────────────────────────────────────────
header "CASE 2: AWS Flink — AppWrapper → EKS → S3"
info "Submitting Flink job to AWS..."

RESP=$(curl -s -X POST $API/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "demo-flink-aws",
    "engine":       "flink",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "artifact_uri": "local:///opt/flink/examples/streaming/WordCount.jar",
    "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority":     "batch-low"
  }')
echo "$RESP" | python3 -m json.tool
AW=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('spark_application',''))")
ok "appwrapper: $AW"

info "Watching AppWrapper + FlinkDeployment on EKS..."
sleep 15
kubectl get appwrapper $AW -n $NS --context $EKS_CTX 2>/dev/null | cat
kubectl get flinkdeployment $AW -n $NS --context $EKS_CTX 2>/dev/null | cat || true
kubectl get pods -n $NS --context $EKS_CTX 2>/dev/null | grep $AW || true
pause

# ── Case 3: GCP Spark ──────────────────────────────────────────────────────
header "CASE 3: GCP Spark — SparkApplication → GKE → GCS"
info "Submitting Spark job to GCP..."

RESP=$(curl -s -X POST $API/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "demo-spark-gcp",
    "engine":       "spark",
    "target_cloud": "gcp",
    "region":       "us-west2",
    "main_class":   "org.apache.spark.examples.SparkPi",
    "artifact_uri": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    "input_path":   "gs://cross-cloud-data-platform-controller-us-west2/input/",
    "output_path":  "gs://cross-cloud-data-platform-controller-us-west2/output/",
    "priority":     "batch-low"
  }')
echo "$RESP" | python3 -m json.tool
SPARK_APP=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('spark_application',''))")
ok "spark_application: $SPARK_APP"

info "Watching SparkApplication on GKE (Ctrl-C to stop)..."
kubectl get sparkapplication $SPARK_APP -n $NS --context $GKE_CTX -w &
WATCH_PID=$!
sleep 30
kill $WATCH_PID 2>/dev/null

info "Final status:"
kubectl get sparkapplication $SPARK_APP -n $NS --context $GKE_CTX 2>/dev/null || echo "(may have completed)"
pause

# ── Case 4: GCP Flink ──────────────────────────────────────────────────────
header "CASE 4: GCP Flink — AppWrapper → GKE → GCS"
info "Submitting Flink job to GCP..."

RESP=$(curl -s -X POST $API/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "demo-flink-gcp",
    "engine":       "flink",
    "target_cloud": "gcp",
    "region":       "us-west2",
    "artifact_uri": "local:///opt/flink/examples/streaming/WordCount.jar",
    "input_path":   "gs://cross-cloud-data-platform-controller-us-west2/input/",
    "output_path":  "gs://cross-cloud-data-platform-controller-us-west2/output/",
    "priority":     "interactive-high"
  }')
echo "$RESP" | python3 -m json.tool
AW=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('spark_application',''))")
ok "appwrapper: $AW"

sleep 15
kubectl get appwrapper $AW -n $NS --context $GKE_CTX 2>/dev/null | cat
kubectl get flinkdeployment $AW -n $NS --context $GKE_CTX 2>/dev/null | cat || true
kubectl get pods -n $NS --context $GKE_CTX 2>/dev/null | grep $AW || true
pause

# ── Case 5: OPA Enforcement ────────────────────────────────────────────────
header "CASE 5: OPA Enforcement — Cross-Region Violation"
info "Submitting job with WRONG region in output path (should be DENIED by OPA)..."

curl -s -X POST $API/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "opa-violation-test",
    "engine":       "spark",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "main_class":   "org.apache.spark.examples.SparkPi",
    "artifact_uri": "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar",
    "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path":  "s3://bucket-in-us-east-1/output/",
    "priority":     "batch-low"
  }' | python3 -m json.tool

info "Verify: no SparkApplication created on EKS (OPA blocked it)"
kubectl get sparkapplication -n $NS --context $EKS_CTX 2>/dev/null | grep opa-violation || echo "(none — OPA denied admission ✓)"

info "OPA constraint status:"
kubectl get dataresidency --context $EKS_CTX 2>/dev/null | cat
pause

# ── Case 6: Preemption ─────────────────────────────────────────────────────
header "CASE 6: Priority Preemption — interactive-high preempts batch-low"
info "Submitting 3 batch-low Spark jobs to fill the queue..."

for i in 1 2 3; do
  RESP=$(curl -s -X POST $API/api/v1/jobs \
    -H "Content-Type: application/json" \
    -d "{\"job_name\":\"preempt-batch-$i\",\"engine\":\"spark\",\"target_cloud\":\"aws\",\"region\":\"us-west-1\",\"main_class\":\"org.apache.spark.examples.SparkPi\",\"artifact_uri\":\"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar\",\"input_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/input/\",\"output_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/output/\",\"priority\":\"batch-low\"}")
  echo "  batch-$i: $(echo $RESP | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('spark_application','?'))")"
done

info "Queue depth after batch submissions:"
kubectl get workloads -n $NS --context $EKS_CTX 2>/dev/null | cat

sleep 5
info "Now submitting interactive-high Flink job — should preempt a batch job..."
RESP=$(curl -s -X POST $API/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "preempt-interactive",
    "engine":       "flink",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "artifact_uri": "local:///opt/flink/examples/streaming/WordCount.jar",
    "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority":     "interactive-high"
  }')
echo "$RESP" | python3 -m json.tool

sleep 10
info "Checking for preemption events:"
kubectl get events -n $NS --context $EKS_CTX \
  --field-selector reason=Preempted 2>/dev/null | cat || echo "(no preemption events yet — may need a few seconds)"

info "Workload status after preemption:"
kubectl get workloads -n $NS --context $EKS_CTX 2>/dev/null | cat
pause

# ── Case 7: Metrics Snapshot ───────────────────────────────────────────────
header "CASE 7: Metrics Snapshot"
info "Capturing point-in-time metrics from both clusters..."
cd "$REPO_DIR" && ./scripts/metrics-snapshot.sh "demo-run"
ok "Snapshot saved to test-result-snapshots/"
ls -t "$REPO_DIR/test-result-snapshots/"*.json | head -3

echo "\n${BOLD}${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ALL DEMO CASES COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
