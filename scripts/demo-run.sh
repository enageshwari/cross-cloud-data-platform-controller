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
config() {
  echo "${BOLD}  engine:       $1${NC}"
  echo "${BOLD}  target_cloud: $2${NC}"
  echo "${BOLD}  region:       $3${NC}"
  echo "${BOLD}  priority:     $4${NC}"
}

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
kubectl logs -n $NS -l app=control-plane-api --context $GKE_CTX --since=2h 2>/dev/null \
  | grep -E "submitter ready|presigner ready|API starting" | tail -7 \
  || echo "  (startup logs not found — pods may have been running > 2h; check manually)"

pause

# ── Case 1: AWS Spark ──────────────────────────────────────────────────────
header "CASE 1: AWS Spark — SparkApplication → EKS → S3"
config "spark" "aws" "us-west-1" "batch-low"
info "Submitting..."

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

info "Watching SparkApplication on EKS..."
kubectl get sparkapplication $SPARK_APP -n $NS --context $EKS_CTX -w &
WATCH_PID=$!
sleep 30
kill $WATCH_PID 2>/dev/null

info "Final status:"
kubectl get sparkapplication $SPARK_APP -n $NS --context $EKS_CTX 2>/dev/null || echo "(may have completed)"
pause

# ── Case 2: AWS Flink ──────────────────────────────────────────────────────
header "CASE 2: AWS Flink — AppWrapper → EKS → S3"
config "flink" "aws" "us-west-1" "batch-low"
info "Submitting..."

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

sleep 15
kubectl get appwrapper $AW -n $NS --context $EKS_CTX 2>/dev/null | cat
kubectl get flinkdeployment $AW -n $NS --context $EKS_CTX 2>/dev/null | cat || true
kubectl get pods -n $NS --context $EKS_CTX 2>/dev/null | grep $AW || true
pause

# ── Case 3: GCP Spark ──────────────────────────────────────────────────────
header "CASE 3: GCP Spark — SparkApplication → GKE → GCS"
config "spark" "gcp" "us-west2" "batch-low"
info "Submitting..."

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

info "Watching SparkApplication on GKE..."
kubectl get sparkapplication $SPARK_APP -n $NS --context $GKE_CTX -w &
WATCH_PID=$!
sleep 30
kill $WATCH_PID 2>/dev/null

info "Final status:"
kubectl get sparkapplication $SPARK_APP -n $NS --context $GKE_CTX 2>/dev/null || echo "(may have completed)"
pause

# ── Case 4: GCP Flink ──────────────────────────────────────────────────────
header "CASE 4: GCP Flink — AppWrapper → GKE → GCS"
config "flink" "gcp" "us-west2" "interactive-high"
info "Submitting..."

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
echo "  What we're testing: output_path contains 'us-east-1' but declared region is 'us-west-1'"
echo "  OPA rule: outputPath must contain the declared region token"
echo "  Expected: admission DENIED — CRD never lands in etcd"
echo ""

info "Applying SparkApplication directly via kubectl (bypasses API, OPA still fires)..."
kubectl apply -f - --context $EKS_CTX <<EOF
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: opa-test-wrong-region
  namespace: $NS
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
echo "" && echo "${RED}^ If you see 'denied' above — OPA is working correctly${NC}"

info "OPA constraint violations:"
kubectl get dataresidency --context $EKS_CTX 2>/dev/null | cat
pause

# ── Case 6: Preemption ─────────────────────────────────────────────────────
header "CASE 6: Priority Preemption — interactive-high preempts batch-low"
echo "  How it works:"
echo "  - Both queues share a Kueue cohort (shared quota pool)"
echo "  - interactive-high (priority 1000) can reclaim quota from batch-low (100)"
echo "  - Preemption fires when batch jobs hold quota that interactive needs"
echo ""

info "Current ClusterQueue quota state:"
kubectl get clusterqueue multi-cloud-cluster-queue -o wide --context $EKS_CTX 2>/dev/null | cat

info "Submitting 5 batch-low Spark jobs to saturate the queue..."
BATCH_APPS=()
for i in 1 2 3 4 5; do
  RESP=$(curl -s -X POST $API/api/v1/jobs \
    -H "Content-Type: application/json" \
    -d "{\"job_name\":\"preempt-batch-$i\",\"engine\":\"spark\",\"target_cloud\":\"aws\",\"region\":\"us-west-1\",\"main_class\":\"org.apache.spark.examples.SparkPi\",\"artifact_uri\":\"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar\",\"input_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/input/\",\"output_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/output/\",\"priority\":\"batch-low\"}")
  APP=$(echo $RESP | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('spark_application','?'))")
  BATCH_APPS+=($APP)
  echo "  batch-$i → $APP"
done

sleep 5
info "Queue state (look for ADMITTED batch workloads):"
kubectl get workloads -n $NS --context $EKS_CTX 2>/dev/null | grep -E "QUEUE|batch" | head -10

info "Now submitting interactive-high Flink job..."
config "flink" "aws" "us-west-1" "interactive-high"
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

info "Waiting 20s for Kueue to evaluate preemption..."
sleep 20

info "Preemption events (Reason=Preempted):"
kubectl get events -n $NS --context $EKS_CTX \
  --field-selector reason=Preempted 2>/dev/null | cat
echo "(if empty, all batch jobs completed before quota was exhausted — preemption not needed)"

info "Final workload state:"
kubectl get workloads -n $NS --context $EKS_CTX 2>/dev/null | grep -E "QUEUE|interactive|preempt" | head -10
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
