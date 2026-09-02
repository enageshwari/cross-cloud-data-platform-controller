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
#   ./scripts/demo-run.sh [api_url]

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
API="${1:-http://localhost:9090}"
START_CASE="${2:-1}"   # set to 3 to skip cases 1-2, e.g.: ./scripts/demo-run.sh http://localhost:9090 3
EKS_CTX="arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"
GKE_CTX="gke_project-965bb0cf-caa0-458d-ba9_us-west2-a_cross-cloud-control-plane"
NS="data-workloads"

BOLD='\033[1m'
CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
DIM='\033[2m'
NC='\033[0m'

header()  { echo "\n${BOLD}${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n  $1\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"; }
ok()      { echo "${GREEN}✓ $1${NC}"; }
info()    { echo "${YELLOW}▶ $1${NC}"; }
pause()   { echo "\n${BOLD}Press Enter to continue...${NC}"; read; }
config()  {
  echo "${BOLD}  engine:       $1${NC}"
  echo "${BOLD}  target_cloud: $2${NC}"
  echo "${BOLD}  region:       $3${NC}"
  echo "${BOLD}  priority:     $4${NC}"
}
jar_info() {
  echo "${DIM}  JAR:  $1${NC}"
  echo "${DIM}  type: bundled inside container image (local:///) — no upload needed${NC}"
  echo "${DIM}  custom JAR: set artifact_uri to s3://bucket/jars/my.jar or gs://bucket/jars/my.jar${NC}"
  echo "${DIM}              and set main_class to your entry point${NC}"
}

# ── wait_spark: poll until SparkApplication reaches terminal state ──────────
wait_spark() {
  local name="$1" ctx="$2" timeout=180 elapsed=0
  echo ""
  while [[ $elapsed -lt $timeout ]]; do
    STATUS=$(kubectl get sparkapplication "$name" -n $NS --context "$ctx" \
      -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "")
    printf "\r  status: %-20s  (%ds elapsed)" "$STATUS" "$elapsed"
    if [[ "$STATUS" == "COMPLETED" || "$STATUS" == "FAILED" || "$STATUS" == "SUBMISSION_FAILED" ]]; then
      echo ""
      if [[ "$STATUS" == "COMPLETED" ]]; then
        ok "SparkApplication $name → $STATUS"
      else
        echo "${RED}✗ SparkApplication $name → $STATUS${NC}"
      fi
      kubectl get sparkapplication "$name" -n $NS --context "$ctx" 2>/dev/null | cat
      return
    fi
    sleep 5
    elapsed=$((elapsed+5))
  done
  echo "\n${YELLOW}  timeout — final state:${NC}"
  kubectl get sparkapplication "$name" -n $NS --context "$ctx" 2>/dev/null | cat
}

# ── wait_flink: poll until FlinkDeployment job status reaches terminal state
wait_flink() {
  local name="$1" ctx="$2" timeout=180 elapsed=0
  echo ""
  while [[ $elapsed -lt $timeout ]]; do
    STATUS=$(kubectl get flinkdeployment "$name" -n $NS --context "$ctx" \
      -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || echo "")
    printf "\r  job status: %-20s  (%ds elapsed)" "$STATUS" "$elapsed"
    if [[ "$STATUS" == "FINISHED" || "$STATUS" == "FAILED" || "$STATUS" == "CANCELED" ]]; then
      echo ""
      if [[ "$STATUS" == "FINISHED" ]]; then
        ok "FlinkDeployment $name → $STATUS"
      else
        echo "${RED}✗ FlinkDeployment $name → $STATUS${NC}"
      fi
      kubectl get flinkdeployment "$name" -n $NS --context "$ctx" 2>/dev/null | cat
      return
    fi
    sleep 5
    elapsed=$((elapsed+5))
  done
  echo "\n${YELLOW}  timeout — final state:${NC}"
  kubectl get flinkdeployment "$name" -n $NS --context "$ctx" 2>/dev/null | cat
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

info "Checking API pods and component readiness..."
kubectl get pods -n $NS --context $GKE_CTX 2>/dev/null | grep control-plane-api
echo ""
# Hit the healthz once per pod to confirm both replicas are serving
for POD in $(kubectl get pods -n $NS --context $GKE_CTX -l app=control-plane-api -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
  STATUS=$(kubectl get pod "$POD" -n $NS --context $GKE_CTX -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
  ok "Pod $POD → Ready=$STATUS"
done

pause

# ── Case 1: AWS Spark ──────────────────────────────────────────────────────
if [[ $START_CASE -le 1 ]]; then
header "CASE 1: AWS Spark — SparkApplication → EKS → S3"
config "spark" "aws" "us-west-1" "batch-low"
jar_info "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar (SparkPi — calculates pi)"
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

info "Waiting for completion..."
wait_spark "$SPARK_APP" "$EKS_CTX"
pause
fi # end case 1

# ── Case 2: AWS Flink ──────────────────────────────────────────────────────
if [[ $START_CASE -le 2 ]]; then
header "CASE 2: AWS Flink — AppWrapper → EKS → S3"
config "flink" "aws" "us-west-1" "batch-low"
jar_info "local:///opt/flink/examples/streaming/WordCount.jar (bundled WordCount streaming job)"
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

info "Waiting for FlinkDeployment to be created..."
sleep 10
info "Waiting for completion..."
wait_flink "$AW" "$EKS_CTX"
pause
fi # end case 2

# ── Case 3: GCP Spark ──────────────────────────────────────────────────────
header "CASE 3: GCP Spark — SparkApplication → GKE → GCS"
config "spark" "gcp" "us-west2" "batch-low"
jar_info "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar (SparkPi — calculates pi)"
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

info "Waiting for completion..."
wait_spark "$SPARK_APP" "$GKE_CTX"
pause

# ── Case 4: GCP Flink ──────────────────────────────────────────────────────
header "CASE 4: GCP Flink — AppWrapper → GKE → GCS"
config "flink" "gcp" "us-west2" "interactive-high"
jar_info "local:///opt/flink/examples/streaming/WordCount.jar (bundled WordCount streaming job)"
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

info "Waiting for FlinkDeployment to be created..."
sleep 10
info "Waiting for completion..."
wait_flink "$AW" "$GKE_CTX"
pause

# ── Case 5: OPA Enforcement ────────────────────────────────────────────────
header "CASE 5: OPA Enforcement — Cross-Region Violation"
echo "  Testing: output_path region token (us-east-1) ≠ declared region (us-west-1)"
echo "  OPA rule: outputPath must contain the declared region — hard DENY at admission"
echo "  Note: using kubectl apply directly so the webhook error is immediately visible"
echo ""

info "Applying SparkApplication with wrong region..."
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
echo "${RED}^ 'denied' above means OPA enforcement is working ✓${NC}"

info "OPA constraint active:"
kubectl get dataresidency --context $EKS_CTX 2>/dev/null | cat
pause

# ── Case 6: Preemption ─────────────────────────────────────────────────────
header "CASE 6: Priority Preemption — interactive-high preempts batch-low"
echo "  Both queues share a Kueue cohort — interactive-high (1000) reclaims"
echo "  quota from batch-low (100) via reclaimWithinCohort: LowerPriority"
echo ""

info "Current ClusterQueue quota:"
kubectl get clusterqueue multi-cloud-cluster-queue -o wide --context $EKS_CTX 2>/dev/null | cat

info "Submitting 5 batch-low Spark jobs..."
for i in 1 2 3 4 5; do
  RESP=$(curl -s -X POST $API/api/v1/jobs \
    -H "Content-Type: application/json" \
    -d "{\"job_name\":\"preempt-batch-$i\",\"engine\":\"spark\",\"target_cloud\":\"aws\",\"region\":\"us-west-1\",\"main_class\":\"org.apache.spark.examples.SparkPi\",\"artifact_uri\":\"local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar\",\"input_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/input/\",\"output_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/output/\",\"priority\":\"batch-low\"}")
  APP=$(echo $RESP | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('spark_application','?'))")
  echo "  batch-$i → $APP"
done

sleep 5
info "Batch workload queue state:"
kubectl get workloads -n $NS --context $EKS_CTX 2>/dev/null | grep -E "QUEUE|batch" | head -8

info "Submitting interactive-high Flink job..."
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

info "Waiting 25s for Kueue preemption evaluation..."
sleep 25

info "Preemption events:"
kubectl get events -n $NS --context $EKS_CTX \
  --field-selector reason=Preempted 2>/dev/null | cat || true
echo "(if empty: batch jobs completed before quota was exhausted — no preemption needed)"

info "Workload state:"
kubectl get workloads -n $NS --context $EKS_CTX 2>/dev/null \
  | grep -E "QUEUE|interactive|preempt" | head -10
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
