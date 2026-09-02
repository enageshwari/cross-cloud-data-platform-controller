#!/bin/zsh
# load-test.sh — cross-cloud-data-platform-controller
#
# Validates batch vs interactive preemption and resource contention on EKS.
#
# What it does:
#   1. Submits 3 batch-low Spark jobs concurrently (fills batch-cluster-queue)
#   2. Waits 10s, then submits 1 interactive-high Flink job
#   3. Polls Kueue Workload admission status every 5s for up to 5 min
#   4. Records submission time, admission time, queue wait per workload
#   5. Prints a summary table showing preemption effect
#
# Requirements: kubectl configured for EKS, API server running on $API_URL
#
# Usage:
#   ./scripts/load-test.sh [api_url]
#   ./scripts/load-test.sh http://localhost:8080   (default)

set -euo pipefail

API_URL="${1:-http://localhost:8080}"
CONTEXT="arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"
NAMESPACE="data-workloads"
TIMEOUT=300   # 5 minutes max wait
POLL=5

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo "${CYAN}[$(date -u +%H:%M:%S)]${NC} $*"; }
ok()   { echo "${GREEN}[$(date -u +%H:%M:%S)] ✓${NC} $*"; }
warn() { echo "${YELLOW}[$(date -u +%H:%M:%S)] ⚠${NC} $*"; }

# Arrays to track job metadata
typeset -A JOB_NAMES JOB_TYPES JOB_PRIORITIES SUBMIT_TIMES WORKLOAD_NAMES

kubectl config use-context "$CONTEXT" >/dev/null 2>&1

# ─────────────────────────────────────────────
# 1. Submit 3 batch Spark jobs concurrently
# ─────────────────────────────────────────────
log "Submitting 3 batch-low Spark jobs concurrently..."

for i in 1 2 3; do
  JOB_NAME="load-test-batch-$i"
  SUBMIT_TIME=$(python3 -c "import time; print(int(time.time()))")
  RESPONSE=$(curl -s -X POST "$API_URL/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d "{
      \"job_name\": \"$JOB_NAME\",
      \"engine\": \"spark\",
      \"target_cloud\": \"aws\",
      \"region\": \"us-west-1\",
      \"artifact_uri\": \"apache/spark:3.5.3\",
      \"input_path\": \"s3://cross-cloud-data-platform-controller-us-west-1/input/\",
      \"output_path\": \"s3://cross-cloud-data-platform-controller-us-west-1/output/\",
      \"priority\": \"batch-low\"
    }")
  JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null)
  WL_NAME=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('spark_application',''))" 2>/dev/null)
  JOB_NAMES[$JOB_ID]="$JOB_NAME"
  JOB_TYPES[$JOB_ID]="spark"
  JOB_PRIORITIES[$JOB_ID]="batch-low"
  SUBMIT_TIMES[$JOB_ID]="$SUBMIT_TIME"
  WORKLOAD_NAMES[$JOB_ID]="$WL_NAME"
  ok "Batch job $i submitted: job_id=$JOB_ID workload=$WL_NAME"
done

# ─────────────────────────────────────────────
# 2. Wait 10s, then submit interactive Flink job
# ─────────────────────────────────────────────
log "Waiting 10s before submitting interactive job (to let batch fill quota)..."
sleep 10

INTERACTIVE_NAME="load-test-interactive-1"
SUBMIT_TIME=$(date -u +%s)
RESPONSE=$(curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d "{
    \"job_name\": \"$INTERACTIVE_NAME\",
    \"engine\": \"flink\",
    \"target_cloud\": \"aws\",
    \"region\": \"us-west-1\",
    \"artifact_uri\": \"apache/flink:1.20\",
    \"input_path\": \"s3://cross-cloud-data-platform-controller-us-west-1/input/\",
    \"output_path\": \"s3://cross-cloud-data-platform-controller-us-west-1/output/\",
    \"priority\": \"interactive-high\"
  }")
JOB_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null)
WL_NAME=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('spark_application',''))" 2>/dev/null)
JOB_NAMES[$JOB_ID]="$INTERACTIVE_NAME"
JOB_TYPES[$JOB_ID]="flink"
JOB_PRIORITIES[$JOB_ID]="interactive-high"
SUBMIT_TIMES[$JOB_ID]="$SUBMIT_TIME"
WORKLOAD_NAMES[$JOB_ID]="$WL_NAME"
ok "Interactive Flink job submitted: job_id=$JOB_ID workload=$WL_NAME"

# ─────────────────────────────────────────────
# 3. Poll admission status
# ─────────────────────────────────────────────
log "Polling Kueue workload admission (timeout=${TIMEOUT}s, poll=${POLL}s)..."

typeset -A ADMITTED_TIMES FINISHED_TIMES WORKLOAD_STATUS

DEADLINE=$(($(date -u +%s) + TIMEOUT))
ALL_IDS=("${(@k)JOB_NAMES}")

while true; do
  NOW=$(date -u +%s)
  ALL_DONE=true

  for JOB_ID in $ALL_IDS; do
    WL="${WORKLOAD_NAMES[$JOB_ID]}"
    PRIORITY="${JOB_PRIORITIES[$JOB_ID]}"
    [[ -z "$WL" ]] && { warn "No workload name for $JOB_ID — skipping"; continue; }

    # Get Kueue Workload resource (name has a prefix)
    WL_INFO=$(kubectl get workloads -n "$NAMESPACE" -o json 2>/dev/null | \
      python3 -c "
import sys,json
wls = json.load(sys.stdin)['items']
for w in wls:
    if '$WL' in w['metadata']['name']:
        admitted = next((c for c in w.get('status',{}).get('conditions',[]) if c['type']=='Admitted'),None)
        finished = next((c for c in w.get('status',{}).get('conditions',[]) if c['type']=='Finished'),None)
        print(w['metadata']['name'],
              admitted['status'] if admitted else 'False',
              admitted['lastTransitionTime'] if admitted else '',
              finished['status'] if finished else 'False')
        break
" 2>/dev/null)

    if [[ -z "$WL_INFO" ]]; then
      ALL_DONE=false
      continue
    fi

    WL_FULL=$(echo "$WL_INFO" | awk '{print $1}')
    ADMITTED=$(echo "$WL_INFO" | awk '{print $2}')
    ADMITTED_AT=$(echo "$WL_INFO" | awk '{print $3}')
    FINISHED=$(echo "$WL_INFO" | awk '{print $4}')

    if [[ "$ADMITTED" == "True" ]] && [[ -z "${ADMITTED_TIMES[$JOB_ID]:-}" ]]; then
      ADMITTED_EPOCH=$(python3 -c "from datetime import datetime,timezone; print(int(datetime.strptime('$ADMITTED_AT','%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp()))" 2>/dev/null || echo "0")
      ADMITTED_TIMES[$JOB_ID]="$ADMITTED_EPOCH"
      WAIT=$((ADMITTED_EPOCH - SUBMIT_TIMES[$JOB_ID]))
      ok "[$PRIORITY] ${JOB_NAMES[$JOB_ID]} admitted (queue_wait=${WAIT}s)"
    fi

    if [[ "$FINISHED" == "True" ]] && [[ -z "${FINISHED_TIMES[$JOB_ID]:-}" ]]; then
      FINISHED_TIMES[$JOB_ID]="$NOW"
    fi

    # Done when admitted (Flink/Spark jobs are fire-and-forget from API perspective)
    [[ -z "${ADMITTED_TIMES[$JOB_ID]:-}" ]] && ALL_DONE=false
  done

  $ALL_DONE && break
  [[ $NOW -ge $DEADLINE ]] && { warn "Timeout reached — some jobs may still be pending"; break; }
  sleep $POLL
done

# ─────────────────────────────────────────────
# 4. Print summary table
# ─────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "%-30s %-18s %-12s %-10s %s\n" "JOB NAME" "WORKLOAD" "PRIORITY" "WAIT(s)" "STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for JOB_ID in $ALL_IDS; do
  NAME="${JOB_NAMES[$JOB_ID]}"
  WL="${WORKLOAD_NAMES[$JOB_ID]:-unknown}"
  PRIO="${JOB_PRIORITIES[$JOB_ID]}"
  SUBMIT="${SUBMIT_TIMES[$JOB_ID]}"
  ADMITTED="${ADMITTED_TIMES[$JOB_ID]:-}"

  if [[ -n "$ADMITTED" ]]; then
    WAIT=$((ADMITTED - SUBMIT))
    STATUS="ADMITTED"
  else
    WAIT="?"
    STATUS="PENDING"
  fi

  # Color interactive differently
  if [[ "$PRIO" == "interactive-high" ]]; then
    printf "${GREEN}%-30s %-18s %-12s %-10s %s${NC}\n" "$NAME" "${WL:0:18}" "$PRIO" "$WAIT" "$STATUS"
  else
    printf "%-30s %-18s %-12s %-10s %s\n" "$NAME" "${WL:0:18}" "$PRIO" "$WAIT" "$STATUS"
  fi
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
log "Check preemption events:"
echo "  kubectl get events -n $NAMESPACE --field-selector reason=Preempted"
echo ""
log "Check current Kueue queue depths:"
echo "  kubectl get clusterqueue,localqueue -n $NAMESPACE"
