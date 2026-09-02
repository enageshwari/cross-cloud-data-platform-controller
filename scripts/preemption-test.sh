#!/bin/zsh
# preemption-test.sh
# Saturates batch-cluster-queue (6 CPU nominal) with 7 batch Spark jobs,
# then submits an interactive-high Flink job to trigger preemption.
#
# Usage:
#   ./scripts/preemption-test.sh [api_url]
#   ./scripts/preemption-test.sh http://localhost:8080

set -euo pipefail

API_URL="${1:-http://localhost:8080}"
CONTEXT="arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"
NAMESPACE="data-workloads"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo "${CYAN}[$(python3 -c 'import time;print(time.strftime("%H:%M:%S",time.gmtime()))')]${NC} $*"; }
ok()   { echo "${GREEN}[$(python3 -c 'import time;print(time.strftime("%H:%M:%S",time.gmtime()))')]  ✓${NC} $*"; }
warn() { echo "${YELLOW}[$(python3 -c 'import time;print(time.strftime("%H:%M:%S",time.gmtime()))')]  ⚠${NC} $*"; }

kubectl config use-context "$CONTEXT" >/dev/null 2>&1

typeset -A SUBMIT_TIMES WORKLOAD_NAMES JOB_NAMES JOB_PRIORITIES

# ── Step 1: Submit 7 batch jobs to saturate 6 CPU nominal ──
log "Submitting 7 batch-low Spark jobs (7 CPU > 6 nominal — last one should pend)..."

for i in {1..7}; do
  T=$(python3 -c "import time; print(int(time.time()))")
  R=$(curl -s -X POST "$API_URL/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d "{\"job_name\":\"preempt-batch-$i\",\"engine\":\"spark\",\"target_cloud\":\"aws\",\"region\":\"us-west-1\",\"artifact_uri\":\"apache/spark:3.5.3\",\"input_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/input/\",\"output_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/output/\",\"priority\":\"batch-low\"}")
  JID=$(echo "$R" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
  WL=$(echo "$R"  | python3 -c "import sys,json; print(json.load(sys.stdin).get('spark_application',''))" 2>/dev/null)
  SUBMIT_TIMES[$JID]="$T"
  WORKLOAD_NAMES[$JID]="$WL"
  JOB_NAMES[$JID]="preempt-batch-$i"
  JOB_PRIORITIES[$JID]="batch-low"
  ok "Batch $i → job_id=$JID workload=$WL"
done

# ── Step 2: Wait for batch to fill quota ──
log "Waiting 15s for batch jobs to fill quota (Kueue admission + Spark submission)..."
sleep 15

log "Current Kueue state:"
kubectl get workloads -n "$NAMESPACE" 2>/dev/null
echo ""
kubectl get clusterqueue -o wide 2>/dev/null

# ── Step 3: Submit interactive job ──
log "Submitting interactive-high Flink job (should preempt lowest-priority batch)..."
T=$(python3 -c "import time; print(int(time.time()))")
R=$(curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d "{\"job_name\":\"preempt-interactive-1\",\"engine\":\"flink\",\"target_cloud\":\"aws\",\"region\":\"us-west-1\",\"artifact_uri\":\"apache/flink:1.20\",\"input_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/input/\",\"output_path\":\"s3://cross-cloud-data-platform-controller-us-west-1/output/\",\"priority\":\"interactive-high\"}")
JID=$(echo "$R" | python3 -c "import sys,json; print(json.load(sys.stdin).get('job_id',''))" 2>/dev/null)
WL=$(echo "$R"  | python3 -c "import sys,json; print(json.load(sys.stdin).get('spark_application',''))" 2>/dev/null)
SUBMIT_TIMES[$JID]="$T"
WORKLOAD_NAMES[$JID]="$WL"
JOB_NAMES[$JID]="preempt-interactive-1"
JOB_PRIORITIES[$JID]="interactive-high"
ok "Interactive → job_id=$JID workload=$WL"

# ── Step 4: Poll for 90s watching for preemption ──
log "Polling for preemption (90s window)..."
DEADLINE=$(($(python3 -c "import time; print(int(time.time()))") + 90))
typeset -A ADMITTED_TIMES
ALL_IDS=("${(@k)JOB_NAMES}")

while true; do
  NOW=$(python3 -c "import time; print(int(time.time()))")
  PREEMPTED=$(kubectl get events -n "$NAMESPACE" --field-selector reason=Preempted --no-headers 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$PREEMPTED" -gt 0 ]]; then
    ok "PREEMPTION DETECTED — $PREEMPTED event(s):"
    kubectl get events -n "$NAMESPACE" --field-selector reason=Preempted 2>/dev/null
    echo ""
  fi

  ALL_DONE=true
  for JID in $ALL_IDS; do
    [[ -n "${ADMITTED_TIMES[$JID]:-}" ]] && continue
    WL="${WORKLOAD_NAMES[$JID]}"
    [[ -z "$WL" ]] && { ALL_DONE=false; continue; }
    INFO=$(kubectl get workloads -n "$NAMESPACE" -o json 2>/dev/null | python3 -c "
import sys,json
for w in json.load(sys.stdin)['items']:
    if '$WL' in w['metadata']['name']:
        c = next((x for x in w.get('status',{}).get('conditions',[]) if x['type']=='Admitted'), None)
        if c and c['status']=='True':
            print(c['lastTransitionTime'])
        break
" 2>/dev/null)
    if [[ -n "$INFO" ]]; then
      EPOCH=$(python3 -c "from datetime import datetime,timezone; print(int(datetime.strptime('$INFO','%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp()))")
      WAIT=$((EPOCH - SUBMIT_TIMES[$JID]))
      ADMITTED_TIMES[$JID]="$EPOCH"
      ok "[${JOB_PRIORITIES[$JID]}] ${JOB_NAMES[$JID]} admitted (wait=${WAIT}s)"
    else
      ALL_DONE=false
    fi
  done

  $ALL_DONE && break
  [[ $NOW -ge $DEADLINE ]] && { warn "Timeout — some jobs still pending (expected if quota exhausted)"; break; }
  sleep 5
done

# ── Summary ──
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "%-28s %-18s %-10s %-8s %s\n" "JOB" "WORKLOAD" "PRIORITY" "WAIT(s)" "STATUS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
for JID in $ALL_IDS; do
  NAME="${JOB_NAMES[$JID]}"
  PRIO="${JOB_PRIORITIES[$JID]}"
  WL="${WORKLOAD_NAMES[$JID]:-?}"
  if [[ -n "${ADMITTED_TIMES[$JID]:-}" ]]; then
    WAIT=$((ADMITTED_TIMES[$JID] - SUBMIT_TIMES[$JID]))
    STATUS="ADMITTED"
  else
    WAIT="?"; STATUS="PENDING"
  fi
  if [[ "$PRIO" == "interactive-high" ]]; then
    printf "\033[0;32m%-28s %-18s %-10s %-8s %s\033[0m\n" "$NAME" "${WL:0:18}" "$PRIO" "$WAIT" "$STATUS"
  else
    printf "%-28s %-18s %-10s %-8s %s\n" "$NAME" "${WL:0:18}" "$PRIO" "$WAIT" "$STATUS"
  fi
done
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
log "Preemption events:"
kubectl get events -n "$NAMESPACE" --field-selector reason=Preempted 2>/dev/null || echo "  (none)"
echo ""
log "Final ClusterQueue state:"
kubectl get clusterqueue -o wide 2>/dev/null
