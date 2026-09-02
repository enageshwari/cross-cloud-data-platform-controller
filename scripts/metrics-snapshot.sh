#!/bin/zsh
# metrics-snapshot.sh — cross-cloud-data-platform-controller
#
# Pulls a point-in-time snapshot of Kueue + node metrics from both EKS and GKE.
# No Prometheus required — uses kubectl raw API to scrape directly.
#
# Output: timestamped JSON file in ./metrics-snapshots/
# Usage:
#   ./scripts/metrics-snapshot.sh [label]
#   ./scripts/metrics-snapshot.sh "before-load-test"

set -euo pipefail

EKS_CONTEXT="arn:aws:eks:us-west-1:080147880517:cluster/cross-cloud-data-plane"
GKE_CONTEXT="gke_project-965bb0cf-caa0-458d-ba9_us-west2-a_cross-cloud-control-plane"
NAMESPACE="data-workloads"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SNAPSHOTS_DIR="$SCRIPT_DIR/../test-result-snapshots"

LABEL="${1:-snapshot}"
TIMESTAMP=$(python3 -c "import time; print(time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))")
SAFE_LABEL=$(echo "$LABEL" | tr -cs 'a-zA-Z0-9_-' '-' | sed 's/-*$//')
OUTPUT="$SNAPSHOTS_DIR/${TIMESTAMP}-${SAFE_LABEL}.json"

mkdir -p "$SNAPSHOTS_DIR"
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
log() { echo "${CYAN}[$(python3 -c "import time; print(time.strftime('%H:%M:%S',time.gmtime()))")]${NC} $*"; }
ok()  { echo "${GREEN}[$(python3 -c "import time; print(time.strftime('%H:%M:%S',time.gmtime()))")]  ✓${NC} $*"; }

# ── scrape Kueue metrics via kubectl proxy ──
scrape_kueue() {
  local ctx="$1" cloud="$2" out="$3"
  kubectl config use-context "$ctx" >/dev/null 2>&1
  RAW=$(kubectl get --raw \
    "/api/v1/namespaces/kueue-system/services/https:kueue-controller-manager-metrics-service:8443/proxy/metrics" \
    2>/dev/null || echo "")
  if [[ -n "$RAW" ]]; then
    echo "$RAW" | python3 "$SCRIPT_DIR/scrape_kueue.py" "$cloud" > "$out"
  else
    echo '{"kueue":{}}' > "$out"
  fi
}

# ── collect node metrics ──
collect_nodes() {
  local ctx="$1" cloud="$2" out="$3"
  kubectl config use-context "$ctx" >/dev/null 2>&1
  kubectl get nodes -o json | python3 "$SCRIPT_DIR/parse_nodes.py" "$cloud" > "$out"
}

# ── collect Kueue workload status ──
collect_workloads() {
  local ctx="$1" cloud="$2" out="$3"
  kubectl config use-context "$ctx" >/dev/null 2>&1
  kubectl get workloads -n "$NAMESPACE" -o json | python3 "$SCRIPT_DIR/parse_workloads.py" "$cloud" > "$out"
}

log "Collecting metrics snapshot (label: $LABEL)..."

log "Scraping EKS Kueue metrics..."
scrape_kueue "$EKS_CONTEXT" "aws-eks" "$TMP/eks_kueue.json"
ok "EKS Kueue metrics scraped"

log "Scraping GKE Kueue metrics..."
scrape_kueue "$GKE_CONTEXT" "gcp-gke" "$TMP/gke_kueue.json"
ok "GKE Kueue metrics scraped"

log "Collecting EKS node metrics..."
collect_nodes "$EKS_CONTEXT" "aws-eks" "$TMP/eks_nodes.json"
ok "EKS node metrics collected"

log "Collecting GKE node metrics..."
collect_nodes "$GKE_CONTEXT" "gcp-gke" "$TMP/gke_nodes.json"
ok "GKE node metrics collected"

log "Collecting EKS workload queue status..."
collect_workloads "$EKS_CONTEXT" "aws-eks" "$TMP/eks_queues.json"
ok "EKS workload status collected"

log "Collecting GKE workload queue status..."
collect_workloads "$GKE_CONTEXT" "gcp-gke" "$TMP/gke_queues.json"
ok "GKE workload status collected"

log "Assembling snapshot..."
python3 "$SCRIPT_DIR/assemble_snapshot.py" "$LABEL" "$TIMESTAMP" "$TMP" "$OUTPUT"
ok "Snapshot saved: $OUTPUT"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "SNAPSHOT: $LABEL @ $TIMESTAMP"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 "$SCRIPT_DIR/assemble_snapshot.py" "$LABEL" "$TIMESTAMP" "$TMP" "$OUTPUT"
echo ""
echo "Full snapshot → $OUTPUT"
echo ""
echo "Useful follow-up commands:"
echo "  kubectl get events -n $NAMESPACE --field-selector reason=Preempted"
echo "  kubectl get clusterqueue -o wide"
