#!/bin/zsh
# teardown.sh — Cross-Cloud Data Platform Controller
#
# Deletes ALL cloud resources created for this project:
#   AWS: EKS cluster, node groups, S3 bucket objects
#   GCP: GKE cluster, GCS bucket objects
#
# This is IRREVERSIBLE. The script requires explicit confirmation
# before deleting anything. Run with --dry-run to see what would happen.
#
# Usage:
#   ./scripts/teardown.sh           # interactive confirmation
#   ./scripts/teardown.sh --dry-run # show commands without running them

set -euo pipefail

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

# ── Config ────────────────────────────────────────────────────────────────
EKS_CLUSTER="cross-cloud-data-plane"
EKS_REGION="us-west-1"
S3_BUCKET="cross-cloud-data-platform-controller-us-west-1"

GKE_CLUSTER="cross-cloud-control-plane"
GKE_ZONE="us-west2-a"
GCP_PROJECT="project-965bb0cf-caa0-458d-ba9"
GCS_BUCKET="cross-cloud-data-platform-controller-us-west2"

# ── Helpers ───────────────────────────────────────────────────────────────
BOLD='\033[1m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
NC='\033[0m'

run() {
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "  [dry-run] $*"
  else
    eval "$@"
  fi
}

step() { echo "\n${BOLD}▶ $1${NC}"; }
ok()   { echo "${GREEN}  ✓ $1${NC}"; }
warn() { echo "${YELLOW}  ! $1${NC}"; }

# ── Confirmation ──────────────────────────────────────────────────────────
echo "${RED}${BOLD}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  TEARDOWN — Cross-Cloud Data Platform Controller"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "  This will permanently delete:"
echo ""
echo "  AWS:"
echo "    EKS cluster:  $EKS_CLUSTER ($EKS_REGION)"
echo "    S3 bucket:    s3://$S3_BUCKET (all objects)"
echo ""
echo "  GCP:"
echo "    GKE cluster:  $GKE_CLUSTER ($GKE_ZONE)"
echo "    GCS bucket:   gs://$GCS_BUCKET (all objects)"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "${YELLOW}  DRY RUN — no resources will be deleted${NC}"
  echo ""
else
  printf "${RED}${BOLD}  Type 'yes' to confirm deletion: ${NC}"
  read CONFIRM
  if [[ "$CONFIRM" != "yes" ]]; then
    echo "  Aborted."
    exit 0
  fi
fi

echo ""

# ── AWS Teardown ──────────────────────────────────────────────────────────
step "AWS: Empty S3 bucket (delete all objects and versions)"
run "aws s3 rm s3://$S3_BUCKET --recursive --region $EKS_REGION 2>/dev/null || true"
run "aws s3api delete-objects \
  --bucket $S3_BUCKET \
  --region $EKS_REGION \
  --delete \"\$(aws s3api list-object-versions \
    --bucket $S3_BUCKET \
    --region $EKS_REGION \
    --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
    --output json 2>/dev/null)\" 2>/dev/null || true"
ok "S3 bucket emptied"

step "AWS: Delete EKS cluster (also deletes node groups, VPC, etc.)"
warn "This takes 10-15 minutes..."
run "eksctl delete cluster --name $EKS_CLUSTER --region $EKS_REGION --wait"
ok "EKS cluster deleted"

# ── GCP Teardown ──────────────────────────────────────────────────────────
step "GCP: Empty GCS bucket (delete all objects)"
run "gcloud storage rm -r gs://$GCS_BUCKET/** \
  --project=$GCP_PROJECT 2>/dev/null || true"
ok "GCS bucket emptied"

step "GCP: Delete GKE cluster"
warn "This takes 3-5 minutes..."
run "gcloud container clusters delete $GKE_CLUSTER \
  --zone=$GKE_ZONE \
  --project=$GCP_PROJECT \
  --quiet"
ok "GKE cluster deleted"

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "${BOLD}${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "  DRY RUN COMPLETE — no resources were deleted"
else
  echo "  TEARDOWN COMPLETE"
  echo ""
  echo "  Remaining (not deleted by this script):"
  echo "    AWS IAM user cross-cloud-presigner (keys only — delete manually)"
  echo "    GCP SA gcs-presigner (delete in IAM console if no longer needed)"
  echo "    GCP Artifact Registry image (delete in Artifact Registry console)"
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
