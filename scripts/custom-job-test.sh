#!/bin/zsh
# custom-job-test.sh — cross-cloud-data-platform-controller
#
# Demonstrates submitting custom user JARs (not the bundled SparkPi / WordCount demos).
# Tests all four dispatch paths: Spark+Flink × AWS+GCP.
#
# How to use for a REAL custom job:
#   Edit the variables in the "── Job definitions ──" section below:
#     SPARK_JAR    — your JAR on S3 or GCS  (e.g. s3://my-bucket/jars/my-etl-1.0.jar)
#     SPARK_CLASS  — your Spark main class  (e.g. com.example.MyETLJob)
#     FLINK_JAR    — your JAR on S3 or GCS  (e.g. gs://my-bucket/jars/my-stream-1.0.jar)
#     FLINK_CLASS  — your Flink entry class  (optional — leave empty to use JAR manifest)
#
# For this demo we use the bundled example JARs that ship inside the images.
# These are prefixed with local:/// which means the file is inside the container image.
#
# Usage:
#   ./scripts/custom-job-test.sh [api_url]
#   ./scripts/custom-job-test.sh http://localhost:8080

set -euo pipefail

API_URL="${1:-http://localhost:8080}"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo "${CYAN}[$(python3 -c 'import time;print(time.strftime("%H:%M:%S",time.gmtime()))')]${NC} $*"; }
ok()   { echo "${GREEN}[$(python3 -c 'import time;print(time.strftime("%H:%M:%S",time.gmtime()))')]  ✓${NC} $*"; }
err()  { echo "${YELLOW}[$(python3 -c 'import time;print(time.strftime("%H:%M:%S",time.gmtime()))')]  ✗${NC} $*"; }

# ── Job definitions ──────────────────────────────────────────────────────────
# Replace these with your own JARs and classes for real workloads.

# Spark on AWS — custom ETL class using SparkWordCount as the demo stand-in
SPARK_AWS_JAR="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar"
SPARK_AWS_CLASS="org.apache.spark.examples.SparkPi"
SPARK_AWS_ARGS='["500"]'   # 500 iterations — heavier than default 100
SPARK_AWS_CLOUD="aws"
SPARK_AWS_REGION="us-west-1"
SPARK_AWS_INPUT="s3://cross-cloud-data-platform-controller-us-west-1/input/"
SPARK_AWS_OUTPUT="s3://cross-cloud-data-platform-controller-us-west-1/output/"

# Spark on GCP — same JAR, different cloud and bucket
SPARK_GCP_JAR="local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar"
SPARK_GCP_CLASS="org.apache.spark.examples.SparkPi"
SPARK_GCP_ARGS='["500"]'
SPARK_GCP_CLOUD="gcp"
SPARK_GCP_REGION="us-west2"
SPARK_GCP_INPUT="gs://cross-cloud-data-platform-controller-us-west2/input/"
SPARK_GCP_OUTPUT="gs://cross-cloud-data-platform-controller-us-west2/output/"

# Flink on AWS — custom streaming JAR (WordCount as stand-in)
FLINK_AWS_JAR="local:///opt/flink/examples/streaming/WordCount.jar"
FLINK_AWS_CLASS=""          # empty = use JAR manifest main class
FLINK_AWS_PARALLELISM=2
FLINK_AWS_CLOUD="aws"
FLINK_AWS_REGION="us-west-1"

# Flink on GCP
FLINK_GCP_JAR="local:///opt/flink/examples/streaming/WordCount.jar"
FLINK_GCP_CLASS=""
FLINK_GCP_PARALLELISM=2
FLINK_GCP_CLOUD="gcp"
FLINK_GCP_REGION="us-west2"

# ── Helper: submit a job and print result ─────────────────────────────────────
submit() {
  local desc="$1"
  local payload="$2"
  log "Submitting: $desc"

  RESP=$(curl -s -X POST "$API_URL/api/v1/jobs" \
    -H "Content-Type: application/json" \
    -d "$payload")

  STATUS=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status','error'))" 2>/dev/null || echo "error")
  JOB_ID=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('job_id',''))" 2>/dev/null || echo "")
  WORKLOAD=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('spark_application',''))" 2>/dev/null || echo "")
  MSG=$(echo "$RESP"     | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message',''))" 2>/dev/null || echo "")

  if [[ "$STATUS" == "accepted" ]]; then
    ok "$desc"
    echo "    job_id:   $JOB_ID"
    echo "    workload: $WORKLOAD"
    echo "    message:  $MSG"
  else
    CODE=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('code', d.get('status','?')))" 2>/dev/null || echo "?")
    ERRMSG=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message','unknown'))" 2>/dev/null || echo "unknown")
    err "$desc FAILED [$CODE]: $ERRMSG"
  fi
  echo ""
}

# ── Health check ──────────────────────────────────────────────────────────────
log "Checking API health at $API_URL..."
HEALTH=$(curl -s "$API_URL/healthz" 2>/dev/null || echo '{"status":"unreachable"}')
if echo "$HEALTH" | grep -q '"ok"'; then
  ok "API is up"
else
  err "API not reachable at $API_URL — start it first: cd api && go build -o /tmp/ccdpc-server ./cmd/server && /tmp/ccdpc-server &"
  exit 1
fi
echo ""

# ── Test 1: Spark on AWS with custom JAR + class + args ──────────────────────
submit "Spark / AWS / SparkPi (500 iters) — custom args demo" '{
  "job_name":     "custom-spark-aws",
  "engine":       "spark",
  "target_cloud": "'"$SPARK_AWS_CLOUD"'",
  "region":       "'"$SPARK_AWS_REGION"'",
  "artifact_uri": "'"$SPARK_AWS_JAR"'",
  "main_class":   "'"$SPARK_AWS_CLASS"'",
  "job_args":     '"$SPARK_AWS_ARGS"',
  "input_path":   "'"$SPARK_AWS_INPUT"'",
  "output_path":  "'"$SPARK_AWS_OUTPUT"'",
  "priority":     "batch-low"
}'

# ── Test 2: Spark on GCP with custom JAR + class ─────────────────────────────
submit "Spark / GCP / SparkPi (500 iters)" '{
  "job_name":     "custom-spark-gcp",
  "engine":       "spark",
  "target_cloud": "'"$SPARK_GCP_CLOUD"'",
  "region":       "'"$SPARK_GCP_REGION"'",
  "artifact_uri": "'"$SPARK_GCP_JAR"'",
  "main_class":   "'"$SPARK_GCP_CLASS"'",
  "job_args":     '"$SPARK_GCP_ARGS"',
  "input_path":   "'"$SPARK_GCP_INPUT"'",
  "output_path":  "'"$SPARK_GCP_OUTPUT"'",
  "priority":     "batch-low"
}'

# ── Test 3: Flink on AWS with custom JAR + parallelism ───────────────────────
submit "Flink / AWS / WordCount (parallelism=2)" '{
  "job_name":     "custom-flink-aws",
  "engine":       "flink",
  "target_cloud": "'"$FLINK_AWS_CLOUD"'",
  "region":       "'"$FLINK_AWS_REGION"'",
  "artifact_uri": "'"$FLINK_AWS_JAR"'",
  "parallelism":  '"$FLINK_AWS_PARALLELISM"',
  "input_path":   "s3://cross-cloud-data-platform-controller-us-west-1/input/",
  "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
  "priority":     "interactive-high"
}'

# ── Test 4: Flink on GCP ─────────────────────────────────────────────────────
submit "Flink / GCP / WordCount (parallelism=2)" '{
  "job_name":     "custom-flink-gcp",
  "engine":       "flink",
  "target_cloud": "'"$FLINK_GCP_CLOUD"'",
  "region":       "'"$FLINK_GCP_REGION"'",
  "artifact_uri": "'"$FLINK_GCP_JAR"'",
  "parallelism":  '"$FLINK_GCP_PARALLELISM"',
  "input_path":   "gs://cross-cloud-data-platform-controller-us-west2/input/",
  "output_path":  "gs://cross-cloud-data-platform-controller-us-west2/output/",
  "priority":     "interactive-high"
}'

# ── Test 5: Validation — missing main_class for Spark should return 422 ──────
log "Testing validation: Spark job without main_class (expect 422)..."
RESP=$(curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name":     "missing-class",
    "engine":       "spark",
    "target_cloud": "aws",
    "region":       "us-west-1",
    "artifact_uri": "s3://my-bucket/my.jar",
    "input_path":   "s3://my-bucket/input/",
    "output_path":  "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority":     "batch-low"
  }')
CODE=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('code','?'))" 2>/dev/null)
MSG=$(echo  "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('message','?'))" 2>/dev/null)
if [[ "$CODE" == "422" ]]; then
  ok "Validation correctly returned 422: $MSG"
else
  err "Expected 422, got $CODE: $MSG"
fi
echo ""

log "Done. Check cluster workloads with:"
echo "  kubectl get sparkapplication,appwrapper -n data-workloads"
echo "  kubectl get workloads -n data-workloads"
