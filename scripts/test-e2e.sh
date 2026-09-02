#!/bin/zsh
# test-e2e.sh — full end-to-end smoke test: submit AWS Spark job, PUT via
# presigned URL, verify object landed in S3, then test GCP path.
#
# Usage:
#   ./scripts/test-e2e.sh [api_url]
#   ./scripts/test-e2e.sh http://localhost:8080
set -e
BASE="${1:-http://localhost:8080}"

echo "\n=== 1. Valid AWS Spark job — expect 202 + presigned URL ==="
RESPONSE=$(curl -s -X POST $BASE/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"daily-etl","engine":"spark","target_cloud":"aws","region":"us-west-1",
       "artifact_uri":"apache/spark:3.5.3",
       "input_path":"s3://cross-cloud-data-platform-controller-us-west-1/input/",
       "output_path":"s3://cross-cloud-data-platform-controller-us-west-1/results/",
       "priority":"batch-low"}')
echo $RESPONSE

PRESIGN_URL=$(echo $RESPONSE | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['output_credential']['url'])")
OBJECT_KEY=$(echo $RESPONSE  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['output_credential']['object_key'])")
EXPIRES_AT=$(echo $RESPONSE  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['output_credential']['expires_at'])")

echo "\nPresigned URL obtained"
echo "Object key : $OBJECT_KEY"
echo "Expires at : $EXPIRES_AT"

echo "\n=== 2. PUT test payload via presigned URL ==="
PUT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$PRESIGN_URL" \
  -H "Content-Type: application/octet-stream" \
  -d "e2e-test payload from cross-cloud-data-platform-controller")
echo "S3 PUT HTTP status: $PUT_CODE"
[ "$PUT_CODE" = "200" ] || { echo "FAIL: expected 200"; exit 1; }

echo "\n=== 3. Verify object landed in S3 ==="
aws s3api head-object \
  --bucket cross-cloud-data-platform-controller-us-west-1 \
  --key "$OBJECT_KEY" 2>&1
# Expect: ContentLength, ETag, ServerSideEncryption=AES256

echo "\n=== 4. Valid GCP Jupyter job — expect 202 + GCS signed URL ==="
curl -s -X POST $BASE/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"notebook-session","engine":"jupyter","target_cloud":"gcp","region":"us-west2",
       "artifact_uri":"apache/spark:3.5.3",
       "input_path":"gs://cross-cloud-data-platform-controller-us-west2/input/",
       "output_path":"gs://cross-cloud-data-platform-controller-us-west2/output/",
       "priority":"interactive-high"}'

echo "\n=== 5. Invalid engine — expect 422 ==="
curl -s -X POST $BASE/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{"job_name":"bad","engine":"hadoop","target_cloud":"aws","region":"us-west-1",
       "artifact_uri":"x","input_path":"s3://in/","output_path":"s3://out/","priority":"batch-low"}'

echo "\n\n=== ALL DONE ==="
