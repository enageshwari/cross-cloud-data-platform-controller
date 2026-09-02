#!/bin/zsh
# flink-gcp-test.sh — submit a Flink job to GCP (GCS output)
# Usage: ./scripts/flink-gcp-test.sh [api_url]
API_URL="${1:-http://localhost:8080}"

curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "flink-e2e-gcp",
    "engine": "flink",
    "target_cloud": "gcp",
    "region": "us-west2",
    "artifact_uri": "apache/flink:1.20",
    "input_path": "gs://cross-cloud-data-platform-controller-us-west2/input/",
    "output_path": "gs://cross-cloud-data-platform-controller-us-west2/output/",
    "priority": "batch-low"
  }' | python3 -m json.tool
