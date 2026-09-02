#!/bin/zsh
# gcp-job-test.sh — submit a Spark job to GCP (GCS output, V4 signed URL)
# Usage: ./scripts/gcp-job-test.sh [api_url]
API_URL="${1:-http://localhost:8080}"

curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "gcp-e2e-test",
    "engine": "spark",
    "target_cloud": "gcp",
    "region": "us-west2",
    "artifact_uri": "apache/spark:3.5.3",
    "input_path": "gs://cross-cloud-data-platform-controller-us-west2/input/",
    "output_path": "gs://cross-cloud-data-platform-controller-us-west2/output/",
    "priority": "batch-low"
  }' | python3 -m json.tool
