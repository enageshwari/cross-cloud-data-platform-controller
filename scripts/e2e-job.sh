#!/bin/zsh
# e2e-job.sh — submit a single Spark job to AWS and verify response end-to-end
# Usage: ./scripts/e2e-job.sh [api_url]
API_URL="${1:-http://localhost:8080}"

curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "e2e-spark-test",
    "engine": "spark",
    "target_cloud": "aws",
    "region": "us-west-1",
    "artifact_uri": "apache/spark:3.5.3",
    "input_path": "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path": "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority": "batch-low"
  }' | python3 -m json.tool
