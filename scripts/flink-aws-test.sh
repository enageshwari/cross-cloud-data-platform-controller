#!/bin/zsh
# flink-aws-test.sh — submit a Flink job to AWS EKS
# Usage: ./scripts/flink-aws-test.sh [api_url]
API_URL="${1:-http://localhost:8080}"

curl -s -X POST "$API_URL/api/v1/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "job_name": "flink-e2e-aws",
    "engine": "flink",
    "target_cloud": "aws",
    "region": "us-west-1",
    "artifact_uri": "apache/flink:1.20",
    "input_path": "s3://cross-cloud-data-platform-controller-us-west-1/input/",
    "output_path": "s3://cross-cloud-data-platform-controller-us-west-1/output/",
    "priority": "batch-low"
  }' | python3 -m json.tool
