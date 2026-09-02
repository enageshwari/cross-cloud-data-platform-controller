#!/usr/bin/env python3
"""Scrape key Kueue metrics from stdin (raw Prometheus text format)."""
import sys, re, json, time

cloud = sys.argv[1] if len(sys.argv) > 1 else "unknown"
lines = sys.stdin.read().splitlines()

patterns = {
    "pending_workloads":        r'^kueue_pending_workloads\{',
    "admitted_workloads_total": r'^kueue_admitted_workloads_total\{',
    "evicted_workloads_total":  r'^kueue_evicted_workloads_total\{',
    "quota_reserved_cpu":       r'^kueue_cluster_queue_resource_reservation\{.*resource="cpu"',
    "quota_reserved_memory":    r'^kueue_cluster_queue_resource_reservation\{.*resource="memory"',
    "admitted_active":          r'^kueue_admitted_active_workloads\{',
}

results = {k: [] for k in patterns}
for line in lines:
    if line.startswith('#'):
        continue
    for key, pat in patterns.items():
        if re.match(pat, line):
            label_part = re.search(r'\{(.+)\}', line)
            val_part   = re.search(r'\}\s+([\d.e+\-]+)', line)
            if label_part and val_part:
                labels = dict(re.findall(r'(\w+)="([^"]*)"', label_part.group(1)))
                labels['_value'] = float(val_part.group(1))
                results[key].append(labels)

print(json.dumps({
    "cloud":      cloud,
    "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "kueue":      results
}))
