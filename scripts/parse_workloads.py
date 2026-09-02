#!/usr/bin/env python3
"""Parse kubectl get workloads -o json output from stdin."""
import sys, json, time

cloud = sys.argv[1] if len(sys.argv) > 1 else "unknown"
data = json.load(sys.stdin)
workloads = []

for w in data.get('items', []):
    meta   = w['metadata']
    status = w.get('status', {})
    conds  = {c['type']: c for c in status.get('conditions', [])}
    workloads.append({
        'name':           meta['name'],
        'queue':          meta.get('labels', {}).get('kueue.x-k8s.io/queue-name', ''),
        'priority_class': meta.get('labels', {}).get('kueue.x-k8s.io/priority-class', ''),
        'admitted':       conds.get('Admitted', {}).get('status', 'False'),
        'admitted_at':    conds.get('Admitted', {}).get('lastTransitionTime', ''),
        'finished':       conds.get('Finished', {}).get('status', 'False'),
    })

print(json.dumps({
    "cloud":      cloud,
    "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "workloads":  workloads
}))
