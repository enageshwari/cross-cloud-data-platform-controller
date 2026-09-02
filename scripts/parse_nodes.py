#!/usr/bin/env python3
"""Parse kubectl get nodes -o json output from stdin into a simplified node list."""
import sys, json, time

cloud = sys.argv[1] if len(sys.argv) > 1 else "unknown"
data = json.load(sys.stdin)
nodes = []

def parse_cpu(v):
    if str(v).endswith('m'):
        return int(str(v)[:-1])
    return int(float(v) * 1000)

def parse_mem(v):
    v = str(v)
    if v.endswith('Ki'): return int(v[:-2]) * 1024
    if v.endswith('Mi'): return int(v[:-2]) * 1024 * 1024
    if v.endswith('Gi'): return int(v[:-2]) * 1024 * 1024 * 1024
    return int(v)

for n in data.get('items', []):
    name  = n['metadata']['name']
    alloc = n['status'].get('allocatable', {})
    labels = n['metadata'].get('labels', {})
    ct = labels.get('eks.amazonaws.com/capacityType',
         labels.get('cloud.google.com/gke-provisioning', 'unknown'))
    # Always coerce to string — GKE sometimes has boolean label values
    ct = str(ct)
    nodes.append({
        'name':                 name,
        'capacity_type':        ct,
        'allocatable_cpu_m':    parse_cpu(alloc.get('cpu', '0')),
        'allocatable_memory_gi': round(parse_mem(alloc.get('memory', '0')) / (1024**3), 1),
    })

print(json.dumps({
    "cloud":      cloud,
    "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "nodes":      nodes
}))
