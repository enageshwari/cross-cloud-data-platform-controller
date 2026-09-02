#!/usr/bin/env python3
"""Assemble individual JSON pieces into a single snapshot file and print summary."""
import sys, json, os

label     = sys.argv[1]
timestamp = sys.argv[2]
tmpdir    = sys.argv[3]
output    = sys.argv[4]

def load(name):
    path = os.path.join(tmpdir, name)
    try:
        with open(path) as f:
            content = f.read().strip()
            return json.loads(content) if content else {}
    except Exception as e:
        return {}

eks_kueue  = load("eks_kueue.json")
gke_kueue  = load("gke_kueue.json")
eks_nodes  = load("eks_nodes.json")
gke_nodes  = load("gke_nodes.json")
eks_queues = load("eks_queues.json")
gke_queues = load("gke_queues.json")

snapshot = {
    "label":     label,
    "timestamp": timestamp,
    "eks": {
        "kueue_metrics":   eks_kueue.get("kueue", {}),
        "node_metrics":    eks_nodes.get("nodes", []),
        "queue_workloads": eks_queues.get("workloads", []),
    },
    "gke": {
        "kueue_metrics":   gke_kueue.get("kueue", {}),
        "node_metrics":    gke_nodes.get("nodes", []),
        "queue_workloads": gke_queues.get("workloads", []),
    },
}

with open(output, 'w') as f:
    json.dump(snapshot, f, indent=2)

# Print summary to stdout
for cloud, key in [("EKS", "eks"), ("GKE", "gke")]:
    d = snapshot[key]
    print(f"\n{cloud}:")
    nodes = d['node_metrics']
    print(f"  Nodes: {len(nodes)}")
    for n in nodes:
        print(f"    {n['name'][-35:]:35s}  {n['allocatable_cpu_m']}m CPU  "
              f"{n['allocatable_memory_gi']}Gi mem  [{n['capacity_type']}]")

    wls = d['queue_workloads']
    admitted = [w for w in wls if w['admitted'] == 'True']
    pending  = [w for w in wls if w['admitted'] != 'True']
    print(f"  Workloads: {len(admitted)} admitted, {len(pending)} pending")
    for w in wls:
        status = "ADMITTED" if w['admitted'] == 'True' else "PENDING"
        pc     = w.get('priority_class', 'unknown')
        print(f"    [{status:8s}] {w['name'][-40:]:40s}  [{pc}]")

    kueue = d['kueue_metrics']
    pend = kueue.get('pending_workloads', [])
    if pend:
        print(f"  Kueue pending_workloads metric:")
        for m in pend:
            print(f"    clusterqueue={m.get('cluster_queue','?')} "
                  f"status={m.get('status','?')} count={m['_value']:.0f}")
    evicted = kueue.get('evicted_workloads_total', [])
    if evicted:
        print(f"  Kueue evicted (preemptions):")
        for m in evicted:
            print(f"    clusterqueue={m.get('cluster_queue','?')} "
                  f"reason={m.get('reason','?')} count={m['_value']:.0f}")
