"""
Example of using Rendezvous hashing to assign keys to nodes for consistent hashing.
See https://en.wikipedia.org/wiki/Rendezvous_hashing

Implements rendezvous_hashing and runs with K nodes for various number of samples, then prints stats.
"""

import hashlib
from collections import Counter
from typing import List, Optional, Callable, Any


def hash_function(key_id: str, node_id: str) -> int:
    """Combines key and node to produce a hash value."""
    # Combine key and node into a single string
    combined = key_id + node_id
    # Use SHA-256 hash function from hashlib and return an integer hash value
    return int(hashlib.sha256(combined.encode('utf-8')).hexdigest(), 16)


def rendezvous_hashing(key_id: str, node_ids: List[str]) -> str:
    """Assigns the given key to a node using rendezvous hashing."""
    highest_weight = -1
    assigned_node: Optional[str] = None
    for node_id in node_ids:
        # Calculate the hash value (weight) for this key-node combination
        weight = hash_function(key_id, node_id)
        # If this is the highest weight so far, select this node
        if weight > highest_weight:
            highest_weight = weight
            assigned_node = node_id
    return assigned_node


def run_statistics(node_ids: List[str], num_samples: int):
    # Generate hashes for given number of samples:
    nodes_counts = Counter(node_ids)
    for i in range(0, num_samples):
        assigned_node = rendezvous_hashing(str(i), node_ids)
        nodes_counts[assigned_node] += 1

    # Collect and print statistics:
    total = nodes_counts.total()
    expected = num_samples / len(node_ids)
    max_dist = max([count for _, count in nodes_counts.items()])
    min_dist = min([count for _, count in nodes_counts.items()])

    def diff_percent(observer: int, expected: int): return round(100 * (observer - expected) / expected, 2)

    print(f"Distribution by node for {len(node_ids)} nodes, {num_samples} samples:")
    print(
        "  "
        f"expected items per node={round(expected)}, "
        f"observed min={min_dist} ({diff_percent(min_dist, expected)}%), "
        f"max={max_dist} ({diff_percent(max_dist, expected)}%)")


if __name__ == '__main__':
    nodes = ['Node1', 'Node2', 'Node3', 'Node4']
    run_statistics(nodes, 100)
    run_statistics(nodes, 1_000)
    run_statistics(nodes, 10_000)
    run_statistics(nodes, 100_000)
