import hashlib
from collections import Counter
from typing import List, Optional


def hash_function(key_id: str, node_id: str) -> int:
    """
    A simple hash function that combines key and node to produce a hash value.
    """
    # Combine key and node into a single string
    combined = key_id + node_id
    # Use SHA-256 hash function from hashlib and return an integer hash value
    return int(hashlib.sha256(combined.encode('utf-8')).hexdigest(), 16)


def rendezvous_hashing(key_id: str, node_ids: List[str]) -> str:
    """
    Assigns the given key to a node using rendezvous hashing.
    """
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


if __name__ == '__main__':
    nodes = ['Node1', 'Node2', 'Node3', 'Node4']

    nodes_counts = Counter(nodes)
    for i in range(0, 100000):
        assigned_node = rendezvous_hashing(str(i), nodes)
        nodes_counts[assigned_node] += 1
    print(nodes_counts)
