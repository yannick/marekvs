#!/usr/bin/env python3
"""Pure partition-topology (grudge) builders, ported from Jepsen's
jepsen.nemesis (complete-grudge / bridge / majorities-ring).

A *grudge* is a map node -> set of nodes it must NOT talk to. The chaos
harness realizes it with symmetric iptables DROP rules on the mesh subnet.
These builders are pure set math and fully self-tested (`--test`) — they
must be correct before they gate a live cluster, exactly like the operator's
scale.rs decision functions.

Emitted format (consumed by lib.sh): one line per directed cut "a b",
meaning node a drops traffic from/to node b. The harness applies both
directions, so emitting each unordered pair once is enough; we emit the
full symmetric set for clarity and let iptables dedup.
"""
import sys


def complete_grudge(components):
    """Jepsen complete-grudge: given a partition of nodes into components,
    every node distrusts every node NOT in its own component."""
    allnodes = sorted(n for c in components for n in c)
    grudge = {n: set() for n in allnodes}
    for comp in components:
        cset = set(comp)
        for n in comp:
            grudge[n] = set(allnodes) - cset
    return grudge


def bisect(nodes):
    """Split into two halves, smaller (or equal) half first — deterministic
    like Jepsen's bisect."""
    nodes = sorted(nodes)
    h = len(nodes) // 2
    return [nodes[:h], nodes[h:]]


def grudge_halves(n):
    """Two mutually-deaf components."""
    return complete_grudge(bisect(range(n)))


def grudge_bridge(n):
    """Jepsen bridge: bisect into two halves, but one 'bridge' node keeps
    full connectivity to BOTH halves (removed from every grudge). Requires
    n >= 3 to have a bridge plus a node on each side."""
    if n < 3:
        raise ValueError("bridge needs n >= 3")
    left, right = bisect(range(n))
    # The bridge is the last node of the larger (right) half; it can see all.
    bridge = right[-1]
    g = complete_grudge([left, right])
    # Bridge trusts everyone and everyone trusts the bridge.
    g[bridge] = set()
    for node in g:
        g[node].discard(bridge)
    return g


def grudge_majorities_ring(n):
    """Jepsen majorities-ring: every node sees a majority, but no two see the
    same one. Built as a CIRCULANT graph C_n(1..k) — node i is linked to its
    k nearest neighbours on each side (mod n) — which is symmetric by
    construction (i~j iff their ring distance <= k). k is the smallest value
    making the window (self + 2k neighbours) a strict majority, so cuts
    actually exist. For n=3 the majority window is the whole ring (no cuts);
    real runs use n=5, where each node sees exactly 3 of 5 and all five
    windows differ."""
    if n < 3:
        raise ValueError("majorities-ring needs n >= 3")
    majority = n // 2 + 1
    # smallest k with 2k+1 >= majority
    k = 0
    while 2 * k + 1 < majority:
        k += 1

    def ring_dist(a, b):
        d = abs(a - b) % n
        return min(d, n - d)

    grudge = {}
    for i in range(n):
        grudge[i] = {j for j in range(n) if j != i and ring_dist(i, j) > k}
    return grudge


def emit(grudge):
    lines = []
    for a in sorted(grudge):
        for b in sorted(grudge[a]):
            lines.append(f"{a} {b}")
    return "\n".join(lines)


# ── self-test ────────────────────────────────────────────────────────────
def _test():
    # complete grudge on halves of 4: {0,1} | {2,3}
    g = grudge_halves(4)
    assert g[0] == {2, 3} and g[1] == {2, 3}, g
    assert g[2] == {0, 1} and g[3] == {0, 1}, g
    # symmetry: a denies b  <=>  b denies a
    for gg in (grudge_halves(4), grudge_halves(5), grudge_bridge(5),
               grudge_majorities_ring(5), grudge_majorities_ring(4),
               grudge_majorities_ring(3)):
        for a in gg:
            for b in gg[a]:
                assert a in gg[b], f"asymmetric cut {a}-{b}: {gg}"

    # bridge(5): halves {0,1} | {2,3,4}, bridge=4 sees everyone
    g = grudge_bridge(5)
    assert g[4] == set(), g
    for node in g:
        assert 4 not in g[node], g
    # the two non-bridge halves still cannot see each other
    assert 2 in g[0] and 3 in g[0], g
    assert 0 in g[2] and 1 in g[2], g

    # majorities-ring(5): every node sees a majority (>=3 incl. self),
    # no two windows identical
    g = grudge_majorities_ring(5)
    windows = []
    for i in range(5):
        seen = {i} | ({j for j in range(5) if j != i and j not in g[i]})
        assert len(seen) >= 3, f"node {i} lacks majority: {seen}"
        windows.append(frozenset(seen))
    assert len(set(windows)) == 5, f"windows not all distinct: {windows}"

    # ring must NOT fully partition anyone (every node reaches every other
    # via some path — connectivity of the 'up' graph)
    def connected(gg, n):
        up = {i: {j for j in range(n) if j != i and j not in gg[i]} for i in range(n)}
        seen, stack = {0}, [0]
        while stack:
            x = stack.pop()
            for y in up[x]:
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        return len(seen) == n
    assert connected(grudge_majorities_ring(5), 5), "ring disconnected!"
    assert connected(grudge_bridge(5), 5), "bridge disconnected!"
    # halves ARE disconnected by design
    assert not connected(grudge_halves(4), 4), "halves should be split"
    print("grudge.py self-test: OK")


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "--test":
        _test()
    elif len(sys.argv) >= 3:
        topo, n = sys.argv[1], int(sys.argv[2])
        builders = {
            "halves": grudge_halves,
            "bridge": grudge_bridge,
            "ring": grudge_majorities_ring,
        }
        print(emit(builders[topo](n)))
    else:
        print("usage: grudge.py --test | grudge.py <halves|bridge|ring> <N>",
              file=sys.stderr)
        sys.exit(1)
