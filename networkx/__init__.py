class DiGraph:
    def __init__(self):
        self._nodes = {}
        self._edges = {}
    def add_node(self, n, **attrs):
        self._nodes.setdefault(n, {}).update(attrs)
        self._edges.setdefault(n, set())
    def add_edge(self, u, v):
        self.add_node(u)
        self.add_node(v)
        self._edges[u].add(v)
    def predecessors(self, n):
        return [u for u, targets in self._edges.items() if n in targets]
    def __contains__(self, n):
        return n in self._nodes
    @property
    def nodes(self):
        return self._nodes


def topological_sort(g):
    indegree = {n: 0 for n in g._nodes}
    for u, targets in g._edges.items():
        for v in targets:
            indegree[v] += 1
    queue = [n for n, d in indegree.items() if d == 0]
    out = []
    while queue:
        n = queue.pop(0)
        out.append(n)
        for v in g._edges.get(n, []):
            indegree[v] -= 1
            if indegree[v] == 0:
                queue.append(v)
    if len(out) != len(g._nodes):
        raise ValueError("graph has cycles")
    return out
