from collections import deque
from typing import List


class KahnTopologicalSort:

    @staticmethod
    def sort(dag) -> List[str]:
        in_degree = {tid: len(dag.get_dependencies(tid)) for tid in dag.nodes}
        queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
        order = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for dependent in dag.get_dependents(current):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        if len(order) != len(dag.nodes):
            remaining = set(dag.nodes) - set(order)
            raise ValueError(f"cycle detected involving: {remaining}")

        return order

    @staticmethod
    def get_execution_levels(dag) -> List[List[str]]:
        KahnTopologicalSort.sort(dag)  # validates no cycle up front
        remaining = set(dag.nodes.keys())
        levels = []

        while remaining:
            current_level = [
                tid for tid in remaining
                if all(dep not in remaining for dep in dag.get_dependencies(tid))
            ]
            levels.append(current_level)
            remaining -= set(current_level)

        return levels
