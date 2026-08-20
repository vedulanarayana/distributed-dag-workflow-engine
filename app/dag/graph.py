from typing import Dict, List, Optional, Callable
import uuid


class DAGNode:
    def __init__(self, task_id: str, name: str, func: Optional[Callable] = None, dependencies: List[str] = None):
        self.task_id = task_id
        self.name = name
        self.func = func
        self.dependencies = dependencies or []

    def to_dict(self) -> dict:
        return {"task_id": self.task_id, "name": self.name, "dependencies": self.dependencies}


class DAG:
    def __init__(self, workflow_id: str = None):
        self.workflow_id = workflow_id or str(uuid.uuid4())
        self.nodes: Dict[str, DAGNode] = {}

    def add_node(self, name: str, func: Callable = None, dependencies: List[str] = None) -> DAGNode:
        task_id = str(uuid.uuid4())
        node = DAGNode(task_id, name, func, dependencies)
        self.nodes[task_id] = node
        return node

    def get_node(self, task_id: str) -> Optional[DAGNode]:
        return self.nodes.get(task_id)

    def get_dependencies(self, task_id: str) -> List[str]:
        node = self.nodes.get(task_id)
        return node.dependencies if node else []

    def get_dependents(self, task_id: str) -> List[str]:
        return [nid for nid, n in self.nodes.items() if task_id in n.dependencies]

    def validate(self):
        for task_id, node in self.nodes.items():
            for dep in node.dependencies:
                if dep not in self.nodes:
                    raise ValueError(f"{task_id} depends on unknown task {dep}")
        # this call raises on a cycle, so it doubles as the cycle check —
        # no separate DFS needed, one algorithm doing the job
        KahnTopologicalSort.sort(self)


from app.dag.topological_sort import KahnTopologicalSort  # noqa: E402  (avoids circular import at module load)
