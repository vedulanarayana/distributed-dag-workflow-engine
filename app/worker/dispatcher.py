import asyncio
from app.dag.topological_sort import KahnTopologicalSort


class WorkerDispatcher:

    def __init__(self, state_manager, retry_manager):
        self.state_manager = state_manager
        self.retry_manager = retry_manager

    async def execute_workflow(self, dag, default_executor=None):
        try:
            dag.validate()
        except ValueError as e:
            return {"status": "failed", "reason": str(e)}

        levels = KahnTopologicalSort.get_execution_levels(dag)
        execution_log = []

        for level in levels:
            results = await asyncio.gather(*[
                asyncio.to_thread(self._run_task, task_id, dag, default_executor)
                for task_id in level
            ])
            execution_log.extend(results)

        all_ok = all(r["status"] == "succeeded" for r in execution_log)
        return {
            "workflow_id": dag.workflow_id,
            "status": "succeeded" if all_ok else "failed",
            "total_tasks": len(dag.nodes),
            "levels": len(levels),
            "execution_log": execution_log,
        }

    def _run_task(self, task_id, dag, default_executor):
        node = dag.get_node(task_id)
        func = node.func or default_executor
        self.retry_manager.execute_with_retry(task_id, dag.workflow_id, func, node)
        state = self.state_manager.get_state(task_id)
        return {"task_id": task_id, "name": node.name, "status": state["state"], "retry_count": state["retry_count"]}
