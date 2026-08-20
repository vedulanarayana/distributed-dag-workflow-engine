import pytest
from app.dag.graph import DAG


def test_valid_dag_passes():
    dag = DAG()
    a = dag.add_node("a")
    dag.add_node("b", dependencies=[a.task_id])
    dag.validate()  # should not raise


def test_cycle_is_rejected():
    dag = DAG()
    a = dag.add_node("a")
    b = dag.add_node("b", dependencies=[a.task_id])
    dag.nodes[a.task_id].dependencies.append(b.task_id)  # closes the loop
    with pytest.raises(ValueError, match="cycle"):
        dag.validate()


def test_missing_dependency_is_rejected():
    dag = DAG()
    dag.add_node("a", dependencies=["does-not-exist"])
    with pytest.raises(ValueError, match="unknown task"):
        dag.validate()
