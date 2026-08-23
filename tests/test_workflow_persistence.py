from app.dag.graph import DAG
from app.state.wal import WriteAheadLog
from app.state.state_manager import StateManager


def make_manager(tmp_path):
    wal = WriteAheadLog(str(tmp_path / "wal.log"))
    return StateManager(str(tmp_path / "state.db"), wal)


def test_dag_roundtrips_through_dict():
    dag = DAG()
    a = dag.add_node("a")
    dag.add_node("b", dependencies=[a.task_id])

    restored = DAG.from_dict(dag.to_dict())

    assert restored.workflow_id == dag.workflow_id
    assert set(restored.nodes) == set(dag.nodes)
    restored.validate()  # should not raise


def test_saved_workflow_is_loaded_back_after_restart(tmp_path):
    sm = make_manager(tmp_path)
    dag = DAG()
    a = dag.add_node("extract")
    dag.add_node("transform", dependencies=[a.task_id])
    sm.save_workflow(dag.workflow_id, dag.to_dict())

    # simulate a process restart against the same db file
    sm2 = make_manager(tmp_path)
    loaded = sm2.load_all_workflows()

    assert dag.workflow_id in loaded
    restored = DAG.from_dict(loaded[dag.workflow_id])
    assert set(restored.nodes) == set(dag.nodes)


def test_saving_workflow_again_overwrites_definition(tmp_path):
    sm = make_manager(tmp_path)
    dag = DAG(workflow_id="wf-1")
    dag.add_node("a")
    sm.save_workflow(dag.workflow_id, dag.to_dict())

    dag.add_node("b")
    sm.save_workflow(dag.workflow_id, dag.to_dict())

    loaded = sm.load_all_workflows()
    assert len(loaded["wf-1"]["nodes"]) == 2
