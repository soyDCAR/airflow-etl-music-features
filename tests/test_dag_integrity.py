"""DAG integrity tests — validan estructura sin ejecutar tareas ni servicios."""
import pytest
from airflow.models import DagBag

DAG_ID = "extract_fma_features"
EXPECTED_TASKS = {"download_fma_sample", "extract_features", "load_to_postgres", "run_dbt_transforms"}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture(scope="module")
def fma_dag(dagbag: DagBag):
    dag = dagbag.get_dag(DAG_ID)
    assert dag is not None, f"DAG '{DAG_ID}' no encontrado"
    return dag


def test_dagbag_has_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"


def test_dag_has_exactly_four_tasks(fma_dag) -> None:
    assert len(fma_dag.tasks) == 4


def test_task_ids_match_expected(fma_dag) -> None:
    assert {t.task_id for t in fma_dag.tasks} == EXPECTED_TASKS


def test_task_dependency_order(fma_dag) -> None:
    download = fma_dag.get_task("download_fma_sample")
    extract  = fma_dag.get_task("extract_features")
    load     = fma_dag.get_task("load_to_postgres")
    dbt      = fma_dag.get_task("run_dbt_transforms")
    assert "extract_features"   in {t.task_id for t in download.downstream_list}
    assert "load_to_postgres"   in {t.task_id for t in extract.downstream_list}
    assert "run_dbt_transforms" in {t.task_id for t in load.downstream_list}
    assert dbt.downstream_list == []


def test_dag_has_no_cycles(fma_dag) -> None:
    assert fma_dag.topological_sort() is not None


def test_dag_schedule_is_daily(fma_dag) -> None:
    assert str(fma_dag.schedule_interval) == "@daily"


def test_dag_catchup_is_disabled(fma_dag) -> None:
    assert fma_dag.catchup is False


def test_dag_tags_include_fma(fma_dag) -> None:
    assert "fma" in fma_dag.tags
