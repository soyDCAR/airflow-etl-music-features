"""Tests de retries, SLA y callbacks — sin ejecutar tareas."""

import pytest
from airflow.models import DagBag

DAG_ID = "extract_fma_features"


@pytest.fixture(scope="module")
def fma_dag():
    bag = DagBag(dag_folder="dags/", include_examples=False)
    return bag.get_dag(DAG_ID)


def test_dag_has_sla_miss_callback(fma_dag) -> None:
    assert fma_dag.sla_miss_callback is not None


def test_all_tasks_have_sla(fma_dag) -> None:
    for task in fma_dag.tasks:
        assert task.sla is not None, f"Tarea '{task.task_id}' sin SLA"


def test_all_tasks_have_failure_callback(fma_dag) -> None:
    for task in fma_dag.tasks:
        assert (
            task.on_failure_callback is not None
        ), f"Tarea '{task.task_id}' sin on_failure_callback"


def test_all_tasks_use_exponential_backoff(fma_dag) -> None:
    for task in fma_dag.tasks:
        assert task.retry_exponential_backoff is True


def test_all_tasks_have_at_least_two_retries(fma_dag) -> None:
    for task in fma_dag.tasks:
        assert task.retries >= 2


def test_sla_miss_callback_is_notify_function(fma_dag) -> None:
    assert fma_dag.sla_miss_callback.__name__ == "notify_sla_miss"


def test_failure_callback_is_notify_function(fma_dag) -> None:
    for task in fma_dag.tasks:
        assert task.on_failure_callback.__name__ == "notify_failure"
