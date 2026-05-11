"""
Airflow callbacks — failure notification y SLA alerting.

Nunca lanzan excepciones: están envueltos en try/except para que un
Slack caído no pueda ocultar el error original de la tarea.

Uso en default_args / DAG:
    from callbacks import notify_failure, notify_sla_miss
    default_args = {"on_failure_callback": notify_failure, ...}
    @dag(sla_miss_callback=notify_sla_miss, ...)
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.models import DAG, TaskInstance

log = logging.getLogger(__name__)


def notify_failure(context: dict[str, Any]) -> None:
    """Llamado cuando una tarea pasa a estado FAILED."""
    ti: TaskInstance = context["task_instance"]
    exception = context.get("exception", "unknown error")
    message = (
        f":red_circle: *Task failed*\n"
        f"• DAG: `{ti.dag_id}`\n"
        f"• Task: `{ti.task_id}`\n"
        f"• Run: `{context['run_id']}`\n"
        f"• Attempt: `{ti.try_number}` / `{ti.max_tries + 1}`\n"
        f"• Error: `{exception}`"
    )
    log.error("FAILURE | dag=%s task=%s | %s", ti.dag_id, ti.task_id, exception)
    _post_slack(message)


def notify_sla_miss(dag: DAG, task_list: str, blocking_task_list: str, slas: list, blocking_tis: list) -> None:
    """Llamado cuando el scheduler detecta un SLA miss."""
    message = (
        f":alarm_clock: *SLA missed*\n"
        f"• DAG: `{dag.dag_id}`\n"
        f"• Missed: `{task_list}`\n"
        f"• Blocked by: `{blocking_task_list}`"
    )
    log.warning("SLA MISS | dag=%s | tasks=%s", dag.dag_id, task_list)
    _post_slack(message)


def _post_slack(message: str) -> None:
    """POST al webhook de Slack. No-op si SLACK_WEBHOOK_URL no está configurado."""
    try:
        import requests
        from airflow.models import Variable
        webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var="")
        if not webhook_url:
            log.debug("SLACK_WEBHOOK_URL no configurado — notificación omitida")
            return
        resp = requests.post(webhook_url, json={"text": message}, timeout=10)
        resp.raise_for_status()
        log.info("Slack notification enviada (HTTP %s)", resp.status_code)
    except Exception as exc:  # noqa: BLE001
        log.warning("Slack delivery falló (no fatal): %s", exc)
