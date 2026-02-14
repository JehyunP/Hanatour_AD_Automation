from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import pendulum
KST = pendulum.timezone("Asia/Seoul")

def _to_kst(dt):
    if not dt:
        return "-"
    return dt.in_timezone(KST).strftime("%Y-%m-%d %H:%M:%S %Z")

SLACK_CONN_ID = "slack_webhook_default"
MAX_CHARS = 3500

def _safe_clip(text: str) -> str:
    return text if len(text) <= MAX_CHARS else text[:MAX_CHARS] + "\n...(clipped)"

def _send(text: str) -> None:
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    hook.send(text=text)



def slack_success_alert(context) -> None:
    ti = context["ti"]
    dag = context["dag"]
    dr = context.get("dag_run")
    run_id = getattr(dr, "run_id", "")
    logical_dt = context.get("logical_date")

    msg = f"""✅ *Task Success*
DAG: `{dag.dag_id}`
Task: `{ti.task_id}`
Run: `{run_id}`
'시간: `{_to_kst(logical_dt)}`
"""
    _send(_safe_clip(msg))




def slack_failure_alert(context) -> None:
    ti = context["ti"]
    dag = context["dag"]
    dr = context.get("dag_run")
    run_id = getattr(dr, "run_id", "")
    exception = context.get("exception")

    try:
        payload = ti.xcom_pull(key="payload", task_ids=ti.task_id)
    except Exception:
        payload = None

    logical_dt = context.get("logical_date")

    msg = f"""🚨 *Task Failed*
DAG: `{dag.dag_id}`
Task: `{ti.task_id}`
Run: `{run_id}`
'시간: `{_to_kst(logical_dt)}`
Log: {ti.log_url}

Exception:
```{repr(exception)}```

Payload:
```{payload}```
"""
    _send(_safe_clip(msg))
