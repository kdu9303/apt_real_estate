from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from datetime import datetime


def send_failure_alert(context):
    """
    DAG 또는 Task 실패 시 상세한 보고서 형식의 Slack 알람을 전송합니다.
    
    Args:
        context: Airflow callback context containing task and DAG information
    """
    hook = SlackWebhookHook(slack_webhook_conn_id="conn_slack_airflow_bot")
    
    # Context에서 필요한 정보 추출
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    dag = context.get('dag')
    exception = context.get('exception')
    execution_date = context.get('execution_date')
    
    # 실패 유형 결정
    failure_type = "Task" if task_instance else "DAG"
    
    # 기본 정보 수집
    dag_id = dag.dag_id if dag else "Unknown"
    task_id = task_instance.task_id if task_instance else "N/A"
    run_id = dag_run.run_id if dag_run else "Unknown"
    
    # 실행 시간 계산
    start_date = task_instance.start_date if task_instance and task_instance.start_date else execution_date
    end_date = task_instance.end_date if task_instance and task_instance.end_date else datetime.now()
    duration = str(end_date - start_date) if start_date and end_date else "Unknown"
    
    # 로그 URL 생성 (Airflow 웹서버 기본 URL 사용)
    log_url = f"http://localhost:8080/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}" if task_instance else f"http://localhost:8080/graph?dag_id={dag_id}&execution_date={execution_date}"
    
    # 에러 메시지 추출
    error_message = str(exception) if exception else "No specific error message available"
    
    # 재시도 정보
    try_number = task_instance.try_number if task_instance else 0
    max_tries = task_instance.max_tries if task_instance else 0
    
    # 상세 정보 텍스트 구성 (참고 블로그 스타일 적용)
    detail_text = (
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run ID*: `{run_id}`\n"
        f"*실행 시간*: `{duration}`\n"
        f"*재시도*: `{try_number}/{max_tries}`\n"
        f"*실패 시간*: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"*환경*: `Production`"
    )
    
    # Task 상세 정보가 있는 경우 추가
    if task_instance:
        operator_name = task_instance.operator if hasattr(task_instance, 'operator') else 'Unknown'
        pool_name = task_instance.pool if hasattr(task_instance, 'pool') else 'default_pool'
        queue_name = task_instance.queue if hasattr(task_instance, 'queue') else 'default'
        
        detail_text += (
            f"\n*Operator*: `{operator_name}`\n"
            f"*Pool*: `{pool_name}`\n"
            f"*Queue*: `{queue_name}`"
        )
    
    # 에러 메시지 섹션
    error_text = f"*에러 상세*:\n```{error_message[:800]}{'...' if len(error_message) > 800 else ''}```"
    
    # Slack attachments 형식으로 메시지 구성 (참고 블로그 스타일)
    attachments = [
        {
            "mrkdwn_in": ["text", "pretext"],
            "color": "danger",
            "pretext": f"🚨 *Airflow {failure_type} 실패 알림*",
            "title": f"실패한 {failure_type}: {dag_id}",
            "title_link": f"http://localhost:8080/graph?dag_id={dag_id}",
            "text": detail_text,
            "fields": [
                {
                    "title": "에러 정보",
                    "value": error_text,
                    "short": False
                }
            ],
            "actions": [
                {
                    "type": "button",
                    "name": "view_log",
                    "text": "🔍 로그 확인",
                    "url": log_url,
                    "style": "danger"
                },
                {
                    "type": "button",
                    "name": "view_dag",
                    "text": "📊 DAG 보기",
                    "url": f"http://localhost:8080/graph?dag_id={dag_id}",
                    "style": "primary"
                }
            ],
            "footer": "Airflow Alert System",
            "footer_icon": "https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png",
            "ts": int(datetime.now().timestamp())
        }
    ]
    
    # Slack 메시지 전송
    try:
        hook.send(
            text=f"🚨 Airflow {failure_type} 실패: {dag_id} - {task_id}",
            attachments=attachments,
            channel="#airflow-alerts"
        )
    except Exception as e:
        # 로깅을 위해 기본 메시지라도 전송 시도
        fallback_message = f"🚨 Airflow {failure_type} 실패\nDAG: {dag_id}\nTask: {task_id}\nError: {error_message[:200]}\nTime: {datetime.now()}"
        hook.send(
            text=fallback_message,
            channel="#airflow-alerts"
        )
        print(f"Slack 상세 알림 전송 실패, 기본 메시지로 대체: {e}")


def send_success_alert(context):
    """
    DAG 또는 Task 성공 시 깔끔한 성공 알림을 전송합니다.
    
    Args:
        context: Airflow callback context containing task and DAG information
    """
    hook = SlackWebhookHook(slack_webhook_conn_id="conn_slack_airflow_bot")
    
    # Context에서 필요한 정보 추출
    task_instance = context.get('task_instance')
    dag_run = context.get('dag_run')
    dag = context.get('dag')
    execution_date = context.get('execution_date')
    
    # 성공 유형 결정
    success_type = "Task" if task_instance else "DAG"
    
    # 기본 정보 수집
    dag_id = dag.dag_id if dag else "Unknown"
    task_id = task_instance.task_id if task_instance else "N/A"
    run_id = dag_run.run_id if dag_run else "Unknown"
    
    # 실행 시간 계산
    start_date = task_instance.start_date if task_instance and task_instance.start_date else execution_date
    end_date = task_instance.end_date if task_instance and task_instance.end_date else datetime.now()
    duration = str(end_date - start_date) if start_date and end_date else "Unknown"
    
    # 성공 정보 텍스트 구성 (참고 블로그 스타일 적용)
    success_text = (
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*Run ID*: `{run_id}`\n"
        f"*실행 시간*: `{duration}`\n"
        f"*완료 시간*: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"*환경*: `Production`"
    )
    
    # Slack attachments 형식으로 성공 메시지 구성
    attachments = [
        {
            "mrkdwn_in": ["text", "pretext"],
            "color": "good",
            "pretext": f"✅ *Airflow {success_type} 성공 알림*",
            "title": f"성공한 {success_type}: {dag_id}",
            "title_link": f"http://localhost:8080/graph?dag_id={dag_id}",
            "text": success_text,
            "actions": [
                {
                    "type": "button",
                    "name": "view_dag",
                    "text": "📊 DAG 보기",
                    "url": f"http://localhost:8080/graph?dag_id={dag_id}",
                    "style": "primary"
                }
            ],
            "footer": "Airflow Alert System",
            "footer_icon": "https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png",
            "ts": int(datetime.now().timestamp())
        }
    ]
    
    try:
        hook.send(
            text=f"✅ Airflow {success_type} 성공: {dag_id} - {task_id}",
            attachments=attachments,
            channel="#airflow-success"
        )
    except Exception as e:
        # 기본 메시지로 대체
        fallback_message = f"✅ Airflow {success_type} 성공: {dag_id} - {task_id} at {datetime.now()}"
        hook.send(
            text=fallback_message,
            channel="#airflow-success"
        )
        print(f"Slack 성공 알림 전송 실패, 기본 메시지로 대체: {e}")


def send_retry_alert(context):
    """
    Task 재시도 시 간단한 재시도 알림을 전송합니다.
    
    Args:
        context: Airflow callback context containing task and DAG information
    """
    hook = SlackWebhookHook(slack_webhook_conn_id="conn_slack_airflow_bot")
    
    # Context에서 필요한 정보 추출
    task_instance = context.get('task_instance')
    dag = context.get('dag')
    exception = context.get('exception')
    
    # 기본 정보 수집
    dag_id = dag.dag_id if dag else "Unknown"
    task_id = task_instance.task_id if task_instance else "N/A"
    try_number = task_instance.try_number if task_instance else 0
    max_tries = task_instance.max_tries if task_instance else 0
    
    # 재시도 정보 텍스트 구성
    retry_text = (
        f"*DAG*: `{dag_id}`\n"
        f"*Task*: `{task_id}`\n"
        f"*재시도*: `{try_number}/{max_tries}`\n"
        f"*재시도 시간*: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n"
        f"*에러*: `{str(exception)[:200] if exception else 'Unknown error'}...`"
    )
    
    # Slack attachments 형식으로 재시도 메시지 구성
    attachments = [
        {
            "mrkdwn_in": ["text", "pretext"],
            "color": "warning",
            "pretext": "⚠️ *Airflow Task 재시도 알림*",
            "title": f"재시도 중인 Task: {task_id}",
            "text": retry_text,
            "footer": "Airflow Alert System",
            "footer_icon": "https://airflow.apache.org/docs/apache-airflow/stable/_images/pin_large.png",
            "ts": int(datetime.now().timestamp())
        }
    ]
    
    try:
        hook.send(
            text=f"⚠️ Airflow Task 재시도: {dag_id} - {task_id} ({try_number}/{max_tries})",
            attachments=attachments,
            channel="#airflow-alerts"
        )
    except Exception as e:
        print(f"Slack 재시도 알림 전송 실패: {e}")
