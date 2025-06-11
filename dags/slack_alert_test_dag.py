from datetime import datetime
from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
from src.utils import send_failure_alert, send_success_alert


# 사용 예제 DAG
@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    on_failure_callback=send_failure_alert,  # DAG 레벨 실패 콜백
    tags=["example", "slack", "notifications"],
    doc_md="Slack 알림 기능을 테스트하는 예제 DAG"
)
def slack_notification_example():
    
    # 실패하는 태스크 (테스트용)
    failure_task = PythonOperator(
        task_id="failure_task",
        python_callable=lambda: 1/0,  # 의도적으로 에러 발생
        on_failure_callback=send_failure_alert,  # 태스크 레벨 실패 콜백
        retries=0,  # 재시도 1회
    )
    
    failure_task

slack_notification_example()