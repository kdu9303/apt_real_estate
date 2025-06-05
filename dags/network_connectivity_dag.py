from airflow.decorators import dag, task
from pendulum import datetime
import http.client
import logging

TRINO_HOST = "192.168.0.7"
TRINO_PORT = 8085

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["network", "connectivity", "trino"],
    default_args={"owner": "infra", "retries": 1},
    doc_md="""
    ### Trino 네트워크 연결 테스트 DAG
    Airflow 컨테이너에서 Trino 컨테이너로 네트워크 연결이 가능한지 테스트합니다.
    """
)
def network_connectivity_test():
    @task()
    def check_trino_connectivity():
        logger = logging.getLogger("airflow.task")
        try:
            conn = http.client.HTTPConnection(TRINO_HOST, TRINO_PORT, timeout=5)
            conn.request("GET", "/")
            response = conn.getresponse()
            logger.info(f"Trino 연결 성공! 상태코드: {response.status}")
            print(f"Trino 연결 성공! 상태코드: {response.status}")
            conn.close()
        except Exception as e:
            logger.error(f"Trino 연결 실패: {e}")
            print(f"Trino 연결 실패: {e}")
            raise

    check_trino_connectivity()

network_connectivity_test()
