from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import logging
from datetime import datetime
import pendulum

from configs.links import COOKIES_URL_SEJU, TRAVEL_AREAS_URL
from utils.s3_util import S3Util
from utils.session import Session

local_tz = pendulum.timezone("Asia/Seoul")

def get_travel_areas():
    session = Session(COOKIES_URL_SEJU)
    data = session.get_response(TRAVEL_AREAS_URL)

    if data is None:
        logging.error("No data received from API")
        raise ValueError("Data collection failed")

    bronze_bucket = os.getenv('BRONZE_BUCKET')
    filename = os.getenv('AREA_FILENAME')
    s3 = S3Util(bronze_bucket)
    s3.upload_json(data, filename)
    logging.info(f"Successfully processed {filename}")


# DAG
with DAG(
    dag_id='Fetch_travel_areas_dag',
    start_date=datetime(2026,1,1, tzinfo=local_tz),
    schedule_interval="0 0 1 1,7 *",
    catchup=False,
    max_active_runs=1,
    tags=['Travel_areas', 'To_S3', 'JSON']
) as dag:
    fetch = PythonOperator(
        task_id = 'get_travel_areas',
        python_callable = get_travel_areas,
        retries=3,
        retry_delay=pendulum.duration(minutes=5)
    )

    fetch
