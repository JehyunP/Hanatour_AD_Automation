import os
import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.s3_util import S3Util
from configs.seju_logic import SejuLogic
from configs.dream_logic import DreamLogic
from utils.apply_logic import LogicProducer
from utils.sftp_controller import SFTPController
from utils.products_collector import ProductsCollector
from configs.links import COOKIES_URL_SEJU, DATA_URL, MAIN_URL_SEJU, LANDING_URL_SEJU, LANDING_URL_DREAM
from utils.slack_alert import slack_success_alert, slack_failure_alert


local_tz = pendulum.timezone("Asia/Seoul")
seju_schedule = os.getenv('SEJU_DATA_DAG_TIME')

bronze_bucket = os.getenv("BRONZE_BUCKET")
silver_bucket = os.getenv("SILVER_BUCKET")
gold_bucket = os.getenv("GOLD_BUCKET")

filename = os.getenv("AREA_FILENAME")
data_prefix_seju = os.getenv('DATA_BASE_PREFIX_SEJU')
final_prefix_seju = os.getenv('DATA_FINAL_PREFIX_SEJU')
seju_map_key = os.getenv('TITLE_MAP_SEJU')
ep_seju = os.getenv('EP_FILENAME_SEJU')

data_prefix_dream = os.getenv('DATA_BASE_PREFIX_DREAM')
final_prefix_dream = os.getenv('DATA_FINAL_PREFIX_DREAM')
dream_map_key = os.getenv('TITLE_MAP_DREAM')
ep_dream = os.getenv('EP_FILENAME_DREAM')

SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = os.getenv('SFTP_PORT')
SFTP_USER = os.getenv('SFTP_USER')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD')
SFTP_REMOTE_PATH = os.getenv('SFTP_REMOTE_PATH')
SFTP_REMOTE_PATH_DREAM = os.getenv('SFTP_REMOTE_PATH_DREAM')

SEJU_ID = os.getenv('SEJU_ID')
DREAM_ID = os.getenv('DREAM_ID')

def get_products_data_seju(**context):
    execution_date = context['logical_date']

    collector = ProductsCollector(
        cookie=COOKIES_URL_SEJU,
        data_url=DATA_URL,
        main_url=MAIN_URL_SEJU,
        landing_url=LANDING_URL_SEJU
    )

    collector.run_pipeline(
        bronze_bucket,
        silver_bucket,
        filename,
        data_prefix_seju,
        execution_date,
        180,
        10,
        8
    )


def update_logic_seju(**context):
    logic = SejuLogic()
    base_url = LANDING_URL_SEJU
    app = LogicProducer(data_prefix_seju, logic, base_url, SEJU_ID)
    s3 = S3Util(silver_bucket)

    df = app.apply_logic(s3)
    mapping_df = s3.read_xlsx(seju_map_key)

    new_df = app.apply_mapping(df, mapping_df)
    final_df = app.apply_drop_duplicates(new_df)

    gold_s3 = S3Util(gold_bucket)

    execution_date = context['logical_date']
    yymmdd = execution_date.in_timezone("Asia/Seoul").strftime("%Y%m%d")
    hhmm = execution_date.in_timezone("Asia/Seoul").strftime("%H%M")

    key = f'{final_prefix_seju}/{yymmdd}/{hhmm}/{ep_seju}'
    gold_s3.upload_parquet(final_df, key)

    with SFTPController(SFTP_HOST, SFTP_USER, SFTP_PASSWORD, port=SFTP_PORT) as sftp:
        sftp.upload_df_tsv(final_df, SFTP_REMOTE_PATH)


def update_logic_dream(**context):
    logic = DreamLogic()
    base_url = LANDING_URL_DREAM
    app = LogicProducer(data_prefix_dream, logic, base_url, DREAM_ID)
    s3 = S3Util(silver_bucket)

    df = app.apply_logic(s3)
    mapping_df = s3.read_xlsx(dream_map_key)

    new_df = app.apply_mapping(df, mapping_df)
    final_df = app.apply_drop_duplicates(new_df)

    gold_s3 = S3Util(gold_bucket)

    execution_date = context['logical_date']
    yymmdd = execution_date.in_timezone("Asia/Seoul").strftime("%Y%m%d")
    hhmm = execution_date.in_timezone("Asia/Seoul").strftime("%H%M")

    key = f'{final_prefix_dream}/{yymmdd}/{hhmm}/{ep_dream}'
    gold_s3.upload_parquet(final_df, key)

    with SFTPController(SFTP_HOST, SFTP_USER, SFTP_PASSWORD, port=SFTP_PORT) as sftp:
        sftp.upload_df_tsv(final_df, SFTP_REMOTE_PATH_DREAM)


# DAG
with DAG(
    dag_id='Fetch_data_and_upload_ep_dag',
    start_date=datetime(2026,1,1, tzinfo=local_tz),
    schedule_interval=seju_schedule,
    catchup=False,
    max_active_runs=1,
    tags=['Products_data', 'To_S3', 'Parquet', 'SFTP'],
    on_success_callback=slack_success_alert,
    on_failure_callback=slack_failure_alert,
) as dag:
    fetch_data_seju = PythonOperator(
        task_id = 'get_products_data_seju',
        python_callable = get_products_data_seju,
    )

    update_seju_logic = PythonOperator(
        task_id = 'update_logic_seju',
        python_callable = update_logic_seju,
    )

    update_dream_logic = PythonOperator(
        task_id = 'update_logic_dream',
        python_callable = update_logic_dream,
    )

    fetch_data_seju >> [update_seju_logic, update_dream_logic]