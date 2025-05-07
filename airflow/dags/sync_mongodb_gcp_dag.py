from datetime import datetime, timedelta
from airflow import DAG
import sys
from pprint import pprint
pprint(sys.path)
sys.path.append("/opt/airflow/plugins")
from airflow.operators.python import PythonOperator
from utils.gcp_utils import update_google_sheet
from utils.gcp_utils import authenticate_google_sheets
from utils.mongodb_utils import fetch_mongodb_data
from utils.mongodb_utils import get_mongo_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GCP_CREDENTIALS_PATH = '/opt/airflow/secrets/gcp_credentials.json'
SPREADSHEET_ID = '1Y5rBfMVLqYtQ81_IGWi3Cl-NDoqCQFQZKp8DiHIUVus'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'sync_mongo_with_gcp_pipeline',
    default_args=default_args,
    description='Sync MongoDB data with GCP',
    start_date=datetime(2023, 10, 1),
    catchup=False
)

def task_sync_mongo_to_gcp():
    limit = 100
    conn = get_mongo_connection()
    data = fetch_mongodb_data(conn=conn, limit=limit)
    logger.info("authenticating with Google Sheets API")
    gcp = authenticate_google_sheets(GCP_CREDENTIALS_PATH)
    logger.info("Authenticated with Google Sheets API")
    update_google_sheet(gcp, data, SPREADSHEET_ID)

sync_task = PythonOperator(
    task_id='sync_mongo_to_gcp',
    python_callable=task_sync_mongo_to_gcp,
    dag=dag,
)

sync_task
