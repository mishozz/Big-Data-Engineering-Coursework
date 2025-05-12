from datetime import datetime, timedelta
from airflow.decorators import dag, task
from utils.gcp_utils import update_google_sheet, authenticate_google_sheets
from utils.mongodb_utils import fetch_mongodb_data, get_mongo_connection
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
    'schedule_interval': '@hourly',
}

@dag(
    dag_id='sync_mongo_with_gcp_pipeline',
    default_args=default_args,
    description='Sync MongoDB data with GCP',
    start_date=datetime(2023, 10, 1)
)
def sync_mongo_with_gcp_pipeline():
    """
    DAG to sync MongoDB data with Google Sheets.
    """

    @task()
    def fetch_mongo_data():
        logger.info("Fetching data from MongoDB")
        conn = get_mongo_connection()
        mongo_data = fetch_mongodb_data(conn=conn, db_name='salary_analytics', collection_name='salary_records')
        logger.info(f"Fetched {len(mongo_data)} records from MongoDB")
        conn.close()
        return mongo_data

    @task()
    def fetch_google_sheet_data():
        logger.info("Authenticating with Google Sheets API")
        gcp = authenticate_google_sheets(GCP_CREDENTIALS_PATH)
        logger.info(f"Reading data from spreadsheet ID: {SPREADSHEET_ID}")
        spreadsheet = gcp.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.sheet1
        values = worksheet.get_all_values()
        if not values:
            logger.warning("No data found in the spreadsheet")
            return []
        headers = values[0]
        spreadsheet_data = [
            {headers[j]: row[j] if j < len(row) else '' for j in range(len(headers))}
            for row in values[1:]
        ]
        logger.info(f"Retrieved {len(spreadsheet_data)} records from spreadsheet")
        return spreadsheet_data

    @task()
    def compare_data(mongo_data, spreadsheet_data):
        logger.info("Comparing MongoDB data with spreadsheet data")
        composite_key_fields = ['Age', 'Gender', 'Job_Title', 'Salary', 'processed_timestamp']

        def get_composite_key(record):
            return '|'.join(str(record.get(field, '')) for field in composite_key_fields)

        sheet_data_by_key = {get_composite_key(row): row for row in spreadsheet_data}
        differences = [
            mongo_record for mongo_record in mongo_data
            if get_composite_key(mongo_record) not in sheet_data_by_key
        ]
        logger.info(f"Found {len(differences)} new or modified records in MongoDB")
        return differences

    @task()
    def update_google_sheet_task(differences):
        if differences:
            logger.info(f"Updating Google Sheet with {len(differences)} differences")
            gcp = authenticate_google_sheets(GCP_CREDENTIALS_PATH)
            update_google_sheet(gcp, differences, SPREADSHEET_ID)
            logger.info("Google Sheet updated successfully")
        else:
            logger.info("No differences found. No update needed.")

    mongo_data = fetch_mongo_data()
    spreadsheet_data = fetch_google_sheet_data()
    differences = compare_data(mongo_data, spreadsheet_data)
    update_google_sheet_task(differences)

sync_mongo_with_gcp_pipeline()
