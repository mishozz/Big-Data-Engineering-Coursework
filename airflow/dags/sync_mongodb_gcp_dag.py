from datetime import datetime, timedelta
from airflow import DAG
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
    conn = get_mongo_connection()
    mongo_data = fetch_mongodb_data(conn=conn, db_name='salary_analytics', collection_name='salary_records')
    logger.info("authenticating with Google Sheets API")
    gcp = authenticate_google_sheets(GCP_CREDENTIALS_PATH)
        # Get current data from Google Sheet
    logger.info("Fetching current data from Google Sheet")
    spreadsheet_data = get_spreadsheet_data(gcp, SPREADSHEET_ID)
    logger.info("Comparing MongoDB data with Google Sheet data")
    differences = find_data_differences(mongo_data, spreadsheet_data)
    if differences:
        logger.info(f"Found {len(differences)} differences to update in Google Sheet")
        update_google_sheet(gcp, differences, SPREADSHEET_ID)
    else:
        logger.info("No differences found. No update needed.")
    conn.close()

def get_spreadsheet_data(gcp, spreadsheet_id):
    logger.info(f"Reading data from spreadsheet ID: {spreadsheet_id}")
    
    spreadsheet = gcp.open_by_key(spreadsheet_id)  # Open the spreadsheet by its ID
    worksheet = spreadsheet.sheet1  # Access the first sheet
    values = worksheet.get_all_values()  # Get all values from the sheet
    if not values:
        logger.warning("No data found in the spreadsheet")
        return []
    # Extract headers from the first row
    headers = values[0]
    
    # Convert remaining rows to list of dictionaries
    spreadsheet_data = []
    for i, row in enumerate(values[1:], start=2):  # Start from 2 as row 1 is headers
        # Handle potential missing values in rows
        padded_row = row + [''] * (len(headers) - len(row))
        row_dict = {headers[j]: padded_row[j] for j in range(len(headers))}
        spreadsheet_data.append(row_dict)
    
    logger.info(f"Retrieved {len(spreadsheet_data)} records from spreadsheet")
    return spreadsheet_data


def find_data_differences(mongo_data, spreadsheet_data):
    logger.info("Comparing MongoDB data with spreadsheet data")
    composite_key_fields = ['Age', 'Gender', 'Job_Title', 'Salary', 'processed_timestamp']

    def get_composite_key(record):
        return '|'.join(str(record.get(field, '')) for field in composite_key_fields)

    sheet_data_by_key = {get_composite_key(row): row for row in spreadsheet_data}
    differences = []
    
    # Check for new or modified records
    for mongo_record in mongo_data:
        mongo_key = get_composite_key(mongo_record)
        if mongo_key not in sheet_data_by_key:
            differences.append(mongo_record)
    
    logger.info(f"Found {len(differences)} new or modified records in MongoDB")
    return differences

sync_task = PythonOperator(
    task_id='sync_mongo_to_gcp',
    python_callable=task_sync_mongo_to_gcp,
    dag=dag,
)

sync_task
