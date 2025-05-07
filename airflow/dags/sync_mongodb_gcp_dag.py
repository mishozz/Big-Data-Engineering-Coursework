from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from gspread_dataframe import set_with_dataframe
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
import pymongo
import logging
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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

# MongoDB connection parameters
MONGO_CONNECTION = {
    'host': 'mongodb',
    'port': 27017,
    'username': 'mongodb_user',
    'password': 'mongodb_password',
    'database': 'mldata',
    'collection': 'processed_data'
}

GCP_CREDENTIALS_PATH = '/opt/airflow/secrets/gcp_credentials.json'
SPREADSHEET_ID = '1Y5rBfMVLqYtQ81_IGWi3Cl-NDoqCQFQZKp8DiHIUVus'

def get_mongo_connection():
    """
    Establish a connection to MongoDB with retry logic.
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            client = pymongo.MongoClient(
                host=MONGO_CONNECTION['host'],
                port=MONGO_CONNECTION['port'],
                username=MONGO_CONNECTION['username'],
                password=MONGO_CONNECTION['password'],
                serverSelectionTimeoutMS=5000  # 5 second timeout
            )
            
            # Check connection
            client.server_info()
            logger.info("Successfully connected to MongoDB")
            return client
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {str(e)}")
            raise


def fetch_mongodb_data(query=None, limit=None):
    """
    Fetch data from MongoDB based on optional query and limit.
    
    Args:
        query (dict): MongoDB query dictionary
        limit (int): Maximum number of records to retrieve
    
    Returns:
        list: List of documents
    """
    if query is None:
        query = {}
    
    try:
        client = get_mongo_connection()
        db = client[MONGO_CONNECTION['database']]
        collection = db[MONGO_CONNECTION['collection']]

        # Get the latest records
        cursor = collection.find(query)

        # Apply limit if specified
        if limit:
            cursor = cursor.limit(limit)

        # Convert to list and remove MongoDB _id (not JSON serializable)
        results = list(cursor)
        for doc in results:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])

        logger.info(f"Retrieved {len(results)} documents from MongoDB")
        client.close()
        return results
    
    except Exception as e:
        logger.error(f"Error fetching data from MongoDB: {str(e)}")
        raise


def authenticate_google_sheets():
    try:
        if not os.path.exists(GCP_CREDENTIALS_PATH):
            raise FileNotFoundError(f"GCP credentials file not found at: {GCP_CREDENTIALS_PATH}")

        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        
        credentials = Credentials.from_service_account_file(
            GCP_CREDENTIALS_PATH, scopes=scopes
        )
        
        gc = gspread.authorize(credentials)
        logger.info("Successfully authenticated with Google Sheets API")
        return gc
    
    except Exception as e:
        logger.error(f"Error authenticating with Google Sheets: {str(e)}")
        raise


def update_google_sheet(data):
    try:
        if not data:
            logger.warning("No data to update in Google Sheets")
            return True
        
        if not SPREADSHEET_ID:
            raise ValueError("SPREADSHEET_ID environment variable not set")
        
        # Convert list of dicts to DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        # Connect to Google Sheets
        gc = authenticate_google_sheets()
        
        # Open the spreadsheet and the first sheet
        try:
            spreadsheet = gc.open_by_key(SPREADSHEET_ID)
            #worksheet = gc.open("data").sheet1
            worksheet = spreadsheet.sheet1  # Use the first sheet
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet with ID {SPREADSHEET_ID} not found")
            raise
        
        # Check if sheet is empty and needs headers
        print(f"Worksheet row count: {worksheet.row_count}")
        if worksheet.row_count <= 1:  # Only has the header row or is empty
            logger.info("Sheet appears to be empty, will add headers")
            headers = df.columns.tolist()
            worksheet.append_row(headers)
        
        logger.info(f"Worksheet {worksheet.title}" )
        logger.info(f"DF columns: {df.columns.tolist()}")
        logger.info(f"DF shape: {df.shape}")
        
        # Update sheet with dataframe
        set_with_dataframe(
            worksheet,
            df,
            include_index=False,
            include_column_header=False,
            resize=True,
            row=worksheet.row_count
        )
        
        logger.info(f"Successfully updated Google Sheet with {len(df)} rows")
        return True
    
    except Exception as e:
        logger.error(f"Error updating Google Sheet: {str(e)}")
        raise


def task_sync_mongo_to_gcp():
    """
    Main task to sync MongoDB data to Google Sheets.
    """
    # Fetch data from MongoDB
    query = {}  # Define your query here
    limit = 100  # Define your limit here
    data = fetch_mongodb_data(query=query, limit=limit)
    
    # Update Google Sheets with the fetched data
    update_google_sheet(data)

# Define the task in the DAG
sync_task = PythonOperator(
    task_id='sync_mongo_to_gcp',
    python_callable=task_sync_mongo_to_gcp,
    dag=dag,
)

# Set task dependencies if needed
sync_task
