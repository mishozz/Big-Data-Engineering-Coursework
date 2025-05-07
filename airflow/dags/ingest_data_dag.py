from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.mongodb_utils import get_mongo_connection
import pandas as pd
import logging
import os

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
    'csv_ingestion_pipeline',
    default_args=default_args,
    description='Ingest CSV data, process it, and store in MongoDB',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['csv', 'mongodb', 'ml'],
)

CSV_FILE_PATH = '/opt/airflow/data/input.csv'

def read_csv_file(**kwargs):
    """
    Read the CSV file and validate its structure.
    """
    logger.info(f"Attempting to read CSV file from: {CSV_FILE_PATH}")
    
    # Check if file exists
    if not os.path.exists(CSV_FILE_PATH):
        error_msg = f"CSV file not found at: {CSV_FILE_PATH}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        # Read the CSV file
        df = pd.read_csv(CSV_FILE_PATH)
        logger.info(f"Successfully read CSV with {len(df)} rows and columns: {', '.join(df.columns)}")
        
        # Basic validation
        if df.empty:
            raise ValueError("CSV file is empty")
        
        # Push the DataFrame to the next task via XCom
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json(orient='records'))
        return "CSV file read successfully"
    
    except pd.errors.EmptyDataError:
        error_msg = "CSV file is empty"
        logger.error(error_msg)
        raise
    except pd.errors.ParserError as e:
        error_msg = f"Error parsing CSV file: {str(e)}"
        logger.error(error_msg)
        raise
    except Exception as e:
        error_msg = f"Unexpected error reading CSV file: {str(e)}"
        logger.error(error_msg)
        raise

def preprocess_data(**kwargs):
    """
    Preprocess the data from the CSV file.
    """
    logger.info("Starting data preprocessing")
    
    try:
        # Get the DataFrame from the previous task
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='read_csv_file', key='raw_data')
        df = pd.read_json(json_data)
        
        logger.info(f"Preprocessing {len(df)} records")
        
        # Data preprocessing steps
        # 1. Handle missing values
        df = df.fillna({
            'numeric_columns': 0,  # Replace with actual column names
            'categorical_columns': 'unknown'  # Replace with actual column names
        })
        
        # 2. Remove duplicates
        df_deduped = df.drop_duplicates()
        if len(df_deduped) < len(df):
            logger.info(f"Removed {len(df) - len(df_deduped)} duplicate records")
        df = df_deduped
        
        # 3. Feature engineering/transformation examples
        # Convert date strings to datetime objects
        date_columns = df.select_dtypes(include=['object']).columns[df.select_dtypes(include=['object']).apply(lambda x: pd.to_datetime(x, errors='coerce').notna().all())]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col])
            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_day"] = df[col].dt.day
        
        # Pass the processed data to the next task
        processed_data = df.to_json(orient='records')
        kwargs['ti'].xcom_push(key='processed_data', value=processed_data)
        logger.info(f"Data preprocessing completed. Processed {len(df)} records.")
        return "Data preprocessing completed successfully"
    
    except Exception as e:
        error_msg = f"Error during data preprocessing: {str(e)}"
        logger.error(error_msg)
        raise

def store_to_mongodb(**kwargs):
    """
    Store processed data to MongoDB.
    """
    logger.info("Starting MongoDB storage task")
    
    try:
        # Get processed data from previous task
        ti = kwargs['ti']
        processed_data_json = ti.xcom_pull(task_ids='preprocess_data', key='processed_data')
        data_list = pd.read_json(processed_data_json).to_dict('records')
        
        logger.info(f"Preparing to store {len(data_list)} records to MongoDB")

        try:
            conn = get_mongo_connection()
            db = conn['mldata']
            collection = db['processed_data']
            
            timestamp = datetime.now().isoformat()
            for record in data_list:
                record['ingestion_timestamp'] = timestamp
            
            if data_list:
                result = collection.insert_many(data_list)
                logger.info(f"Successfully inserted {len(result.inserted_ids)} documents into MongoDB")
            else:
                logger.warning("No data to insert into MongoDB")
        except Exception as e:
            logger.error(f"Error storing data to MongoDB: {str(e)}")
            raise
        finally:
            conn.close()
        
        return "Data successfully stored in MongoDB"
    
    except Exception as e:
        error_msg = f"Failed to store data in MongoDB: {str(e)}"
        logger.error(error_msg)
        raise

task_read_csv = PythonOperator(
    task_id='read_csv_file',
    python_callable=read_csv_file,
    dag=dag,
)

task_preprocess = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

task_store_mongodb = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=store_to_mongodb,
    dag=dag,
)

task_read_csv >> task_preprocess >> task_store_mongodb
