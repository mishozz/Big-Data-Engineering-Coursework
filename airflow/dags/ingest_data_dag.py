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

CSV_FILE_PATH = '/opt/airflow/data/Salary_Data.csv'

def read_csv_file(**kwargs):
    if not os.path.exists(CSV_FILE_PATH):
        error_msg = f"CSV file not found at: {CSV_FILE_PATH}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        columns = [
            "Age", "Gender", "Education_Level", 
            "Job_Title", "Years_of_Experience", "Salary"
        ]
        
        df = pd.read_csv(CSV_FILE_PATH, names=columns, header=None)

        if df.iloc[0, 0] == "Age" and df.iloc[0, 5] == "Salary":
            df = df.iloc[1:]
            logger.info("Removed header row from CSV")
 
        df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
        df["Years_of_Experience"] = pd.to_numeric(df["Years_of_Experience"], errors="coerce")
        df["Salary"] = pd.to_numeric(df["Salary"], errors="coerce")
        
        logger.info(f"Successfully read salary CSV with {len(df)} rows")
        logger.info(f"Columns: {', '.join(df.columns)}")
        
        # Push the DataFrame to the next task via XCom
        kwargs['ti'].xcom_push(key='raw_salary_data', value=df.to_json(orient='records'))
        return "Salary CSV file read successfully"
    except Exception as e:
        error_msg = f"Unexpected error reading CSV file: {str(e)}"
        logger.error(error_msg)
        raise

def preprocess_data(**kwargs):
    """
    Preprocess the salary data.
    """
    logger.info("Starting salary data preprocessing")
    try:
        ti = kwargs['ti']
        json_data = ti.xcom_pull(task_ids='read_csv_file', key='raw_salary_data')
        if json_data is None:
            logger.error("No data found in XCom for task 'read_csv_file' with key 'raw_salary_data'")
            raise ValueError("No data found in XCom")
        df = pd.read_json(json_data)
        
        logger.info(f"Preprocessing {len(df)} salary records")
        
        df = df.fillna({
            'Age': df['Age'].median(),
            'Years_of_Experience': df['Years_of_Experience'].median(),
            'Salary': df['Salary'].median(),
            'Gender': 'Unknown',
            'Education_Level': 'Unknown',
            'Job_Title': 'Unknown'
        })

        df_deduped = df.drop_duplicates()
        if len(df_deduped) < len(df):
            logger.info(f"Removed {len(df) - len(df_deduped)} duplicate salary records")
        df = df_deduped

        salary_ranges = [0, 50000, 75000, 100000, 150000, float('inf')]
        salary_labels = ['Entry', 'Junior', 'Mid', 'Senior', 'Executive']
        df['Salary_Band'] = pd.cut(df['Salary'], bins=salary_ranges, labels=salary_labels)
        
        # Create experience bands
        exp_ranges = [0, 2, 5, 10, 15, float('inf')]
        exp_labels = ['Entry', 'Junior', 'Mid', 'Senior', 'Expert']
        df['Experience_Level'] = pd.cut(df['Years_of_Experience'], bins=exp_ranges, labels=exp_labels)
        df['Salary_per_Year_Experience'] = df['Salary'] / df['Years_of_Experience'].replace(0, 1)
        df['Education_Level'] = df['Education_Level'].str.replace("'s", "").str.strip() 
        df['processed_timestamp'] = datetime.now().isoformat()
        
        processed_data = df.to_json(orient='records')
        kwargs['ti'].xcom_push(key='processed_salary_data', value=processed_data)
        logger.info(f"Salary data preprocessing completed. Processed {len(df)} records.")
        
        return "Salary data preprocessing completed successfully"
    
    except Exception as e:
        error_msg = f"Error during salary data preprocessing: {str(e)}"
        logger.error(error_msg)
        raise

def analyze_data(**kwargs):
    """
    Perform analysis on the salary data.
    """
    logger.info("Starting salary data analysis")
    
    try:
        # Get processed data from previous task
        ti = kwargs['ti']
        processed_data_json = ti.xcom_pull(task_ids='preprocess_data', key='processed_salary_data')
        df = pd.read_json(processed_data_json)
        
        # Analysis results dictionary
        analysis_results = {}
        
        # 1. Gender-based salary analysis
        gender_analysis = df.groupby('Gender')['Salary'].agg(['mean', 'median', 'min', 'max', 'count']).reset_index()
        analysis_results['gender_salary_gap'] = gender_analysis.to_dict('records')
        
        # 2. Education level impact on salary
        education_analysis = df.groupby('Education_Level')['Salary'].agg(['mean', 'median', 'min', 'max', 'count']).reset_index()
        analysis_results['education_salary_impact'] = education_analysis.to_dict('records')
        
        # 3. Experience vs. salary correlation
        analysis_results['experience_salary_correlation'] = df[['Years_of_Experience', 'Salary']].corr().iloc[0, 1]
        
        # 4. Top 5 highest paying job titles
        top_jobs = df.groupby('Job_Title')['Salary'].mean().nlargest(5).reset_index()
        analysis_results['top_paying_jobs'] = top_jobs.to_dict('records')
        
        # 5. Age group analysis
        df['Age_Group'] = pd.cut(df['Age'], bins=[20, 30, 40, 50, 60, 100], labels=['20s', '30s', '40s', '50s', '60+'])
        age_analysis = df.groupby('Age_Group')['Salary'].agg(['mean', 'median', 'count']).reset_index()
        analysis_results['age_salary_analysis'] = age_analysis.to_dict('records')
        
        kwargs['ti'].xcom_push(key='salary_analysis_results', value=analysis_results)
        return "Salary data analysis completed successfully"
    
    except Exception as e:
        error_msg = f"Error during salary data analysis: {str(e)}"
        logger.error(error_msg)
        raise

def store_to_mongodb(**kwargs):
    """
    Store processed salary data and analysis results to MongoDB.
    """
    logger.info("Starting MongoDB storage task for salary data")
    
    try:
        ti = kwargs['ti']
        processed_data_json = ti.xcom_pull(task_ids='preprocess_data', key='processed_salary_data')
        analysis_results = ti.xcom_pull(task_ids='analyze_data', key='salary_analysis_results')
        
        data_list = pd.read_json(processed_data_json).to_dict('records')
        
        logger.info(f"Preparing to store {len(data_list)} salary records to MongoDB")

        conn = None
        try:
            conn = get_mongo_connection()
            db = conn['salary_analytics']

            records_collection = db['salary_records']
            timestamp = datetime.now().isoformat()
            
            for record in data_list:
                record['ingestion_timestamp'] = timestamp
            
            if data_list:
                result = records_collection.insert_many(data_list)
                logger.info(f"Successfully inserted {len(result.inserted_ids)} salary documents into MongoDB")
            else:
                logger.warning("No salary data to insert into MongoDB")

            analysis_collection = db['salary_analysis']
            analysis_data = {
                'timestamp': timestamp,
                'run_id': kwargs.get('run_id', 'unknown'),
                'results': analysis_results
            }
            analysis_collection.insert_one(analysis_data)
            logger.info("Successfully stored analysis results in MongoDB")
            
        except Exception as e:
            logger.error(f"Error storing salary data to MongoDB: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
        
        return "Salary data successfully stored in MongoDB"
    
    except Exception as e:
        error_msg = f"Failed to store salary data in MongoDB: {str(e)}"
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

task_analyse_data = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

task_store_mongodb = PythonOperator(
    task_id='store_to_mongodb',
    python_callable=store_to_mongodb,
    dag=dag,
)

task_read_csv >> task_preprocess >> task_analyse_data >> task_store_mongodb
