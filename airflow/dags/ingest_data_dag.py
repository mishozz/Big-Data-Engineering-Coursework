from datetime import datetime, timedelta
from airflow.decorators import dag, task
from utils.mongodb_utils import get_mongo_connection
import pandas as pd
import logging
import os

# Setup logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False,
}

@dag(
    dag_id='csv_ingestion_pipeline',
    default_args=default_args,
    description='Ingest CSV data, process it, and store in MongoDB',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['csv', 'mongodb', 'ml'],
)
def salary_pipeline():

    @task()
    def read_csv_file():
        CSV_FILE_PATH = '/opt/airflow/data/Salary_Data.csv'
        if not os.path.exists(CSV_FILE_PATH):
            error_msg = f"CSV file not found at: {CSV_FILE_PATH}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

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

        return df.to_json(orient='records')

    @task()
    def preprocess_data(raw_data_json: str):
        """
        Preprocess the salary data.
        """
        logger.info("Starting salary data preprocessing")

        df = pd.read_json(raw_data_json)
        
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
        return processed_data

    @task()
    def analyze_data(processed_data_json: str):
        logger.info("Starting salary data analysis")
        df = pd.read_json(processed_data_json)

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
        
        return analysis_results

    @task()
    def train_salary_models(processed_data_json: str):
        logger.info("Starting simplified ML model training")
        from sklearn.model_selection import train_test_split
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import mean_squared_error, r2_score
        import mlflow
        import mlflow.sklearn
        import numpy as np
        logger.info("gathering data for model training")
        df = pd.read_json(processed_data_json)

        logger.info(f"Training simplified ML model on {len(df)} salary records")

        feature_cols = ['Age', 'Years_of_Experience'] 
        target_col = 'Salary'

        X = df[feature_cols]
        y = df[target_col]

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
        del df

        model = LinearRegression()
        os.environ["GIT_PYTHON_REFRESH"] = "quiet"
        logger.info("Starting MLflow tracking")
        mlflow.set_tracking_uri("http://mlflow:5500")
        logger.info("MLflow tracking URI set")
        mlflow.set_experiment("Salary_Prediction_Experiment")
        with mlflow.start_run(run_name="LinearRegressionSalaryModel"):
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)

            # Metrics
            mse = mean_squared_error(y_test, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_test, y_pred)

            logger.info(f"Linear Regression - RMSE: {rmse:.2f}, R²: {r2:.2f}")

            # Log parameters
            mlflow.log_param("model_type", "LinearRegression")
            mlflow.log_param("features", feature_cols)
            mlflow.log_param("test_size", 0.25)

            # Log metrics
            mlflow.log_metric("mse", mse)
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("r2_score", r2)
            input_example = pd.DataFrame({
                'Age': [30],
                'Years_of_Experience': [5]
            })
            # Log model
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path="linear_regression_model",
                input_example=input_example
            )

        return "Model training completed and logged to MLflow"

    @task()
    def store_to_mongodb(processed_data_json: str, analysis_results: dict):
        logger.info("Starting MongoDB storage task for salary data")
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

    # Task dependencies using TaskFlow API
    raw_data = read_csv_file()
    processed_data = preprocess_data(raw_data)
    analysis_results = analyze_data(processed_data)
    _ = train_salary_models(processed_data)
    store_to_mongodb(processed_data, analysis_results)

# Instantiate DAG
dag = salary_pipeline()

