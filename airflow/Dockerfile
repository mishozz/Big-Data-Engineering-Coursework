# filepath: /Users/I538212/Documents/git/Big-Data-Enginiering/coursework2/airflow/Dockerfile
FROM apache/airflow:3.0.0

# Copy requirements.txt into the container
ADD requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
