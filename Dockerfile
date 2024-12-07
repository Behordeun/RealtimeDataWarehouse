FROM apache/airflow:2.10.3
LABEL authors="muhammad"

# Switch to the airflow user
USER airflow

# Set environment variable for Airflow home
ENV AIRFLOW_HOME=/opt/airflow

# Copy the requirements file to the container
COPY requirements.txt /requirements.txt

# Install dependencies from requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# Set default entrypoint
ENTRYPOINT ["/entrypoint"]
