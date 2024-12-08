from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
start_date = datetime(2024, 12, 7)
default_args = {
    'owner': 'behordeun',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date,
}

# Python function to check if the file exists
def check_file_exists(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Required file {file_path} not found.")
    print(f"File {file_path} exists.")

# Define the DAG
with DAG(
    dag_id='dimension_batch_ingestion',
    default_args=default_args,
    description='A DAG to ingest dimension data into Pinot',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Task to check if the customer_dim_large_data.csv file exists
    check_customer_dim_file = PythonOperator(
        task_id='check_customer_dim_file',
        python_callable=check_file_exists,
        op_args=['/opt/airflow/customer_dim_large_data.csv'],
    )

    # Task to create the Pinot table for customer_dim_OFFLINE
    create_customer_dim_table = BashOperator(
        task_id='create_customer_dim_table',
        bash_command=(
            'curl -X POST -H "Content-Type: application/json" '
            '-d @/opt/airflow/dags/tables/customer_dim.json '
            '"http://pinot-controller:9000/tables"'
        )
    )

    # Task to ingest customer_dim_large_data.csv into customer_dim_OFFLINE
    ingest_customer_dim = BashOperator(
        task_id='ingest_customer_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/customer_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=customer_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%2C%22%7D"'
        )
    )

    # Task to ingest account_dim_large_data.csv into account_dim_OFFLINE
    ingest_account_dim = BashOperator(
        task_id='ingest_account_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/account_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%2C%22%7D"'
        )
    )

    # Task to create the Pinot table for branch_dim_OFFLINE
    create_branch_dim_table = BashOperator(
        task_id='create_branch_dim_table',
        bash_command=(
            'curl -X POST -H "Content-Type: application/json" '
            '-d @/opt/airflow/dags/tables/branch_dim.json '
            '"http://pinot-controller:9000/tables"'
        )
    )
    
    # Task to ingest branch_dim_large_data.csv into branch_dim_OFFLINE
    ingest_branch_dim = BashOperator(
        task_id='ingest_branch_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/branch_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=branch_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%2C%22%7D"'
        )
    )

    # Define task dependencies
    check_customer_dim_file >> create_customer_dim_table >> ingest_customer_dim
    ingest_customer_dim >> ingest_account_dim >> ingest_branch_dim
