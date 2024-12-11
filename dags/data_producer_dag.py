from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from kafka_operator import KafkaProduceOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import requests

# Default arguments for the DAG
start_date = datetime(2024, 12, 7)
default_args = {
    'owner': 'behordeun',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': start_date,
}

# Function to check if the table exists
def check_table_exists(**kwargs):
    url = "http://pinot-controller:9000/tables/transaction_facts"
    try:
        response = requests.get(url)
        if response.status_code == 200 and response.json():  # Ensure response is non-empty
            print("transaction_facts table already exists.")
            kwargs['ti'].xcom_push(key='table_exists', value=True)
        else:
            print(f"Table check failed or returned empty response: {response.text}")
            kwargs['ti'].xcom_push(key='table_exists', value=False)
    except requests.RequestException as e:
        print(f"Error checking table existence: {e}")
        kwargs['ti'].xcom_push(key='table_exists', value=False)

# Function to decide whether to skip or create the table
def decide_create_or_skip(**kwargs):
    table_exists = kwargs['ti'].xcom_pull(task_ids='check_transaction_facts_table', key='table_exists')
    if table_exists:
        print("Table exists. Proceeding to validation.")
    else:
        print("Table does not exist. Proceeding to table creation.")
    # No need to return a task id; let all downstream tasks run regardless.

# Define the DAG
with DAG(
    dag_id='facts_data_producer',
    default_args=default_args,
    description='A DAG to produce realtime data into Kafka',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Task to check if the table exists
    check_transaction_facts_table = PythonOperator(
        task_id='check_transaction_facts_table',
        python_callable=check_table_exists,
        provide_context=True,
    )

    # Decision task
    decide_task = PythonOperator(
        task_id='decide_task',
        python_callable=decide_create_or_skip,
        provide_context=True,
    )

    # Task to create the Pinot table for transaction_facts (REALTIME)
    create_transaction_facts_table = BashOperator(
        task_id='create_transaction_facts_table',
        bash_command=(
            'curl -X POST -H "Content-Type: application/json" '
            '-d @/opt/airflow/dags/tables/transaction_facts.json '
            '"http://pinot-controller:9000/tables"'
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Always runs unless upstream fails
    )

    # Task to validate the creation of the transaction_facts table
    validate_transaction_facts_table = BashOperator(
        task_id='validate_transaction_facts_table',
        bash_command=(
            'curl -X GET "http://pinot-controller:9000/tables/transaction_facts" '
            '|| { echo "Validation failed: transaction_facts table does not exist."; exit 1; }'
        )
    )

    # Task to generate and send transaction data to the Kafka topic
    produce_transaction_data = KafkaProduceOperator(
        task_id='produce_transaction_data',
        kafka_broker='kafka_broker:9092',
        kafka_topic='transaction_facts',
        num_records=1000,  # Number of records to produce
    )

    # Define dependencies
    check_transaction_facts_table >> decide_task
    decide_task >> create_transaction_facts_table >> validate_transaction_facts_table
    decide_task >> validate_transaction_facts_table  # If skipping creation, go directly to validation
    validate_transaction_facts_table >> produce_transaction_data
