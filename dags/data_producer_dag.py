from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from kafka_operator import KafkaProduceOperator
from datetime import datetime, timedelta
import os

# Default arguments for the DAG
start_date = datetime(2024, 12, 7)
default_args = {
    'owner': 'behordeun',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': start_date,
}


# Define the DAG
with DAG(
    dag_id='facts_data_producer',
    default_args=default_args,
    description='A DAG to produce realtime data into Kafka',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Task to create the Pinot table for transaction_facts (REALTIME)
    create_transaction_facts_table = BashOperator(
        task_id='create_transaction_facts_table',
        bash_command=(
            'curl -X POST -H "Content-Type: application/json" '
            '-d @/opt/airflow/dags/tables/transaction_facts.json '
            '"http://pinot-controller:9000/tables"'
        )
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
        num_records=1000  # Number of records to produce
    )

    # Define task dependencies    
    create_transaction_facts_table >> validate_transaction_facts_table >> produce_transaction_data
