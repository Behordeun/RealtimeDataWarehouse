from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json
import random

# DAG default arguments
default_args = {
    'owner': 'behordeun',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='transaction_facts_generator',
    default_args=default_args,
    description='Generate transaction facts and publish them to Kafka',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 12, 7),
    catchup=False,
    tags=['fact_data']
) as dag:

    def generate_transaction_data(num_records, kafka_broker, kafka_topic):
        """
        Generate random transaction fact data and send to Kafka topic.

        Args:
            num_records (int): Number of records to generate.
            kafka_broker (str): Kafka broker address.
            kafka_topic (str): Kafka topic to publish the data to.
        """
        producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for _ in range(num_records):
            transaction_data = {
                "transaction_id": f"T{random.randint(100000, 999999)}",
                "transaction_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "amount": round(random.uniform(10, 1000), 2),
                "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
                "branch_id": f"B{random.randint(1, 50)}",
                "transaction_type": random.choice(["DEBIT", "CREDIT"]),
            }
            producer.send(kafka_topic, value=transaction_data)
            print(f"Sent transaction data: {transaction_data}")

        producer.close()

    start_task = EmptyOperator(
        task_id='start_task'
    )

    # PythonOperator to generate and publish transaction data
    generate_txn_data_task = PythonOperator(
        task_id='generate_txn_fact_data',
        python_callable=generate_transaction_data,
        op_kwargs={
            'num_records': 1000,
            'kafka_broker': 'kafka_broker:9092',
            'kafka_topic': 'transaction_facts',
        }
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Define task dependencies
    start_task >> generate_txn_data_task >> end_task
