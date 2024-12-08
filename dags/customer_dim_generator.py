import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

start_date = datetime(2024, 12, 7)
default_args = {
    'owner': 'behordeun',
    'depends_on_past': False,
    'backfill': False,
}

# Parameters
num_rows = 100  # Number of rows to generate
output_file = './customer_dim_large_data.csv'  # Use absolute path


# Function to generate random customer data
def generate_random_data(row_num):
    customer_id = f"C{row_num:05d}"
    first_name = f"FirstName{row_num}"
    last_name = f"LastName{row_num}"
    email = f"customer{row_num}@example.com"
    phone_number = f"+1-800-{random.randint(1000000, 9999999)}"

    # Generate registration date in milliseconds
    now = datetime.now()
    # Random date within the last 10 years
    random_date = now - timedelta(days=random.randint(0, 3650))
    registration_date_millis = int(random_date.timestamp() * 1000)

    return customer_id, first_name, last_name, email, phone_number, registration_date_millis


def generate_customer_dim_data():
    # Initialize lists to store data
    customer_ids = []
    first_names = []
    last_names = []
    emails = []
    phone_numbers = []
    registration_dates = []

    # Generate data using a while loop
    row_num = 1
    while row_num <= num_rows:
        data = generate_random_data(row_num)
        customer_ids.append(data[0])
        first_names.append(data[1])
        last_names.append(data[2])
        emails.append(data[3])
        phone_numbers.append(data[4])
        registration_dates.append(data[5])
        row_num += 1

    # Create a DataFrame
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "first_name": first_names,
        "last_name": last_names,
        "email": emails,
        "phone_number": phone_numbers,
        "registration_date": registration_dates
    })

    # Save DataFrame to CSV
    try:
        df.to_csv(output_file, index=False)
        print(
            f"CSV file '{output_file}' with {num_rows} rows has been generated successfully.")
    except Exception as e:
        print(f"Error writing to CSV: {str(e)}")
        raise


# Define the DAG
with DAG('customer_dim_generator',
         default_args=default_args,
         description='Generate large customer dimension data CSV file',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['schema']) as dag:
    start = EmptyOperator(
        task_id='start_task',
    )

    generate_customer_dim_data_task = PythonOperator(
        task_id='generate_customer_dim_data',
        python_callable=generate_customer_dim_data
    )

    end = EmptyOperator(
        task_id='end_task',
    )

    start >> generate_customer_dim_data_task >> end
