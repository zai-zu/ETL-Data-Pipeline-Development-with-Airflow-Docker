from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, date, timedelta
import csv
import random
import csv
import logging
import uuid
import polars as pl
from faker import Faker

# Configure logging.
logging.basicConfig(
    level=logging.INFO,                    
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler()]
)


def _create_data(locale: str) -> Faker:
    """
    Creates a Faker instance for generating localized fake data.
    Args:
        locale (str): The locale code for the desired fake data language/region.
    Returns:
        Faker: An instance of the Faker class configured with the specified locale.
    """
    # Log the action.
    logging.info(f"Created synthetic data for {locale.split('_')[-1]} country code.")
    return Faker(locale)


def _generate_record(fake: Faker) -> list:
    """
    Generates a single fake user record.
    Args:
        fake (Faker): A Faker instance for generating random data.
    Returns:
        list: A list containing various fake user details such as name, username, email, etc.
    """
    # Generate random personal data.
    person_name = fake.name()
    user_name = person_name.replace(" ", "").lower()  # Create a lowercase username without spaces.
    email = f"{user_name}@{fake.free_email_domain()}"  # Combine the username with a random email domain.
    personal_number = fake.ssn()  # Generate a random social security number.
    birth_date = fake.date_of_birth()  # Generate a random birth date.
    address = fake.address().replace("\n", ", ")  # Replace newlines in the address with commas.
    phone_number = fake.phone_number()  # Generate a random phone number.
    mac_address = fake.mac_address()  # Generate a random MAC address.
    ip_address = fake.ipv4()  # Generate a random IPv4 address.
    iban = fake.iban()  # Generate a random IBAN.
    accessed_at = fake.date_time_between("-1y")  # Generate a random date within the last year.
    session_duration = random.randint(0, 36_000)  # Random session duration in seconds (up to 10 hours).
    download_speed = random.randint(0, 1_000)  # Random download speed in Mbps.
    upload_speed = random.randint(0, 800)  # Random upload speed in Mbps.
    consumed_traffic = random.randint(0, 2_000_000)  # Random consumed traffic in kB.

    # Return all the generated data as a list.
    return [
        person_name, user_name, email, personal_number, birth_date,
        address, phone_number, mac_address, ip_address, iban, accessed_at,
        session_duration, download_speed, upload_speed, consumed_traffic
    ]


def _write_to_csv() -> None:
    """
    Generates multiple fake user records and writes them to a CSV file.
    """
    # Create a Faker instance with Romanian data.
    fake = _create_data("es_MX")
    
    # Define the CSV headers.
    headers = [
        "person_name", "user_name", "email", "personal_number", "birth_date", "address",
        "phone", "mac_address", "ip_address", "iban", "accessed_at",
        "session_duration", "download_speed", "upload_speed", "consumed_traffic"
    ]

    # Establish number of rows based date.
    if str(date.today()) == "2024-09-23":
        rows = random.randint(100_372, 100_372)
    else:
        rows = random.randint(0, 1_101)
    
    # Open the CSV file for writing.
    import os
    os.makedirs("/opt/airflow/data", exist_ok=True)

    with open("/opt/airflow/data/raw_data.csv", mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        
        # Generate and write each record to the CSV.
        for _ in range(rows):
            writer.writerow(_generate_record(fake))
    # Log the action.
    logging.info(f"Written {rows} records to the CSV file.")


def _add_id() -> None:
    """
    Adds a unique UUID to each row in a CSV file.
    """
    # Load the CSV into a Polars DataFrame.
    df = pl.read_csv("/opt/airflow/data/raw_data.csv")
    # Generate a list of UUIDs (one for each row).
    uuid_list = [str(uuid.uuid4()) for _ in range(df.height)]
    # Add a new column with unique IDs.
    df = df.with_columns(pl.Series("unique_id", uuid_list))
    # Save the updated DataFrame back to a CSV.
    df.write_csv("/opt/airflow/data/raw_data.csv")
    # Log the action.
    logging.info("Added UUID to the dataset.")


def _update_datetime() -> None:
    """
    Update the 'accessed_at' column in a CSV file with the appropriate timestamp.
    """
        # Change date only for next runs.
    if str(date.today()) != "2024-09-23":
        # Get the current time without milliseconds and calculate yesterday's time.
        current_time = datetime.now().replace(microsecond=0)
        yesterday_time = str(current_time - timedelta(days=1))
        # Load the CSV into a Polars DataFrame.
        df = pl.read_csv("/opt/airflow/data/raw_data.csv")
        # Replace all values in the 'accessed_at' column with yesterday's timestamp.
        df = df.with_columns(pl.lit(yesterday_time).alias("accessed_at"))
        # Save the updated DataFrame back to a CSV file.
        df.write_csv("/opt/airflow/data/raw_data.csv")
        # Log the action.
        logging.info("Updated accessed timestamp.")


def save_raw_data(**kwargs):
    '''
    Execute all steps for data generation.
    '''
    # Logging starting of the process.
    logging.info(f"Started batch processing for {date.today()}.")
    # Generate and write records to the CSV.
    _write_to_csv()
    # Add UUID to dataset.
    _add_id()
    # Update the timestamp.
    _update_datetime()
    # Logging ending of the process.
    logging.info(f"Finished batch processing {date.today()}.")







# ─────────────────────────  Parámetros generales  ──────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


# ─────────────────────────────  Definición del DAG  ─────────────────────────
with DAG(
    dag_id="extract_raw_data_pipeline",
    default_args=default_args,
    description="DataDriven Main Pipeline.",
    schedule_interval="@daily",
    start_date=datetime(2024, 6, 1),
    catchup=False,
) as dag:

    # 1) Dummy para generar (o simular) el CSV
    extract_raw_data_task = PythonOperator(
        task_id="extract_raw_data",
        python_callable = save_raw_data,
    )

    # 2) Crear esquema si no existe
    create_raw_schema_task = PostgresOperator(
        task_id="create_raw_schema",
        postgres_conn_id= "postgres_conn",
        sql="CREATE SCHEMA IF NOT EXISTS driven_raw;",
    )

    # 3) Crear tabla (con columnas ya ensanchadas)
    create_raw_table_task = PostgresOperator(
        task_id="create_raw_table",
        postgres_conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS driven_raw.raw_batch_data (
                person_name       VARCHAR(100),
                user_name         VARCHAR(100),
                email             VARCHAR(120),
                personal_number   NUMERIC,
                birth_date        VARCHAR(100),
                address           VARCHAR(255),   -- ← más larga
                phone             VARCHAR(100),
                mac_address       VARCHAR(100),
                ip_address        VARCHAR(100),
                iban              VARCHAR(120),
                accessed_at       TIMESTAMP,
                session_duration  INT,
                download_speed    INT,
                upload_speed      INT,
                consumed_traffic  INT,
                unique_id         VARCHAR(120)
            );
        """,
    )

    # 3.1) Ensanchar columna si la tabla ya existía
    widen_address_task = PostgresOperator(
        task_id="widen_address",
        postgres_conn_id="postgres_conn",
        sql="""
            ALTER TABLE driven_raw.raw_batch_data
            ALTER COLUMN address TYPE VARCHAR(255);
        """,
    )

    # 4) Cargar el CSV a la tabla
    load_raw_data_task = PostgresOperator(
        task_id="load_raw_data",
        postgres_conn_id="postgres_conn",
        sql="""
            COPY driven_raw.raw_batch_data(
                person_name, user_name, email, personal_number, birth_date,
                address, phone, mac_address, ip_address, iban, accessed_at,
                session_duration, download_speed, upload_speed, consumed_traffic,
                unique_id
            )
            FROM '/opt/airflow/data/raw_data.csv'
            DELIMITER ','
            CSV HEADER;
        """,
    )

    # 5) Ejecutar modelos dbt
    run_dbt_staging_task = BashOperator(
        task_id="run_dbt_staging",
        bash_command="set -x; cd /opt/airflow/dbt && dbt run --select tag:staging",
    )

    run_dbt_trusted_task = BashOperator(
        task_id="run_dbt_trusted",
        bash_command="""
        set -x
        cd /opt/airflow/dbt
        dbt run --full-refresh --select tag:staging
        """,
    )

    # ──────────────  Dependencias  ──────────────
    [extract_raw_data_task, create_raw_schema_task] >> create_raw_table_task
    create_raw_table_task >> widen_address_task >> load_raw_data_task
    load_raw_data_task >> run_dbt_staging_task >> run_dbt_trusted_task
