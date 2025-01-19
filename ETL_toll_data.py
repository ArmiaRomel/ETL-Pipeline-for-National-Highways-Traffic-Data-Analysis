from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'Armia Garas',
    'start_date': days_ago(0),
    'email': ['armiaromeel@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args = default_args,
    description = 'De-congest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats.'
)

address = '~/airflow/dags/toll_project/'

def extracting_csv():
    df = pd.read_csv(f'{address}vehicle-data.csv', header=None)
    csv_data = df[[0,1,2,3]]
    csv_data.to_csv(f'{address}csv_data.csv', index=False, header=False)

def extracting_tsv():
    df = pd.read_csv(f'{address}tollplaza-data.tsv', sep='\t', header=None)
    tsv_data = df[[4,5,6]]
    tsv_data.to_csv(f'{address}tsv_data.csv', index=False, header=False)

def consolidating():
    csv_data = pd.read_csv(f'{address}csv_data.csv', header=None)
    tsv_data = pd.read_csv(f'{address}tsv_data.csv', header=None)
    txt_data = pd.read_csv(f'{address}fixed_width_data.csv', header=None)
    extracted_data = pd.concat([csv_data, tsv_data, txt_data], axis=1)
    extracted_data.to_csv(f'{address}extracted_data.csv', index=False, header=False)

def transforming():
    extracted_data = pd.read_csv(f'{address}extracted_data.csv', header=None)
    extracted_data[3] = extracted_data[3].str.upper()
    extracted_data.to_csv(f'{address}transformed_data.csv', index=False, header=False)


download = BashOperator(
    task_id = 'download_data',
    bash_command = f"curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o {address}tolldata.tgz",
    dag = dag
)

unzip = BashOperator(
    task_id = 'unzip_data',
    bash_command = f"tar -xzf {address}tolldata.tgz \
                    -C {address}",
    dag = dag
)

extract_csv = PythonOperator(
    task_id = 'extract_data_from_csv',
    python_callable = extracting_csv,
    dag = dag
)

extract_tsv = PythonOperator(
    task_id = 'extract_data_from_tsv',
    python_callable = extracting_tsv,
    dag = dag
)

extract_txt = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = f"cut -c 59-62,63-67 {address}payment-data.txt \
                    | tr ' ' ',' > {address}fixed_width_data.csv",
    dag = dag
)

consolidate = PythonOperator(
    task_id = 'consolidate_data',
    python_callable = consolidating,
    dag = dag
)

transform = PythonOperator(
    task_id = 'transform_data',
    python_callable = transforming,
    dag = dag
)

download >> unzip >> extract_csv >> extract_tsv >> extract_txt >> consolidate >> transform

