# **ETL Pipeline for National Highways Traffic Data Analysis**

This project aims to de-congest the national highways by analyzing road traffic data from different toll plazas. Each highway is operated by a different toll operator with a unique IT setup, and the data is available in various file formats. The goal is to collect, consolidate, and analyze the data to provide insights into traffic patterns.

The data files in the provided tarball lack headers. To address this, a [fileformats](https://github.com/user-attachments/files/18469511/fileformats.txt) file is included in the project repository. This file explains the structure of each data file, including the headers and their details.

---

## **Technologies Used**

- **Apache Airflow** (for orchestrating the ETL pipeline)
- **Python** (for data manipulation and transformations)
- **Pandas** (for reading, processing, and writing CSV/TSV files)
- **Bash** (for handling file operations)
- **Other**: Airflow Operators (`BashOperator`, `PythonOperator`), `curl`, `tar`, `cut`, `tr`

---

## **Setup and Installation**

Follow these steps to set up Python and install the necessary libraries for the Airflow environment:

1. **Install Python3 and pip**:
    ```bash
    sudo apt install python3-pip
    ```

2. **Install `virtualenv`** to create an isolated Python environment:
    ```bash
    pip3 install virtualenv
    ```

3. **Create a virtual environment** for Airflow:
    ```bash
    virtualenv airflow-env
    ```

4. **Activate the virtual environment**:
    ```bash
    source airflow-env/bin/activate
    ```

5. **Install Apache Airflow** and the required extra dependencies:
    ```bash
    pip3 install apache-airflow[gcp,sentry,statsd]
    ```

6. **Initialize the Airflow database**:
    ```bash
    airflow db init
    ```

7. **Create an Airflow user** (replace the placeholders with your information):
    ```bash
    airflow users create --username admin --password admin --firstname user_fname --lastname user_lname --role Admin --email user@gmail.com
    ```

---

## **How to Use**

1. **Create the project Python script**:
   - Save your DAG script inside the `~/airflow/dags/` directory.

2. **Start the Airflow scheduler and webserver**:

Airflow requires two processes to be running in different terminals: the **scheduler** and the **webserver**.

- **Open the first terminal** and run the Airflow scheduler:
    ```bash
    airflow scheduler
    ```
![Image](https://github.com/user-attachments/assets/28eea867-61dc-45f4-ba92-cebc524eb217)

- **Open the second terminal** and run the Airflow webserver:
    ```bash
    airflow webserver
    ```
![Image](https://github.com/user-attachments/assets/1f4fdeae-b1af-48a8-8bf4-cfdd29697f0a)

After running the above commands, the Airflow Web UI will be available at [http://localhost:8080](http://localhost:8080).

---

## **Code Walkthrough**

# **DAG Setup**

```python
default_args = {
    'owner': 'Armia Garas',
    'start_date': days_ago(0),  # The DAG will start running immediately
    'email': ['armiaromeel@gmail.com'],  # Set email for notifications
    'email_on_failure': True,  # Send an email notification if the task fails during execution
    'email_on_retry': True,  # Send an email notification if the task is retried due to a failure
    'retries': 1,  # Retry the task once if it fails
    'retry_delay': timedelta(minutes=5)  # Wait 5 minutes before retrying
}

dag = DAG(
    'ETL_toll_data',  # The unique name of the DAG
    schedule_interval = timedelta(days=1),  # DAG runs daily
    default_args = default_args,  # Use the default arguments for the DAG
    description = 'De-congest national highways by analyzing toll plaza data.'
)
```
- `default_args`: Specifies parameters for task retries, failure notifications, and owner information.
- `dag`: Creates the Airflow DAG, where the tasks are defined and scheduled.

# **Task Definitions**

**Downloading Data**
Downloads the compressed [traffic data](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz) file from a cloud storage URL.
```python
download = BashOperator(
    task_id='download_data',
    bash_command=f"curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o {address}tolldata.tgz",
    dag=dag
)
```

**Unzipping the Data**
Extracts the downloaded .tgz (tarball) file to the specified directory.
```python
unzip = BashOperator(
    task_id='unzip_data',
    bash_command=f"tar -xzf {address}tolldata.tgz -C {address}",
    dag=dag
)
```

**Extracting Data from CSV**
Selects the first four columns of the data which are `Rowid`, `Timestamp`, `Anonymized Vehicle number` and `Vehicle type` then saves the processed data into a new CSV file.
```python
def extracting_csv():
    df = pd.read_csv(f'{address}vehicle-data.csv', header=None)
    csv_data = df[[0,1,2,3]]
    csv_data.to_csv(f'{address}csv_data.csv', index=False, header=False)
```

**Extracting Data from TSV**
Selects the fifth, sixth and seventh columns of the data which are `Number of axles`, `Tollplaza id` and `Tollplaza code` then saves the processed data into a new CSV file.
```python
def extracting_tsv():
    df = pd.read_csv(f'{address}tollplaza-data.tsv', sep='\t', header=None)  # Read TSV data using tab delimiter
    tsv_data = df[[4,5,6]]
    tsv_data.to_csv(f'{address}tsv_data.csv', index=False, header=False)
```

**Extracting Fixed-Width Data**
Selects the last two columns of the data which are `Type of Payment code` and `Vehicle Code` then saves the processed data into a new CSV file.
```python
extract_txt = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = f"cut -c 59-62,63-67 {address}payment-data.txt \
                    | tr ' ' ',' > {address}fixed_width_data.csv",
# Cut specific character ranges, replace spaces with commas, and save as CSV
    dag = dag
)
```

**Consolidating Data**
Concatenates the CSV, TSV, and fixed-width data along columns (axis=1) then saves the consolidated data as a new CSV file.
```python
def consolidating():
    csv_data = pd.read_csv(f'{address}csv_data.csv', header=None)
    tsv_data = pd.read_csv(f'{address}tsv_data.csv', header=None)
    txt_data = pd.read_csv(f'{address}fixed_width_data.csv', header=None)
    extracted_data = pd.concat([csv_data, tsv_data, txt_data], axis=1)
    extracted_data.to_csv(f'{address}extracted_data.csv', index=False, header=False)
```

**Transforming Data**
Converts the fourth column `vehicle_type` to uppercase then saves the transformed data as a new CSV file.
```python
def transforming():
    extracted_data = pd.read_csv(f'{address}extracted_data.csv', header=None)
    extracted_data[3] = extracted_data[3].str.upper()  # Convert column 3 to uppercase
    extracted_data.to_csv(f'{address}transformed_data.csv', index=False, header=False)
```

## **Airflow UI Screenshots**

1. **Airflow DAG Structure**:
![Image](https://github.com/user-attachments/assets/d72038e9-8342-4e25-858f-85521bd658a8)

2. **Run Details (Logs, Status)**:
![Image](https://github.com/user-attachments/assets/ecfc7c72-4d5d-45f3-bb0c-33b21cd14f02)
