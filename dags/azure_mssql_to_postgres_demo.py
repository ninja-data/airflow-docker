from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pyodbc
import psycopg2
from datetime import datetime

def get_max_lsn_from_postgres(**kwargs):
    conn_params = {
        'dbname': 'postgres',
        'user': 'admin_login',
        'password': '25892589aA',
        'host': 'postgresqlserver-seles-demo-001.postgres.database.azure.com'
    }
    
    query = "SELECT max(start_lsn) FROM stage.dim_address"
    
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            max_lsn = result[0] if result[0] else '2024-07-31 03:35:57.990'  # Default if no results

    # Format the timestamp to remove microseconds
    if isinstance(max_lsn, datetime):
        max_lsn = max_lsn.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Log the max_lsn for debugging
    print(f"Max LSN from Postgres: {max_lsn}")
    
    return max_lsn



def extract_from_mssql(**kwargs):
    conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:mssqserver-sales-demo-001.database.windows.net,1433;Database=mssqldb-sales-demo-001;Uid=admin_login;Pwd={25892589aA};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    
    # Fetch the max LSN from the previous task
    date_param = kwargs['ti'].xcom_pull(task_ids='get_max_lsn_from_postgres')


    query = f"""with cte as (
                SELECT
                sys.fn_cdc_map_lsn_to_time([__$start_lsn]) as [__$start_lsn],
                sys.fn_cdc_map_lsn_to_time([__$end_lsn]) as [__$end_lsn],
                CONVERT(varchar(max), __$seqval, 1) as [__$seqval],
                [__$operation],
                AddressID,
                AddressLine1,
                AddressLine2,
                City,
                StateProvince,
                CountryRegion,
                PostalCode,
                rowguid,
                ModifiedDate,
                [__$command_id]
            from
                cdc.SalesLT_Address_CT slac 
            )
            select * from cte where [__$start_lsn] > '{date_param}'
            """
    
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            # Convert rows into a list of tuples
            rows_list = [list(row) for row in rows]
    
    return rows_list

def load_to_postgres(**kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids='extract_from_mssql')
    
    conn_params = {
        'dbname': 'postgres',
        'user': 'admin_login',
        'password': '25892589aA',
        'host': 'postgresqlserver-seles-demo-001.postgres.database.azure.com'
    }
    
    insert_query = '''
    INSERT INTO stage.dim_address (
        start_lsn,
        end_lsn,
        seqval,
        operation,
        address_id,
        address_line1,
        address_line2,
        city,
        state_province,
        country_region,
        postal_code,
        rowguid,
        modified_date,
        command_id
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
    
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            for row in rows:
                cursor.execute(insert_query, row)
            conn.commit()

with DAG(
    dag_id='azure_mssql_to_postgres_demo',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    start_date=days_ago(1),
    catchup=False,
) as dag:

    get_max_lsn_task = PythonOperator(
        task_id='get_max_lsn_from_postgres',
        python_callable=get_max_lsn_from_postgres,
        provide_context=True,
    )

    extract_task = PythonOperator(
        task_id='extract_from_mssql',
        python_callable=extract_from_mssql,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )
    
    get_max_lsn_task >> extract_task >> load_task
