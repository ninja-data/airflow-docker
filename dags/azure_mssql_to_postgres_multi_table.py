from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pyodbc
import psycopg2
from datetime import datetime

# TODO 1 = delete, 2 = insert, 3 = update (before image), and 4 = update

# Define the column mappings for each table
table_columns = {
    'dim_address': [
        #'start_lsn', 'end_lsn', 'seqval', 'operation', 'command_id',
        'address_id', 'address_line1', 'address_line2',
        'city', 'state_province', 'country_region',
        'postal_code', 'rowguid', 'modified_date'
    ],
    'dim_customer': [
        #'start_lsn', 'end_lsn', 'seqval', 'operation', 'command_id',
        'customer_id', 'title', 'first_name', 
        'middle_name', 'last_name', 'suffix', 
        'company_name', 'sales_person', 'email_address', 
        'phone', 'password_hash', 'password_salt', 
        'rowguid', 'modified_date'
    ],
    'dim_customer_address': [
        'customer_address_id', 'customer_id', 'address_id', 'address_type',
        'rowguid', 'modified_date'
    ],
    'dim_region': [
        #'start_lsn', 'end_lsn', 'seqval', 'operation', 'command_id',
        'region_id', 'region_name', 'modified_date'
    ],
    'fact_sales_order_detail': [
        #'start_lsn', 'end_lsn', 'seqval', 'operation', 'command_id',
        'sales_order_detail_id', 'sales_order_id', 'order_qty',
        'product_id', 'unit_price', 'unit_price_discount', 
        'line_total', 'rowguid', 'modified_date'
    ],
    'fact_sales_order_header': [
        'sales_order_id', 'revision_number', 'order_date', 
        'due_date', 'ship_date', 'status', 
        'online_order_flag', 'sales_order_number', 'purchase_order_number', 
        'account_number', 'customer_id', 'ship_to_address_id', 
        'bill_to_address_id', 'ship_method', 'credit_card_approval_code', 
        'sub_total', 'tax_amt', 'freight', 
        'total_due', 'comment', 'rowguid', 'modified_date'
    ]
}


def get_max_lsn_from_postgres(table, **kwargs):
    conn_params = {
        'dbname': 'postgres',
        'user': 'admin_login',
        'password': '25892589aA',
        'host': 'postgresqlserver-seles-demo-001.postgres.database.azure.com'
    }
    
    query = f"select last_update from metadata.update_metadata where table_name = '{table}'"
    
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

def transform_table_name(table_name):
    parts = table_name.split('_')
    return ''.join(parts[1:])

def extract_from_mssql(table, **kwargs):
    conn_str = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:mssqserver-sales-demo-001.database.windows.net,1433;Database=mssqldb-sales-demo-001;Uid=admin_login;Pwd={25892589aA};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    
    date_param = kwargs['ti'].xcom_pull(task_ids=f'get_max_lsn_from_postgres_{table}')
    
    columns = table_columns.get(table, [])
    columns= [col.replace('_', '') for col in columns] # Replace underscores with spaces
    column_str = ', '.join(columns)
    select_columns = ', '.join([f'tb.{col}' for col in columns])
    
    table = transform_table_name(table)

    query = f"""with cte as (
                SELECT
                sys.fn_cdc_map_lsn_to_time([__$start_lsn]) as [__$start_lsn],
                sys.fn_cdc_map_lsn_to_time([__$end_lsn]) as [__$end_lsn],
                CONVERT(varchar(max), __$seqval, 1) as [__$seqval],
                [__$operation],
                [__$command_id],
                {select_columns}
            from
                cdc.SalesLT_{table}_CT tb 
            )
            select * from cte where [__$start_lsn] > '{date_param}' and [__$operation] <> 3
            """
    print(query)
    
    with pyodbc.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            rows_list = [list(row) for row in rows]
   
    print(rows_list)
    return rows_list

def load_to_postgres(table, **kwargs):
    rows = kwargs['ti'].xcom_pull(task_ids=f'extract_from_mssql_{table}')
    
    conn_params = {
        'dbname': 'postgres',
        'user': 'admin_login',
        'password': '25892589aA',
        'host': 'postgresqlserver-seles-demo-001.postgres.database.azure.com'
    }
    
    columns = table_columns.get(table, [])
    column_str = ', '.join(columns)
    value_str = ', '.join(['%s'] * (len(columns)+5))

    truncate_query = f'TRUNCATE stage.{table}'

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(truncate_query)
            conn.commit()
    
    insert_query = f'''
    INSERT INTO stage.{table} (
        start_lsn, end_lsn, seqval, operation, command_id, {column_str}
    ) VALUES ({value_str})
    '''
    print(f'ROWS {rows}')

    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, rows)
            conn.commit()


def process_in_postgres(table, **kwargs):
    # rows = kwargs['ti'].xcom_pull(task_ids=f'extract_from_mssql_{table}')
    
    conn_params = {
        'dbname': 'postgres',
        'user': 'admin_login',
        'password': '25892589aA',
        'host': 'postgresqlserver-seles-demo-001.postgres.database.azure.com'
    }
    
    columns = table_columns.get(table, [])
    column_str = ', '.join(columns)
    update_column_str = ', '.join([f'{column} = EXCLUDED.{column}' for column in columns[1:]])
    # value_str = ', '.join(['%s'] * (len(columns)+5))
    
    if table == 'dim_region':
        process_query = f'''
                    -- BEGIN;

                    -- UPSERT
                    INSERT INTO target.{table} 
                        ({column_str})
                    SELECT 
                        {column_str}
                    FROM 
                        stage.{table}
                    WHERE 
                        operation IN (2, 4)
                    ORDER BY seqval;

                    -- SOFT DELETE
                    UPDATE target.{table} t
                    SET is_deleted = TRUE
                    FROM stage.{table} s
                    WHERE s.{columns[0]} = t.{columns[0]}
                    AND s.operation = 1;

                    -- SAVE LAST UPDATE DATE
                    DO $$
                    BEGIN
                        IF (select count(*) from stage.{table}) > 0 THEN
                            INSERT INTO metadata.update_metadata (table_name, last_update)
                            SELECT 
                                '{table}', 
                                MAX(start_lsn) AS last_update 
                            FROM 
                                stage.{table}
                            ON CONFLICT (table_name)
                            DO UPDATE SET
                                last_update = EXCLUDED.last_update;
                        END IF;
                    END $$;

                    -- SCD
                    UPDATE target.dim_region
                    SET is_current = false
                    WHERE EXISTS (
                        SELECT 1
                        FROM stage.dim_region dr
                        WHERE dr.region_id = target.dim_region.region_id
                    );

                    WITH max_datetime AS (
                        SELECT region_id, MAX(modified_date) AS max_modified_date
                        FROM stage.dim_region
                        GROUP BY region_id
                    )
                    UPDATE target.dim_region dr
                    SET is_current = true
                    FROM max_datetime md
                    WHERE dr.region_id = md.region_id
                    AND dr.modified_date = md.max_modified_date;

                    -- COMMIT;
                    '''
    else:
        process_query = f'''
                    -- BEGIN;

                    -- UPSERT
                    INSERT INTO target.{table} 
                        ({column_str})
                    SELECT 
                        {column_str}
                    FROM 
                        (
                            SELECT 
                                *,
                                ROW_NUMBER() OVER (PARTITION BY {columns[0]} ORDER BY seqval DESC) AS row_number
                            FROM 
                                stage.{table}
                            WHERE 
                                operation IN (2, 4)
                        ) nq
                    WHERE 
                        row_number = 1
                    ON CONFLICT ({columns[0]})
                    DO UPDATE SET 
                        {update_column_str};

                    -- SOFT DELETE
                    UPDATE target.{table} t
                    SET is_deleted = TRUE
                    FROM stage.{table} s
                    WHERE s.{columns[0]} = t.{columns[0]}
                    AND s.operation = 1;

                    -- SAVE LAST UPDATE DATE
                    DO $$
                    BEGIN
                        IF (select count(*) from stage.{table}) > 0 THEN
                            INSERT INTO metadata.update_metadata (table_name, last_update)
                            SELECT 
                                '{table}', 
                                MAX(start_lsn) AS last_update 
                            FROM 
                                stage.{table}
                            ON CONFLICT (table_name)
                            DO UPDATE SET
                                last_update = EXCLUDED.last_update;
                        END IF;
                    END $$;
                    -- COMMIT;
                    '''
    print(process_query)
    
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            cursor.execute(process_query)
            conn.commit()

with DAG(
    dag_id='azure_mssql_to_postgres_multi_table',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    schedule_interval='*/5 * * * *',  # Run every 5 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    for table in table_columns.keys():
        get_max_lsn_task = PythonOperator(
            task_id=f'get_max_lsn_from_postgres_{table}',
            python_callable=get_max_lsn_from_postgres,
            op_kwargs={'table': table},
            provide_context=True,
        )

        extract_task = PythonOperator(
            task_id=f'extract_from_mssql_{table}',
            python_callable=extract_from_mssql,
            op_kwargs={'table': table},
            provide_context=True,
        )
        
        load_task = PythonOperator(
            task_id=f'load_to_postgres_{table}',
            python_callable=load_to_postgres,
            op_kwargs={'table': table},
            provide_context=True,
        )

        process_task = PythonOperator(
            task_id=f'process_in_postgres_{table}',
            python_callable=process_in_postgres,
            op_kwargs={'table': table},
            provide_context=True,
        )
        
        get_max_lsn_task >> extract_task >> load_task >> process_task
