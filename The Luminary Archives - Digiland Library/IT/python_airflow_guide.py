# Python for Apache Airflow - Essential Guide for SQL Developers
# By Aitana Catalyst for Ray Serrano
# Focus: Just what you need, nothing more

# ============================================
# PART 1: PYTHON BASICS YOU'LL ACTUALLY USE
# ============================================

# 1.1 Variables and Data Types
# -----------------------------
dag_name = "my_etl_pipeline"              # string
retry_count = 3                           # integer
retry_delay = 5.0                         # float
is_active = True                          # boolean
task_list = ['extract', 'transform', 'load']  # list
config = {'retries': 2, 'email': 'ray@example.com'}  # dictionary

# 1.2 Dictionaries (CRITICAL for Airflow)
# ----------------------------------------
# You'll use these constantly for configuration
default_args = {
    'owner': 'ray',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['ray@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Accessing dictionary values
owner = default_args['owner']
retries = default_args.get('retries', 0)  # with default value

# 1.3 Functions (Your SQL logic goes here)
# -----------------------------------------
def process_data(table_name, date):
    """Your SQL/processing logic wrapped in Python"""
    query = f"""
        SELECT * FROM {table_name}
        WHERE date = '{date}'
    """
    # Execute your SQL here
    return query

# 1.4 The **kwargs pattern (Airflow uses this everywhere)
# --------------------------------------------------------
def my_task(**context):
    """
    Airflow passes context through **kwargs
    This gives you access to execution date, task instance, etc.
    """
    execution_date = context['execution_date']
    task_instance = context['task_instance']
    
    # Your actual logic here
    print(f"Running for date: {execution_date}")
    
    # Push data to next task
    task_instance.xcom_push(key='my_data', value='processed_data')
    
# 1.5 String Formatting (for dynamic SQL)
# ----------------------------------------
table = "users"
date = "2024-01-01"

# Method 1: f-strings (Python 3.6+) - RECOMMENDED
query = f"SELECT * FROM {table} WHERE date = '{date}'"

# Method 2: .format() method
query = "SELECT * FROM {} WHERE date = '{}'".format(table, date)

# Method 3: % formatting (older style)
query = "SELECT * FROM %s WHERE date = '%s'" % (table, date)

# ============================================
# PART 2: CORE AIRFLOW PATTERNS
# ============================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# 2.1 Basic DAG Structure
# ------------------------
with DAG(
    dag_id='example_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't backfill
    tags=['etl', 'daily']
) as dag:
    
    # Tasks go here (see below)
    pass

# 2.2 Common Operators You'll Use
# --------------------------------

# Python Operator - For custom Python logic
def extract_data(**context):
    """Extract data from source"""
    execution_date = context['execution_date']
    
    # Your extraction logic
    data = "extracted_data"
    
    # Pass data to next task
    return data

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

# SQL Operator - Direct SQL execution
sql_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql="""
        CREATE TABLE IF NOT EXISTS processed_data (
            id SERIAL PRIMARY KEY,
            data VARCHAR(255),
            processed_date DATE
        )
    """,
    dag=dag
)

# Bash Operator - For shell commands
bash_task = BashOperator(
    task_id='run_dbt',
    bash_command='dbt run --models my_model',
    dag=dag
)

# 2.3 Task Dependencies
# ---------------------
# Method 1: Bit shift operators (recommended)
extract_task >> transform_task >> load_task

# Method 2: set_downstream/set_upstream
extract_task.set_downstream(transform_task)
transform_task.set_downstream(load_task)

# Method 3: Lists for parallel tasks
extract_task >> [transform_1, transform_2] >> load_task

# ============================================
# PART 3: COMMON PATTERNS FOR SQL DEVELOPERS
# ============================================

# 3.1 Dynamic SQL with Templating
# --------------------------------
class SQLTemplateOperator(PostgresOperator):
    """Custom operator for templated SQL"""
    
    template_fields = ['sql']  # Makes SQL templateable
    
templated_sql = """
    INSERT INTO daily_summary
    SELECT 
        '{{ ds }}' as date,
        COUNT(*) as total_records
    FROM source_table
    WHERE date = '{{ ds }}'
"""

sql_with_template = SQLTemplateOperator(
    task_id='daily_summary',
    sql=templated_sql,
    dag=dag
)

# 3.2 Using Hooks for Database Connections
# -----------------------------------------
def custom_sql_logic(**context):
    """Run complex SQL logic with Python control flow"""
    
    # Get connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Your SQL logic
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    
    if count > 1000:
        cursor.execute("CALL process_large_dataset()")
    else:
        cursor.execute("CALL process_small_dataset()")
    
    conn.commit()
    cursor.close()
    conn.close()

# 3.3 XCom for Passing Data Between Tasks
# ----------------------------------------
def get_record_count(**context):
    """First task - get count"""
    # Your logic to get count
    count = 12345
    
    # Push to XCom
    context['task_instance'].xcom_push(key='record_count', value=count)
    return count

def process_based_on_count(**context):
    """Second task - use count from first task"""
    
    # Pull from XCom
    ti = context['task_instance']
    count = ti.xcom_pull(task_ids='get_count', key='record_count')
    
    if count > 10000:
        print("Large dataset processing")
    else:
        print("Small dataset processing")

# 3.4 Dynamic DAG Generation (Advanced)
# --------------------------------------
def create_dag(dag_id, schedule, table_name):
    """Factory function to create DAGs dynamically"""
    
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        default_args=default_args
    )
    
    with dag:
        extract = PostgresOperator(
            task_id='extract',
            sql=f"SELECT * FROM {table_name}"
        )
        
        transform = PythonOperator(
            task_id='transform',
            python_callable=lambda: print(f"Processing {table_name}")
        )
        
        extract >> transform
    
    return dag

# Create multiple DAGs dynamically
tables = ['users', 'orders', 'products']
for table in tables:
    dag_id = f'process_{table}'
    globals()[dag_id] = create_dag(dag_id, '@daily', table)

# ============================================
# PART 4: PRACTICAL EXAMPLES
# ============================================

# 4.1 Complete ETL Pipeline Example
# ----------------------------------
from airflow.models import Variable

with DAG(
    'complete_etl_example',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as etl_dag:
    
    def extract(**context):
        """Extract data from source database"""
        pg_hook = PostgresHook(postgres_conn_id='source_db')
        
        sql = """
            SELECT * FROM transactions 
            WHERE date = '{{ ds }}'
        """
        
        df = pg_hook.get_pandas_df(sql)
        
        # Save to temp location (in real scenario, use S3/GCS)
        df.to_csv(f'/tmp/transactions_{{ ds }}.csv')
        
        return f'/tmp/transactions_{{ ds }}.csv'
    
    def transform(**context):
        """Transform the extracted data"""
        ti = context['task_instance']
        file_path = ti.xcom_pull(task_ids='extract')
        
        import pandas as pd
        df = pd.read_csv(file_path)
        
        # Your transformations
        df['amount_usd'] = df['amount'] * 1.1  # Example transformation
        df['processed_date'] = context['execution_date']
        
        # Save transformed data
        output_path = f'/tmp/transformed_{{ ds }}.csv'
        df.to_csv(output_path)
        
        return output_path
    
    def load(**context):
        """Load data to destination"""
        ti = context['task_instance']
        file_path = ti.xcom_pull(task_ids='transform')
        
        import pandas as pd
        df = pd.read_csv(file_path)
        
        # Load to destination
        pg_hook = PostgresHook(postgres_conn_id='dest_db')
        df.to_sql('processed_transactions', pg_hook.get_sqlalchemy_engine())
        
        # Log success
        print(f"Loaded {len(df)} records")
    
    # Define tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )
    
    # Set dependencies
    extract_task >> transform_task >> load_task

# 4.2 Incremental Load Pattern
# -----------------------------
def incremental_load(**context):
    """Load only new/changed records"""
    
    # Get last successful run
    last_run = Variable.get('last_successful_run', default_var='2024-01-01')
    current_run = context['execution_date'].strftime('%Y-%m-%d')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = f"""
        INSERT INTO target_table
        SELECT * FROM source_table
        WHERE updated_at > '{last_run}'
        AND updated_at <= '{current_run}'
    """
    
    pg_hook.run(sql)
    
    # Update variable for next run
    Variable.set('last_successful_run', current_run)

# ============================================
# PART 5: DEBUGGING & BEST PRACTICES
# ============================================

# 5.1 Logging (Essential for debugging)
# --------------------------------------
import logging

def task_with_logging(**context):
    """Example with proper logging"""
    
    # Airflow will capture these logs
    logging.info("Starting task")
    
    try:
        # Your logic here
        result = some_operation()
        logging.info(f"Operation successful: {result}")
        
    except Exception as e:
        logging.error(f"Task failed: {str(e)}")
        raise  # Re-raise to mark task as failed
    
    finally:
        logging.info("Task cleanup")

# 5.2 Testing Tasks Locally
# --------------------------
if __name__ == "__main__":
    """Test your functions before deploying"""
    
    # Create fake context for testing
    test_context = {
        'execution_date': datetime.now(),
        'task_instance': None  # Mock this if needed
    }
    
    # Test your function
    extract_data(**test_context)

# 5.3 Common Gotchas & Solutions
# -------------------------------
"""
1. TIMEZONE ISSUES
   - Always use timezone-aware datetimes
   - from airflow.utils.timezone import datetime
   
2. XCOM SIZE LIMITS
   - XCom has size limits (~48KB)
   - For large data, save to S3/GCS and pass path
   
3. CONNECTIONS
   - Set up in Airflow UI: Admin -> Connections
   - Reference by conn_id in code
   
4. TESTING
   - Use 'airflow tasks test dag_id task_id execution_date'
   - Test locally first with if __name__ == "__main__"
   
5. TEMPLATING
   - {{ ds }} = execution date (YYYY-MM-DD)
   - {{ ds_nodash }} = execution date (YYYYMMDD)
   - {{ prev_ds }} = previous execution date
   - {{ params.my_param }} = custom parameters
"""

# ============================================
# PART 6: YOUR SPECIFIC USE CASES
# ============================================

# 6.1 dbt Integration
# --------------------
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt && dbt run --models {{ params.models }}',
    params={'models': 'my_model+'},  # Parameterizable
    dag=dag
)

# 6.2 Data Quality Checks
# ------------------------
def data_quality_check(**context):
    """Check data quality before proceeding"""
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check for nulls
    null_check = """
        SELECT COUNT(*) FROM my_table 
        WHERE important_column IS NULL
    """
    
    null_count = pg_hook.get_first(null_check)[0]
    
    if null_count > 0:
        raise ValueError(f"Found {null_count} null values!")
    
    logging.info("Data quality check passed")

# 6.3 Sensor Pattern (Wait for conditions)
# -----------------------------------------
from airflow.sensors.sql import SqlSensor

wait_for_data = SqlSensor(
    task_id='wait_for_source_data',
    conn_id='postgres_default',
    sql="SELECT COUNT(*) FROM source_table WHERE date = '{{ ds }}'",
    mode='poke',  # or 'reschedule'
    timeout=3600,  # 1 hour
    poke_interval=300,  # Check every 5 minutes
    dag=dag
)

# ============================================
# QUICK REFERENCE - MOST COMMON PATTERNS
# ============================================

"""
1. BASIC DAG STRUCTURE:
   with DAG(...) as dag:
       task1 = Operator(...)
       task2 = Operator(...)
       task1 >> task2

2. PYTHON TASK:
   def my_function(**context):
       # logic here
       return result
   
   task = PythonOperator(
       task_id='my_task',
       python_callable=my_function
   )

3. SQL TASK:
   task = PostgresOperator(
       task_id='run_sql',
       sql='SELECT * FROM table',
       postgres_conn_id='my_connection'
   )

4. DEPENDENCIES:
   task1 >> task2  # task2 runs after task1
   task1 >> [task2, task3] >> task4  # parallel

5. XCOM (pass data):
   # Push: context['task_instance'].xcom_push(key='my_key', value=data)
   # Pull: context['task_instance'].xcom_pull(task_ids='task_id', key='my_key')

6. TEMPLATING:
   sql = "SELECT * FROM table WHERE date = '{{ ds }}'"
   
7. SCHEDULING:
   '@daily' = '0 0 * * *'
   '@hourly' = '0 * * * *'
   '@weekly' = '0 0 * * 0'
   
8. TESTING:
   airflow tasks test my_dag my_task 2024-01-01
"""

# ============================================
# PART 7: REAL-WORLD EXAMPLES
# ============================================

# 7.1 COMPLETE DATA WAREHOUSE REFRESH
# ------------------------------------
"""
Scenario: Daily refresh of dimensional model
- Extract from multiple sources
- Transform in staging
- Load to star schema
- Update aggregate tables
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

with DAG(
    'data_warehouse_refresh',
    default_args={
        'owner': 'ray',
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'email_on_failure': True,
        'email': ['ray@company.com']
    },
    description='Daily DW refresh with SCD Type 2',
    schedule_interval='0 3 * * *',  # 3 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dw', 'production']
) as dw_dag:
    
    # Start marker
    start = DummyOperator(task_id='start')
    
    # 1. EXTRACT LAYER
    def extract_from_source(source_name, **context):
        """Generic extraction function"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        source_queries = {
            'customers': f"""
                SELECT 
                    customer_id,
                    customer_name,
                    customer_email,
                    created_at,
                    updated_at
                FROM customers
                WHERE updated_at >= '{execution_date}'::date - INTERVAL '1 day'
                AND updated_at < '{execution_date}'::date + INTERVAL '1 day'
            """,
            'orders': f"""
                SELECT 
                    order_id,
                    customer_id,
                    order_date,
                    total_amount,
                    status
                FROM orders
                WHERE order_date = '{execution_date}'
            """,
            'products': """
                SELECT 
                    product_id,
                    product_name,
                    category,
                    price,
                    last_modified
                FROM products
                WHERE active = true
            """
        }
        
        pg_hook = PostgresHook(postgres_conn_id=f'{source_name}_db')
        df = pg_hook.get_pandas_df(source_queries[source_name])
        
        # Add metadata
        df['etl_timestamp'] = datetime.now()
        df['etl_date'] = execution_date
        
        # Save to staging
        staging_hook = PostgresHook(postgres_conn_id='staging_db')
        engine = staging_hook.get_sqlalchemy_engine()
        
        # Truncate and load pattern
        with engine.connect() as conn:
            conn.execute(f"TRUNCATE TABLE staging.{source_name}")
            df.to_sql(
                source_name, 
                conn, 
                schema='staging', 
                if_exists='append', 
                index=False
            )
        
        # Log metrics
        context['task_instance'].xcom_push(
            key=f'{source_name}_count', 
            value=len(df)
        )
        
        logging.info(f"Extracted {len(df)} records from {source_name}")
        return len(df)
    
    # Create extract tasks dynamically
    extract_tasks = []
    for source in ['customers', 'orders', 'products']:
        task = PythonOperator(
            task_id=f'extract_{source}',
            python_callable=extract_from_source,
            op_kwargs={'source_name': source},
            dag=dw_dag
        )
        extract_tasks.append(task)
    
    # 2. TRANSFORM LAYER - Customer Dimension SCD Type 2
    transform_customer_dim = PostgresOperator(
        task_id='transform_customer_dimension',
        postgres_conn_id='staging_db',
        sql="""
            -- SCD Type 2 for Customer Dimension
            WITH staged_customers AS (
                SELECT 
                    customer_id,
                    customer_name,
                    customer_email,
                    created_at,
                    updated_at,
                    etl_date
                FROM staging.customers
            ),
            existing_customers AS (
                SELECT 
                    dim_customer_key,
                    customer_id,
                    customer_name,
                    customer_email,
                    valid_from,
                    valid_to,
                    is_current
                FROM dw.dim_customer
                WHERE is_current = true
            ),
            changes AS (
                SELECT 
                    sc.*,
                    ec.dim_customer_key,
                    CASE 
                        WHEN ec.customer_id IS NULL THEN 'NEW'
                        WHEN sc.customer_name != ec.customer_name 
                          OR sc.customer_email != ec.customer_email THEN 'CHANGED'
                        ELSE 'UNCHANGED'
                    END as change_type
                FROM staged_customers sc
                LEFT JOIN existing_customers ec 
                    ON sc.customer_id = ec.customer_id
            )
            -- Insert new and changed records
            INSERT INTO dw.dim_customer (
                customer_id,
                customer_name, 
                customer_email,
                valid_from,
                valid_to,
                is_current
            )
            SELECT 
                customer_id,
                customer_name,
                customer_email,
                etl_date as valid_from,
                '9999-12-31'::date as valid_to,
                true as is_current
            FROM changes
            WHERE change_type IN ('NEW', 'CHANGED');
            
            -- Close old records
            UPDATE dw.dim_customer
            SET 
                valid_to = CURRENT_DATE - INTERVAL '1 day',
                is_current = false
            WHERE customer_id IN (
                SELECT customer_id 
                FROM changes 
                WHERE change_type = 'CHANGED'
            )
            AND is_current = true;
        """,
        dag=dw_dag
    )
    
    # 3. FACT TABLE LOAD
    load_fact_sales = PostgresOperator(
        task_id='load_fact_sales',
        postgres_conn_id='staging_db',
        sql="""
            INSERT INTO dw.fact_sales (
                order_date_key,
                customer_key,
                product_key,
                order_id,
                quantity,
                unit_price,
                total_amount,
                etl_timestamp
            )
            SELECT 
                TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER as order_date_key,
                dc.dim_customer_key as customer_key,
                dp.dim_product_key as product_key,
                o.order_id,
                od.quantity,
                od.unit_price,
                od.quantity * od.unit_price as total_amount,
                CURRENT_TIMESTAMP as etl_timestamp
            FROM staging.orders o
            JOIN staging.order_details od ON o.order_id = od.order_id
            JOIN dw.dim_customer dc ON o.customer_id = dc.customer_id
                AND dc.is_current = true
            JOIN dw.dim_product dp ON od.product_id = dp.product_id
                AND dp.is_current = true
            WHERE o.etl_date = '{{ ds }}';
        """,
        dag=dw_dag
    )
    
    # 4. DATA QUALITY CHECKS
    def run_data_quality_checks(**context):
        """Comprehensive DQ checks"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        pg_hook = PostgresHook(postgres_conn_id='dw_db')
        execution_date = context['execution_date'].strftime('%Y-%m-%d')
        
        checks = [
            {
                'name': 'fact_sales_completeness',
                'sql': f"""
                    SELECT COUNT(*) 
                    FROM dw.fact_sales 
                    WHERE etl_timestamp::date = '{execution_date}'
                """,
                'expected_min': 100  # Expect at least 100 sales
            },
            {
                'name': 'orphaned_facts',
                'sql': """
                    SELECT COUNT(*)
                    FROM dw.fact_sales f
                    LEFT JOIN dw.dim_customer c 
                        ON f.customer_key = c.dim_customer_key
                    WHERE c.dim_customer_key IS NULL
                """,
                'expected_max': 0  # No orphaned records
            },
            {
                'name': 'duplicate_dimensions',
                'sql': """
                    SELECT COUNT(*)
                    FROM (
                        SELECT customer_id, COUNT(*)
                        FROM dw.dim_customer
                        WHERE is_current = true
                        GROUP BY customer_id
                        HAVING COUNT(*) > 1
                    ) dups
                """,
                'expected_max': 0
            }
        ]
        
        failed_checks = []
        
        for check in checks:
            result = pg_hook.get_first(check['sql'])[0]
            
            if 'expected_min' in check and result < check['expected_min']:
                failed_checks.append(f"{check['name']}: {result} < {check['expected_min']}")
            
            if 'expected_max' in check and result > check['expected_max']:
                failed_checks.append(f"{check['name']}: {result} > {check['expected_max']}")
            
            logging.info(f"Check {check['name']}: {result}")
        
        if failed_checks:
            raise ValueError(f"DQ Checks failed: {', '.join(failed_checks)}")
        
        return "All checks passed"
    
    dq_checks = PythonOperator(
        task_id='data_quality_checks',
        python_callable=run_data_quality_checks,
        dag=dw_dag
    )
    
    # 5. AGGREGATION LAYER
    refresh_aggregates = PostgresOperator(
        task_id='refresh_aggregate_tables',
        postgres_conn_id='dw_db',
        sql="""
            -- Daily sales summary
            INSERT INTO dw.agg_daily_sales
            SELECT 
                order_date_key,
                COUNT(DISTINCT customer_key) as unique_customers,
                COUNT(*) as total_orders,
                SUM(total_amount) as revenue,
                AVG(total_amount) as avg_order_value,
                CURRENT_TIMESTAMP as created_at
            FROM dw.fact_sales
            WHERE order_date_key = TO_CHAR('{{ ds }}'::date, 'YYYYMMDD')::INTEGER
            GROUP BY order_date_key;
            
            -- Customer lifetime value update
            INSERT INTO dw.customer_ltv (customer_key, total_orders, total_spent, last_order_date)
            SELECT 
                customer_key,
                COUNT(*) as total_orders,
                SUM(total_amount) as total_spent,
                MAX(order_date_key) as last_order_date
            FROM dw.fact_sales
            WHERE customer_key IN (
                SELECT DISTINCT customer_key 
                FROM dw.fact_sales 
                WHERE etl_timestamp::date = '{{ ds }}'
            )
            GROUP BY customer_key
            ON CONFLICT (customer_key) 
            DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_spent = EXCLUDED.total_spent,
                last_order_date = EXCLUDED.last_order_date;
        """,
        dag=dw_dag
    )
    
    # End marker
    end = DummyOperator(task_id='end')
    
    # Dependencies
    start >> extract_tasks >> transform_customer_dim >> load_fact_sales >> dq_checks >> refresh_aggregates >> end


# 7.2 EVENT-DRIVEN PIPELINE WITH SENSORS
# ----------------------------------------
"""
Scenario: Wait for file arrival, process, and notify
"""

from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator

with DAG(
    'event_driven_file_processing',
    default_args=default_args,
    schedule_interval=None,  # Triggered externally
    start_date=datetime(2024, 1, 1),
    tags=['event-driven']
) as event_dag:
    
    # Wait for file
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/data/incoming/sales_{{ ds }}.csv',
        fs_conn_id='fs_default',
        poke_interval=300,  # Check every 5 minutes
        timeout=3600 * 4,  # 4 hour timeout
        mode='reschedule',  # Don't hold worker slot
        dag=event_dag
    )
    
    def process_file(**context):
        """Process the arrived file"""
        import pandas as pd
        import os
        
        filepath = f"/data/incoming/sales_{context['ds']}.csv"
        
        # Read and validate
        df = pd.read_csv(filepath)
        
        # Validation
        required_columns = ['date', 'product_id', 'quantity', 'price']
        missing_cols = set(required_columns) - set(df.columns)
        
        if missing_cols:
            raise ValueError(f"Missing columns: {missing_cols}")
        
        # Data quality checks
        if df['quantity'].isna().any():
            raise ValueError("Found null quantities")
        
        if (df['price'] < 0).any():
            raise ValueError("Found negative prices")
        
        # Process
        df['total'] = df['quantity'] * df['price']
        df['processed_at'] = datetime.now()
        
        # Save to database
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        df.to_sql(
            'processed_sales',
            pg_hook.get_sqlalchemy_engine(),
            if_exists='append',
            index=False
        )
        
        # Archive file
        archive_path = f"/data/archive/sales_{context['ds']}.csv"
        os.rename(filepath, archive_path)
        
        # Return metrics for notification
        return {
            'records_processed': len(df),
            'total_revenue': df['total'].sum(),
            'avg_order_value': df['total'].mean()
        }
    
    process = PythonOperator(
        task_id='process_file',
        python_callable=process_file,
        dag=event_dag
    )
    
    def build_email_content(**context):
        """Build email with processing results"""
        ti = context['task_instance']
        metrics = ti.xcom_pull(task_ids='process_file')
        
        html_content = f"""
        <h3>Daily Sales Processing Complete</h3>
        <p>Date: {context['ds']}</p>
        <ul>
            <li>Records Processed: {metrics['records_processed']:,}</li>
            <li>Total Revenue: ${metrics['total_revenue']:,.2f}</li>
            <li>Average Order Value: ${metrics['avg_order_value']:.2f}</li>
        </ul>
        """
        
        return html_content
    
    notify = EmailOperator(
        task_id='send_notification',
        to=['ray@company.com'],
        subject='Sales Processing Complete - {{ ds }}',
        html_content="{{ task_instance.xcom_pull(task_ids='build_email') }}",
        dag=event_dag
    )
    
    wait_for_file >> process >> notify


# 7.3 DYNAMIC DAG WITH CONFIGURATION
# ------------------------------------
"""
Scenario: Create DAGs from YAML configuration
Useful for: Multiple similar pipelines with different parameters
"""

import yaml
from pathlib import Path

# Configuration file example (config/pipelines.yaml):
"""
pipelines:
  - name: customer_pipeline
    schedule: '@daily'
    source_table: customers
    target_schema: dw
    transformations:
      - deduplicate
      - standardize_phone
      - validate_email
  
  - name: product_pipeline
    schedule: '@weekly'
    source_table: products
    target_schema: dw
    transformations:
      - deduplicate
      - normalize_prices
"""

def load_pipeline_configs():
    """Load pipeline configurations from YAML"""
    config_path = Path(__file__).parent / 'config' / 'pipelines.yaml'
    with open(config_path) as f:
        return yaml.safe_load(f)

def create_transformation_task(transform_name):
    """Factory for transformation functions"""
    
    def deduplicate(**context):
        # Deduplication logic
        pass
    
    def standardize_phone(**context):
        # Phone standardization logic
        pass
    
    def validate_email(**context):
        # Email validation logic
        pass
    
    transforms = {
        'deduplicate': deduplicate,
        'standardize_phone': standardize_phone,
        'validate_email': validate_email,
        # Add more as needed
    }
    
    return transforms.get(transform_name, lambda **c: None)

# Create DAGs from configuration
configs = load_pipeline_configs()

for pipeline_config in configs['pipelines']:
    dag_id = pipeline_config['name']
    
    # Create DAG
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=pipeline_config['schedule'],
        catchup=False
    )
    
    with dag:
        # Extract task
        extract = PostgresOperator(
            task_id='extract',
            sql=f"SELECT * FROM {pipeline_config['source_table']}",
            postgres_conn_id='source_db'
        )
        
        # Create transformation tasks dynamically
        prev_task = extract
        for transform in pipeline_config['transformations']:
            task = PythonOperator(
                task_id=f'transform_{transform}',
                python_callable=create_transformation_task(transform)
            )
            prev_task >> task
            prev_task = task
        
        # Load task
        load = PostgresOperator(
            task_id='load',
            sql=f"""
                INSERT INTO {pipeline_config['target_schema']}.{pipeline_config['source_table']}
                SELECT * FROM staging.{pipeline_config['source_table']}
            """,
            postgres_conn_id='target_db'
        )
        
        prev_task >> load
    
    # Register DAG
    globals()[dag_id] = dag


# 7.4 MACHINE LEARNING PIPELINE
# ------------------------------
"""
Scenario: Train model, evaluate, and deploy if performance improves
"""

with DAG(
    'ml_model_training',
    default_args=default_args,
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    tags=['ml', 'training']
) as ml_dag:
    
    def prepare_training_data(**context):
        """Prepare features for training"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import pandas as pd
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get training data
        query = """
            SELECT 
                customer_age,
                total_purchases,
                days_since_last_purchase,
                avg_order_value,
                churned
            FROM ml.customer_features
            WHERE feature_date >= CURRENT_DATE - INTERVAL '90 days'
        """
        
        df = pg_hook.get_pandas_df(query)
        
        # Save for next task
        df.to_pickle('/tmp/training_data.pkl')
        
        return {'rows': len(df), 'features': list(df.columns)}
    
    def train_model(**context):
        """Train the ML model"""
        import pandas as pd
        from sklearn.model_selection import train_test_split
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score, roc_auc_score
        import joblib
        
        # Load data
        df = pd.read_pickle('/tmp/training_data.pkl')
        
        # Split features and target
        X = df.drop('churned', axis=1)
        y = df['churned']
        
        # Train/test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        
        # Train model
        model = RandomForestClassifier(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        
        # Save model
        model_path = f'/tmp/model_{context["ds"]}.pkl'
        joblib.dump(model, model_path)
        
        # Push metrics
        context['task_instance'].xcom_push(key='accuracy', value=accuracy)
        context['task_instance'].xcom_push(key='auc', value=auc)
        context['task_instance'].xcom_push(key='model_path', value=model_path)
        
        logging.info(f"Model trained - Accuracy: {accuracy:.3f}, AUC: {auc:.3f}")
        
        return {'accuracy': accuracy, 'auc': auc}
    
    def evaluate_and_deploy(**context):
        """Deploy model if it performs better than current"""
        import joblib
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ti = context['task_instance']
        new_auc = ti.xcom_pull(task_ids='train_model', key='auc')
        model_path = ti.xcom_pull(task_ids='train_model', key='model_path')
        
        # Get current model performance
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        current_auc = pg_hook.get_first(
            "SELECT auc FROM ml.model_registry WHERE is_current = true"
        )[0]
        
        if new_auc > current_auc:
            # Deploy new model
            import boto3
            s3 = boto3.client('s3')
            
            # Upload to S3
            s3_key = f"models/churn_model_{context['ds']}.pkl"
            s3.upload_file(model_path, 'ml-models-bucket', s3_key)
            
            # Update registry
            pg_hook.run(f"""
                UPDATE ml.model_registry SET is_current = false WHERE is_current = true;
                
                INSERT INTO ml.model_registry (
                    model_name, version, auc, s3_path, trained_date, is_current
                ) VALUES (
                    'churn_model', 
                    '{context["ds"]}',
                    {new_auc},
                    '{s3_key}',
                    CURRENT_DATE,
                    true
                );
            """)
            
            return f"Model deployed! New AUC: {new_auc:.3f} > Current: {current_auc:.3f}"
        else:
            return f"Model not deployed. New AUC: {new_auc:.3f} <= Current: {current_auc:.3f}"
    
    # Define tasks
    prepare = PythonOperator(
        task_id='prepare_training_data',
        python_callable=prepare_training_data
    )
    
    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )
    
    deploy = PythonOperator(
        task_id='evaluate_and_deploy',
        python_callable=evaluate_and_deploy
    )
    
    prepare >> train >> deploy


# 7.5 CROSS-DAG DEPENDENCIES
# ---------------------------
"""
Scenario: Coordinate multiple DAGs
"""

from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    'downstream_processing',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1)
) as downstream_dag:
    
    # Wait for upstream DAG to complete
    wait_for_upstream = ExternalTaskSensor(
        task_id='wait_for_etl',
        external_dag_id='data_warehouse_refresh',
        external_task_id='end',  # Wait for specific task
        mode='reschedule',
        timeout=3600,
        dag=downstream_dag
    )
    
    # Your downstream processing
    process = PostgresOperator(
        task_id='run_reports',
        sql="""
            -- Generate reports from refreshed DW
            INSERT INTO reports.daily_summary
            SELECT * FROM dw.generate_daily_report('{{ ds }}')
        """
    )
    
    wait_for_upstream >> process


# ============================================
# PART 8: IMDB DATASET EXAMPLES
# ============================================
"""
Using the public IMDB dataset as our source
Files available at: https://datasets.imdbws.com/
- title.basics.tsv.gz (basic info about titles)
- title.ratings.tsv.gz (IMDB ratings)
- title.crew.tsv.gz (directors and writers)
- name.basics.tsv.gz (people in the industry)
- title.principals.tsv.gz (principal cast/crew)
"""

# 8.1 IMDB DATA PIPELINE - COMPLETE ETL
# --------------------------------------
"""
Build a movie analytics data warehouse from IMDB data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import pandas as pd
import gzip
import requests

with DAG(
    'imdb_analytics_pipeline',
    default_args={
        'owner': 'ray',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True
    },
    description='Build movie analytics DW from IMDB data',
    schedule_interval='@weekly',  # IMDB updates weekly
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['imdb', 'movies', 'analytics']
) as imdb_dag:
    
    # 1. DOWNLOAD IMDB FILES
    def download_imdb_file(filename, **context):
        """Download and extract IMDB data files"""
        import os
        
        url = f'https://datasets.imdbws.com/{filename}.tsv.gz'
        local_path = f'/tmp/imdb_{filename}.tsv.gz'
        extracted_path = f'/tmp/imdb_{filename}.tsv'
        
        # Download file
        response = requests.get(url, stream=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        # Extract gz file
        with gzip.open(local_path, 'rb') as f_in:
            with open(extracted_path, 'wb') as f_out:
                f_out.write(f_in.read())
        
        # Get file size for monitoring
        file_size = os.path.getsize(extracted_path) / (1024 * 1024)  # MB
        
        logging.info(f"Downloaded {filename}: {file_size:.2f} MB")
        context['task_instance'].xcom_push(
            key=f'{filename}_path', 
            value=extracted_path
        )
        
        return extracted_path
    
    # Download tasks for each IMDB file
    download_titles = PythonOperator(
        task_id='download_title_basics',
        python_callable=download_imdb_file,
        op_kwargs={'filename': 'title.basics'},
        dag=imdb_dag
    )
    
    download_ratings = PythonOperator(
        task_id='download_title_ratings',
        python_callable=download_imdb_file,
        op_kwargs={'filename': 'title.ratings'},
        dag=imdb_dag
    )
    
    download_names = PythonOperator(
        task_id='download_name_basics',
        python_callable=download_imdb_file,
        op_kwargs={'filename': 'name.basics'},
        dag=imdb_dag
    )
    
    # 2. CREATE STAGING TABLES
    create_staging_tables = PostgresOperator(
        task_id='create_staging_tables',
        postgres_conn_id='postgres_default',
        sql="""
            -- Drop and recreate staging schema
            DROP SCHEMA IF EXISTS imdb_staging CASCADE;
            CREATE SCHEMA imdb_staging;
            
            -- Title basics staging
            CREATE TABLE imdb_staging.title_basics (
                tconst VARCHAR(20) PRIMARY KEY,
                title_type VARCHAR(50),
                primary_title TEXT,
                original_title TEXT,
                is_adult BOOLEAN,
                start_year INTEGER,
                end_year INTEGER,
                runtime_minutes INTEGER,
                genres TEXT
            );
            
            -- Ratings staging
            CREATE TABLE imdb_staging.title_ratings (
                tconst VARCHAR(20) PRIMARY KEY,
                average_rating DECIMAL(3,1),
                num_votes INTEGER
            );
            
            -- Names staging
            CREATE TABLE imdb_staging.name_basics (
                nconst VARCHAR(20) PRIMARY KEY,
                primary_name TEXT,
                birth_year INTEGER,
                death_year INTEGER,
                primary_profession TEXT,
                known_for_titles TEXT
            );
            
            -- Create indexes for joins
            CREATE INDEX idx_title_type ON imdb_staging.title_basics(title_type);
            CREATE INDEX idx_start_year ON imdb_staging.title_basics(start_year);
            CREATE INDEX idx_rating ON imdb_staging.title_ratings(average_rating);
        """,
        dag=imdb_dag
    )
    
    # 3. LOAD DATA TO STAGING
    def load_imdb_to_staging(table_name, **context):
        """Load IMDB TSV files to staging tables"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ti = context['task_instance']
        file_path = ti.xcom_pull(
            task_ids=f'download_{table_name}',
            key=f'{table_name}_path'
        )
        
        # Read TSV with specific settings for IMDB data
        df = pd.read_csv(
            file_path,
            sep='\t',
            na_values='\\N',  # IMDB uses \N for null
            low_memory=False
        )
        
        # Data cleaning based on table
        if table_name == 'title.basics':
            # Clean title basics
            df['isAdult'] = df['isAdult'].astype(bool)
            df['startYear'] = pd.to_numeric(df['startYear'], errors='coerce')
            df['endYear'] = pd.to_numeric(df['endYear'], errors='coerce')
            df['runtimeMinutes'] = pd.to_numeric(df['runtimeMinutes'], errors='coerce')
            
            # Rename columns to match our schema
            df.columns = ['tconst', 'title_type', 'primary_title', 
                         'original_title', 'is_adult', 'start_year', 
                         'end_year', 'runtime_minutes', 'genres']
            
            # Filter to only movies and recent years for manageable size
            df = df[(df['title_type'] == 'movie') & 
                   (df['start_year'] >= 2000) & 
                   (df['start_year'] <= 2024)]
        
        elif table_name == 'title.ratings':
            # Clean ratings
            df.columns = ['tconst', 'average_rating', 'num_votes']
            # Keep only well-rated movies with enough votes
            df = df[df['num_votes'] >= 1000]
        
        elif table_name == 'name.basics':
            # Clean names
            df['birthYear'] = pd.to_numeric(df['birthYear'], errors='coerce')
            df['deathYear'] = pd.to_numeric(df['deathYear'], errors='coerce')
            
            df.columns = ['nconst', 'primary_name', 'birth_year', 
                         'death_year', 'primary_profession', 'known_for_titles']
            
            # Keep only people with known movies
            df = df[df['known_for_titles'].notna()]
        
        # Load to staging
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Use chunks for large datasets
        chunk_size = 10000
        for i in range(0, len(df), chunk_size):
            chunk = df[i:i+chunk_size]
            chunk.to_sql(
                table_name.replace('.', '_'),
                engine,
                schema='imdb_staging',
                if_exists='append',
                index=False,
                method='multi'
            )
        
        logging.info(f"Loaded {len(df)} records to staging.{table_name}")
        return len(df)
    
    load_titles = PythonOperator(
        task_id='load_titles_to_staging',
        python_callable=load_imdb_to_staging,
        op_kwargs={'table_name': 'title.basics'},
        dag=imdb_dag
    )
    
    load_ratings = PythonOperator(
        task_id='load_ratings_to_staging',
        python_callable=load_imdb_to_staging,
        op_kwargs={'table_name': 'title.ratings'},
        dag=imdb_dag
    )
    
    # 4. CREATE DIMENSIONAL MODEL
    create_dim_model = PostgresOperator(
        task_id='create_dimensional_model',
        postgres_conn_id='postgres_default',
        sql="""
            -- Create analytics schema
            CREATE SCHEMA IF NOT EXISTS imdb_analytics;
            
            -- Date dimension
            CREATE TABLE IF NOT EXISTS imdb_analytics.dim_date (
                date_key INTEGER PRIMARY KEY,
                year INTEGER,
                decade INTEGER,
                century INTEGER
            );
            
            -- Movie dimension
            CREATE TABLE IF NOT EXISTS imdb_analytics.dim_movie (
                movie_key SERIAL PRIMARY KEY,
                tconst VARCHAR(20) UNIQUE,
                title TEXT,
                original_title TEXT,
                runtime_minutes INTEGER,
                genres TEXT[],
                is_adult BOOLEAN,
                load_date DATE DEFAULT CURRENT_DATE
            );
            
            -- Genre dimension (normalized from comma-separated)
            CREATE TABLE IF NOT EXISTS imdb_analytics.dim_genre (
                genre_key SERIAL PRIMARY KEY,
                genre_name VARCHAR(50) UNIQUE
            );
            
            -- Movie-Genre bridge table
            CREATE TABLE IF NOT EXISTS imdb_analytics.bridge_movie_genre (
                movie_key INTEGER REFERENCES imdb_analytics.dim_movie(movie_key),
                genre_key INTEGER REFERENCES imdb_analytics.dim_genre(genre_key),
                PRIMARY KEY (movie_key, genre_key)
            );
            
            -- Fact table
            CREATE TABLE IF NOT EXISTS imdb_analytics.fact_movie_ratings (
                rating_key SERIAL PRIMARY KEY,
                movie_key INTEGER REFERENCES imdb_analytics.dim_movie(movie_key),
                release_year_key INTEGER REFERENCES imdb_analytics.dim_date(date_key),
                average_rating DECIMAL(3,1),
                num_votes INTEGER,
                popularity_score DECIMAL(10,2),  -- calculated field
                etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Aggregate tables for performance
            CREATE TABLE IF NOT EXISTS imdb_analytics.agg_genre_yearly (
                genre_name VARCHAR(50),
                year INTEGER,
                avg_rating DECIMAL(3,1),
                total_movies INTEGER,
                total_votes BIGINT,
                PRIMARY KEY (genre_name, year)
            );
            
            CREATE TABLE IF NOT EXISTS imdb_analytics.agg_movie_trends (
                decade INTEGER,
                avg_runtime INTEGER,
                avg_rating DECIMAL(3,1),
                total_movies INTEGER,
                most_popular_genre VARCHAR(50),
                PRIMARY KEY (decade)
            );
        """,
        dag=imdb_dag
    )
    
    # 5. TRANSFORM AND LOAD DIMENSIONS
    transform_dimensions = PostgresOperator(
        task_id='transform_load_dimensions',
        postgres_conn_id='postgres_default',
        sql="""
            -- Populate date dimension
            INSERT INTO imdb_analytics.dim_date (date_key, year, decade, century)
            SELECT DISTINCT 
                start_year as date_key,
                start_year as year,
                (start_year / 10) * 10 as decade,
                (start_year / 100) * 100 as century
            FROM imdb_staging.title_basics
            WHERE start_year IS NOT NULL
            ON CONFLICT (date_key) DO NOTHING;
            
            -- Populate movie dimension
            INSERT INTO imdb_analytics.dim_movie (
                tconst, title, original_title, 
                runtime_minutes, genres, is_adult
            )
            SELECT 
                tconst,
                primary_title,
                original_title,
                runtime_minutes,
                string_to_array(genres, ',') as genres,
                is_adult
            FROM imdb_staging.title_basics tb
            WHERE EXISTS (
                SELECT 1 FROM imdb_staging.title_ratings tr 
                WHERE tr.tconst = tb.tconst
            )
            ON CONFLICT (tconst) DO UPDATE SET
                title = EXCLUDED.title,
                runtime_minutes = EXCLUDED.runtime_minutes;
            
            -- Populate genre dimension
            INSERT INTO imdb_analytics.dim_genre (genre_name)
            SELECT DISTINCT unnest(string_to_array(genres, ','))
            FROM imdb_staging.title_basics
            WHERE genres IS NOT NULL
            ON CONFLICT (genre_name) DO NOTHING;
            
            -- Populate movie-genre bridge
            INSERT INTO imdb_analytics.bridge_movie_genre (movie_key, genre_key)
            SELECT DISTINCT
                m.movie_key,
                g.genre_key
            FROM imdb_analytics.dim_movie m
            CROSS JOIN LATERAL unnest(m.genres) as genre_name
            JOIN imdb_analytics.dim_genre g ON g.genre_name = genre_name
            ON CONFLICT DO NOTHING;
        """,
        dag=imdb_dag
    )
    
    # 6. LOAD FACT TABLE
    load_facts = PostgresOperator(
        task_id='load_fact_table',
        postgres_conn_id='postgres_default',
        sql="""
            -- Load fact table with calculated popularity score
            INSERT INTO imdb_analytics.fact_movie_ratings (
                movie_key,
                release_year_key,
                average_rating,
                num_votes,
                popularity_score
            )
            SELECT 
                m.movie_key,
                tb.start_year as release_year_key,
                tr.average_rating,
                tr.num_votes,
                -- Popularity score: combination of rating and votes
                (tr.average_rating * LOG(tr.num_votes + 1)) as popularity_score
            FROM imdb_staging.title_ratings tr
            JOIN imdb_staging.title_basics tb ON tr.tconst = tb.tconst
            JOIN imdb_analytics.dim_movie m ON m.tconst = tr.tconst
            WHERE tb.start_year IS NOT NULL
            ON CONFLICT DO NOTHING;
        """,
        dag=imdb_dag
    )
    
    # 7. BUILD AGGREGATES
    build_aggregates = PostgresOperator(
        task_id='build_aggregate_tables',
        postgres_conn_id='postgres_default',
        sql="""
            -- Genre performance by year
            TRUNCATE imdb_analytics.agg_genre_yearly;
            INSERT INTO imdb_analytics.agg_genre_yearly
            SELECT 
                g.genre_name,
                d.year,
                AVG(f.average_rating) as avg_rating,
                COUNT(DISTINCT f.movie_key) as total_movies,
                SUM(f.num_votes) as total_votes
            FROM imdb_analytics.fact_movie_ratings f
            JOIN imdb_analytics.dim_date d ON f.release_year_key = d.date_key
            JOIN imdb_analytics.bridge_movie_genre bg ON f.movie_key = bg.movie_key
            JOIN imdb_analytics.dim_genre g ON bg.genre_key = g.genre_key
            GROUP BY g.genre_name, d.year;
            
            -- Decade trends
            TRUNCATE imdb_analytics.agg_movie_trends;
            INSERT INTO imdb_analytics.agg_movie_trends
            WITH decade_stats AS (
                SELECT 
                    d.decade,
                    AVG(m.runtime_minutes) as avg_runtime,
                    AVG(f.average_rating) as avg_rating,
                    COUNT(DISTINCT f.movie_key) as total_movies
                FROM imdb_analytics.fact_movie_ratings f
                JOIN imdb_analytics.dim_movie m ON f.movie_key = m.movie_key
                JOIN imdb_analytics.dim_date d ON f.release_year_key = d.date_key
                GROUP BY d.decade
            ),
            popular_genres AS (
                SELECT DISTINCT ON (d.decade)
                    d.decade,
                    g.genre_name
                FROM imdb_analytics.fact_movie_ratings f
                JOIN imdb_analytics.dim_date d ON f.release_year_key = d.date_key
                JOIN imdb_analytics.bridge_movie_genre bg ON f.movie_key = bg.movie_key
                JOIN imdb_analytics.dim_genre g ON bg.genre_key = g.genre_key
                GROUP BY d.decade, g.genre_name
                ORDER BY d.decade, COUNT(*) DESC
            )
            SELECT 
                ds.decade,
                ds.avg_runtime,
                ds.avg_rating,
                ds.total_movies,
                pg.genre_name as most_popular_genre
            FROM decade_stats ds
            LEFT JOIN popular_genres pg ON ds.decade = pg.decade;
        """,
        dag=imdb_dag
    )
    
    # 8. DATA QUALITY CHECKS
    def run_quality_checks(**context):
        """Validate the loaded data"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        checks = [
            {
                'name': 'movies_loaded',
                'sql': 'SELECT COUNT(*) FROM imdb_analytics.dim_movie',
                'min_expected': 1000
            },
            {
                'name': 'ratings_loaded',
                'sql': 'SELECT COUNT(*) FROM imdb_analytics.fact_movie_ratings',
                'min_expected': 1000
            },
            {
                'name': 'genres_populated',
                'sql': 'SELECT COUNT(DISTINCT genre_name) FROM imdb_analytics.dim_genre',
                'min_expected': 10
            },
            {
                'name': 'no_orphaned_facts',
                'sql': """
                    SELECT COUNT(*) 
                    FROM imdb_analytics.fact_movie_ratings f
                    LEFT JOIN imdb_analytics.dim_movie m ON f.movie_key = m.movie_key
                    WHERE m.movie_key IS NULL
                """,
                'max_expected': 0
            }
        ]
        
        for check in checks:
            result = pg_hook.get_first(check['sql'])[0]
            
            if 'min_expected' in check and result < check['min_expected']:
                raise ValueError(f"Check {check['name']} failed: {result} < {check['min_expected']}")
            
            if 'max_expected' in check and result > check['max_expected']:
                raise ValueError(f"Check {check['name']} failed: {result} > {check['max_expected']}")
            
            logging.info(f"✓ Check {check['name']}: {result}")
        
        return "All checks passed"
    
    quality_checks = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_quality_checks,
        dag=imdb_dag
    )
    
    # 9. GENERATE INSIGHTS REPORT
    generate_insights = PostgresOperator(
        task_id='generate_insights',
        postgres_conn_id='postgres_default',
        sql="""
            -- Create insights table
            CREATE TABLE IF NOT EXISTS imdb_analytics.insights_{{ ds_nodash }} AS
            WITH top_movies AS (
                SELECT 
                    m.title,
                    d.year,
                    f.average_rating,
                    f.num_votes,
                    f.popularity_score
                FROM imdb_analytics.fact_movie_ratings f
                JOIN imdb_analytics.dim_movie m ON f.movie_key = m.movie_key
                JOIN imdb_analytics.dim_date d ON f.release_year_key = d.date_key
                ORDER BY f.popularity_score DESC
                LIMIT 100
            ),
            genre_evolution AS (
                SELECT 
                    genre_name,
                    MIN(year) as first_popular_year,
                    MAX(year) as last_popular_year,
                    AVG(avg_rating) as overall_avg_rating
                FROM imdb_analytics.agg_genre_yearly
                GROUP BY genre_name
                HAVING COUNT(*) > 5
            )
            SELECT 
                'report_date' as metric_type,
                '{{ ds }}' as metric_value
            UNION ALL
            SELECT 
                'total_movies_analyzed',
                COUNT(*)::TEXT
            FROM imdb_analytics.dim_movie
            UNION ALL
            SELECT 
                'highest_rated_movie',
                title || ' (' || average_rating || ')'
            FROM top_movies
            LIMIT 1;
            
            -- Log summary
            DO $
            DECLARE
                total_movies INTEGER;
                avg_rating DECIMAL;
            BEGIN
                SELECT COUNT(*), AVG(average_rating)
                INTO total_movies, avg_rating
                FROM imdb_analytics.fact_movie_ratings;
                
                RAISE NOTICE 'Pipeline complete: % movies, avg rating: %', 
                            total_movies, ROUND(avg_rating, 2);
            END $;
        """,
        dag=imdb_dag
    )
    
    # DEFINE DEPENDENCIES
    download_tasks = [download_titles, download_ratings, download_names]
    load_tasks = [load_titles, load_ratings]
    
    download_tasks >> create_staging_tables >> load_tasks
    load_tasks >> create_dim_model >> transform_dimensions >> load_facts
    load_facts >> build_aggregates >> quality_checks >> generate_insights


# 8.2 IMDB RECOMMENDATION ENGINE PIPELINE
# ----------------------------------------
"""
Build a movie recommendation system using IMDB data
"""

with DAG(
    'imdb_recommendation_engine',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    tags=['imdb', 'ml', 'recommendations']
) as rec_dag:
    
    def build_similarity_matrix(**context):
        """Build movie similarity based on genres and ratings"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sklearn.metrics.pairwise import cosine_similarity
        import numpy as np
        
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get movie features
        query = """
            SELECT 
                m.movie_key,
                m.tconst,
                m.title,
                ARRAY_AGG(DISTINCT g.genre_name) as genres,
                AVG(f.average_rating) as avg_rating,
                AVG(f.num_votes) as avg_votes
            FROM imdb_analytics.dim_movie m
            JOIN imdb_analytics.fact_movie_ratings f ON m.movie_key = f.movie_key
            LEFT JOIN imdb_analytics.bridge_movie_genre bg ON m.movie_key = bg.movie_key
            LEFT JOIN imdb_analytics.dim_genre g ON bg.genre_key = g.genre_key
            GROUP BY m.movie_key, m.tconst, m.title
            HAVING COUNT(f.rating_key) > 0
        """
        
        df = pg_hook.get_pandas_df(query)
        
        # Create genre feature matrix
        all_genres = set()
        for genres in df['genres']:
            if genres:
                all_genres.update(genres)
        
        genre_features = []
        for genres in df['genres']:
            features = [1 if g in genres else 0 for g in sorted(all_genres)]
            genre_features.append(features)
        
        # Combine with rating features
        rating_features = df[['avg_rating', 'avg_votes']].values
        rating_features = (rating_features - rating_features.mean(axis=0)) / rating_features.std(axis=0)
        
        # Combine all features
        all_features = np.hstack([genre_features, rating_features])
        
        # Calculate similarity
        similarity_matrix = cosine_similarity(all_features)
        
        # Store top N similar movies for each movie
        recommendations = []
        for i, movie in df.iterrows():
            # Get top 10 similar movies (excluding itself)
            similar_indices = similarity_matrix[i].argsort()[-11:-1][::-1]
            
            for j in similar_indices:
                recommendations.append({
                    'movie_id': movie['tconst'],
                    'movie_title': movie['title'],
                    'recommended_id': df.iloc[j]['tconst'],
                    'recommended_title': df.iloc[j]['title'],
                    'similarity_score': similarity_matrix[i][j]
                })
        
        # Save recommendations
        rec_df = pd.DataFrame(recommendations)
        rec_df.to_sql(
            'movie_recommendations',
            pg_hook.get_sqlalchemy_engine(),
            schema='imdb_analytics',
            if_exists='replace',
            index=False
        )
        
        return len(recommendations)
    
    build_recommendations = PythonOperator(
        task_id='build_similarity_matrix',
        python_callable=build_similarity_matrix,
        dag=rec_dag
    )


# ============================================
# PART 9: MODERN DATA STACK WITH SNOWFLAKE + DBT + ASTRONOMER
# ============================================
"""
Production-ready pipeline using:
- Astronomer (Airflow platform)
- Snowflake (Data Warehouse) 
- dbt (Transformation)
- Tableau (BI/Visualization)
- IMDB data as source
"""

# 9.1 ASTRONOMER + SNOWFLAKE + DBT SETUP
# ----------------------------------------
"""
Prerequisites:
1. Astronomer CLI installed: curl -sSL https://install.astronomer.io | sudo bash
2. Snowflake account with warehouse, database, and schema
3. dbt project initialized with Snowflake profile
4. Tableau Desktop/Server with Snowflake connector
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
    DbtCloudGetJobRunArtifactOperator
)
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from datetime import datetime, timedelta
import pandas as pd

# 9.2 COMPLETE MODERN DATA STACK PIPELINE
# ----------------------------------------
with DAG(
    'modern_data_stack_imdb',
    default_args={
        'owner': 'ray',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email': ['ray@company.com'],
        'email_on_failure': True,
    },
    description='Modern Data Stack: IMDB → Snowflake → dbt → Tableau',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['snowflake', 'dbt', 'imdb', 'tableau', 'astronomer']
) as dag:
    
    # 1. SNOWFLAKE SETUP - Create schemas and tables
    setup_snowflake = SnowflakeOperator(
        task_id='setup_snowflake_environment',
        snowflake_conn_id='snowflake_default',
        sql="""
            -- Create schemas for different layers
            USE WAREHOUSE COMPUTE_WH;
            USE DATABASE ANALYTICS;
            
            CREATE SCHEMA IF NOT EXISTS RAW_IMDB;
            CREATE SCHEMA IF NOT EXISTS STAGING;
            CREATE SCHEMA IF NOT EXISTS ANALYTICS;
            CREATE SCHEMA IF NOT EXISTS TABLEAU_SERVING;
            
            -- Create raw tables for IMDB data
            CREATE TABLE IF NOT EXISTS RAW_IMDB.TITLE_BASICS (
                TCONST VARCHAR(20),
                TITLE_TYPE VARCHAR(50),
                PRIMARY_TITLE VARCHAR(500),
                ORIGINAL_TITLE VARCHAR(500),
                IS_ADULT BOOLEAN,
                START_YEAR NUMBER,
                END_YEAR NUMBER,
                RUNTIME_MINUTES NUMBER,
                GENRES VARCHAR(200),
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
            
            CREATE TABLE IF NOT EXISTS RAW_IMDB.TITLE_RATINGS (
                TCONST VARCHAR(20),
                AVERAGE_RATING DECIMAL(3,1),
                NUM_VOTES NUMBER,
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
            
            CREATE TABLE IF NOT EXISTS RAW_IMDB.NAME_BASICS (
                NCONST VARCHAR(20),
                PRIMARY_NAME VARCHAR(200),
                BIRTH_YEAR NUMBER,
                DEATH_YEAR NUMBER,
                PRIMARY_PROFESSION VARCHAR(500),
                KNOWN_FOR_TITLES VARCHAR(500),
                LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
            
            -- Create staging tables with better data types
            CREATE TABLE IF NOT EXISTS STAGING.STG_MOVIES (
                MOVIE_ID VARCHAR(20),
                TITLE VARCHAR(500),
                RELEASE_YEAR NUMBER,
                RUNTIME_MINUTES NUMBER,
                GENRES ARRAY,
                IS_ADULT BOOLEAN,
                AVERAGE_RATING DECIMAL(3,1),
                NUM_VOTES NUMBER,
                POPULARITY_SCORE DECIMAL(10,2),
                DW_INSERT_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            );
            
            -- Create file format for loading
            CREATE FILE FORMAT IF NOT EXISTS RAW_IMDB.TSV_FORMAT
                TYPE = CSV
                FIELD_DELIMITER = '\t'
                SKIP_HEADER = 1
                NULL_IF = ('\\N', 'NULL', '')
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                COMPRESSION = GZIP;
            
            -- Create stage for S3/Azure/GCS
            CREATE STAGE IF NOT EXISTS RAW_IMDB.IMDB_STAGE
                FILE_FORMAT = RAW_IMDB.TSV_FORMAT;
        """,
        dag=dag
    )
    
    # 2. EXTRACT AND LOAD TO SNOWFLAKE
    def load_imdb_to_snowflake(**context):
        """Download IMDB data and load to Snowflake"""
        import requests
        import gzip
        from snowflake.connector import connect
        from snowflake.connector.pandas_tools import write_pandas
        
        # Download IMDB files
        files = {
            'title.basics': 'https://datasets.imdbws.com/title.basics.tsv.gz',
            'title.ratings': 'https://datasets.imdbws.com/title.ratings.tsv.gz',
            'name.basics': 'https://datasets.imdbws.com/name.basics.tsv.gz'
        }
        
        # Get Snowflake connection
        sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        for file_name, url in files.items():
            logging.info(f"Processing {file_name}")
            
            # Download and extract
            response = requests.get(url, stream=True)
            df = pd.read_csv(
                gzip.GzipFile(fileobj=response.raw),
                sep='\t',
                na_values='\\N',
                low_memory=False
            )
            
            # Clean column names for Snowflake
            df.columns = [col.upper().replace('.', '_') for col in df.columns]
            
            # Filter to reduce size (movies from 2010+)
            if file_name == 'title.basics':
                df = df[(df['TITLETYPE'] == 'movie') & 
                       (pd.to_numeric(df['STARTYEAR'], errors='coerce') >= 2010)]
                
                # Prepare for Snowflake
                df.rename(columns={
                    'TITLETYPE': 'TITLE_TYPE',
                    'PRIMARYTITLE': 'PRIMARY_TITLE',
                    'ORIGINALTITLE': 'ORIGINAL_TITLE',
                    'ISADULT': 'IS_ADULT',
                    'STARTYEAR': 'START_YEAR',
                    'ENDYEAR': 'END_YEAR',
                    'RUNTIMEMINUTES': 'RUNTIME_MINUTES'
                }, inplace=True)
                
                df['IS_ADULT'] = df['IS_ADULT'].astype(bool)
                
                table_name = 'TITLE_BASICS'
                
            elif file_name == 'title.ratings':
                df.rename(columns={
                    'AVERAGERATING': 'AVERAGE_RATING',
                    'NUMVOTES': 'NUM_VOTES'
                }, inplace=True)
                
                # Keep only well-voted movies
                df = df[pd.to_numeric(df['NUM_VOTES'], errors='coerce') >= 1000]
                
                table_name = 'TITLE_RATINGS'
                
            elif file_name == 'name.basics':
                df.rename(columns={
                    'PRIMARYNAME': 'PRIMARY_NAME',
                    'BIRTHYEAR': 'BIRTH_YEAR',
                    'DEATHYEAR': 'DEATH_YEAR',
                    'PRIMARYPROFESSION': 'PRIMARY_PROFESSION',
                    'KNOWNFORTITLES': 'KNOWN_FOR_TITLES'
                }, inplace=True)
                
                # Keep only relevant people
                df = df[df['KNOWN_FOR_TITLES'].notna()]
                df = df.head(10000)  # Limit for demo
                
                table_name = 'NAME_BASICS'
            
            # Load to Snowflake using pandas connector
            with sf_hook.get_conn() as conn:
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=table_name,
                    database='ANALYTICS',
                    schema='RAW_IMDB',
                    auto_create_table=False,
                    overwrite=True
                )
                
                logging.info(f"Loaded {nrows} rows to RAW_IMDB.{table_name}")
        
        return "IMDB data loaded to Snowflake"
    
    load_to_snowflake = PythonOperator(
        task_id='load_imdb_to_snowflake',
        python_callable=load_imdb_to_snowflake,
        dag=dag
    )
    
    # 3. DBT TRANSFORMATIONS (using Cosmos)
    # --------------------------------------
    """
    dbt project structure:
    imdb_analytics/
    ├── models/
    │   ├── staging/
    │   │   ├── stg_movies.sql
    │   │   ├── stg_ratings.sql
    │   │   └── stg_names.sql
    │   ├── intermediate/
    │   │   ├── int_movies_enriched.sql
    │   │   └── int_genre_normalized.sql
    │   ├── marts/
    │   │   ├── fct_movie_ratings.sql
    │   │   ├── dim_movies.sql
    │   │   ├── dim_genres.sql
    │   │   └── dim_dates.sql
    │   └── tableau/
    │       ├── tableau_movie_dashboard.sql
    │       └── tableau_genre_trends.sql
    └── dbt_project.yml
    """
    
    # Using Cosmos for dbt integration
    dbt_transformations = DbtTaskGroup(
        group_id='dbt_transformations',
        project_config=ProjectConfig(
            dbt_project_path='/usr/local/airflow/dags/dbt/imdb_analytics',
        ),
        profile_config=ProfileConfig(
            profile_name='snowflake',
            target_name='prod',
            profiles_yml_filepath='/usr/local/airflow/dags/dbt/.dbt/profiles.yml'
        ),
        operator_args={
            'install_deps': True,
            'full_refresh': False,
        },
        dag=dag
    )
    
    # Alternative: Using dbt Cloud
    run_dbt_cloud = DbtCloudRunJobOperator(
        task_id='run_dbt_cloud_job',
        dbt_cloud_conn_id='dbt_cloud_default',
        job_id=12345,  # Your dbt Cloud job ID
        check_interval=30,
        timeout=3600,
        dag=dag
    )
    
    # 4. SNOWFLAKE POST-DBT OPTIMIZATION
    optimize_snowflake = SnowflakeOperator(
        task_id='optimize_snowflake_tables',
        snowflake_conn_id='snowflake_default',
        sql="""
            -- Create clustering keys for better performance
            ALTER TABLE ANALYTICS.FCT_MOVIE_RATINGS 
                CLUSTER BY (RELEASE_YEAR, GENRE_KEY);
            
            -- Create materialized views for Tableau
            CREATE OR REPLACE MATERIALIZED VIEW TABLEAU_SERVING.MV_GENRE_TRENDS AS
            SELECT 
                g.GENRE_NAME,
                d.YEAR,
                d.DECADE,
                COUNT(DISTINCT f.MOVIE_KEY) as MOVIE_COUNT,
                AVG(f.AVERAGE_RATING) as AVG_RATING,
                SUM(f.NUM_VOTES) as TOTAL_VOTES,
                AVG(f.POPULARITY_SCORE) as AVG_POPULARITY
            FROM ANALYTICS.FCT_MOVIE_RATINGS f
            JOIN ANALYTICS.DIM_GENRES g ON f.GENRE_KEY = g.GENRE_KEY
            JOIN ANALYTICS.DIM_DATES d ON f.DATE_KEY = d.DATE_KEY
            GROUP BY 1, 2, 3;
            
            CREATE OR REPLACE MATERIALIZED VIEW TABLEAU_SERVING.MV_TOP_MOVIES AS
            SELECT 
                m.TITLE,
                m.RELEASE_YEAR,
                m.RUNTIME_MINUTES,
                f.AVERAGE_RATING,
                f.NUM_VOTES,
                f.POPULARITY_SCORE,
                LISTAGG(g.GENRE_NAME, ', ') as GENRES
            FROM ANALYTICS.FCT_MOVIE_RATINGS f
            JOIN ANALYTICS.DIM_MOVIES m ON f.MOVIE_KEY = m.MOVIE_KEY
            LEFT JOIN ANALYTICS.BRIDGE_MOVIE_GENRE bg ON m.MOVIE_KEY = bg.MOVIE_KEY
            LEFT JOIN ANALYTICS.DIM_GENRES g ON bg.GENRE_KEY = g.GENRE_KEY
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY f.POPULARITY_SCORE DESC
            LIMIT 1000;
            
            -- Create row access policies for Tableau
            CREATE ROW ACCESS POLICY IF NOT EXISTS TABLEAU_SERVING.TABLEAU_ACCESS_POLICY
                AS (IS_TABLEAU_USER BOOLEAN) RETURNS BOOLEAN ->
                IS_TABLEAU_USER = TRUE;
            
            -- Grant Tableau service account access
            GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TABLEAU_ROLE;
            GRANT USAGE ON DATABASE ANALYTICS TO ROLE TABLEAU_ROLE;
            GRANT USAGE ON SCHEMA TABLEAU_SERVING TO ROLE TABLEAU_ROLE;
            GRANT SELECT ON ALL VIEWS IN SCHEMA TABLEAU_SERVING TO ROLE TABLEAU_ROLE;
        """,
        dag=dag
    )
    
    # 5. DATA QUALITY CHECKS WITH GREAT EXPECTATIONS
    def run_data_quality_checks(**context):
        """Run quality checks on transformed data"""
        sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        quality_checks = [
            {
                'name': 'fact_table_freshness',
                'query': """
                    SELECT MAX(DW_INSERT_DATE) 
                    FROM ANALYTICS.FCT_MOVIE_RATINGS
                """,
                'check': lambda x: (datetime.now() - x).days < 1
            },
            {
                'name': 'no_null_keys',
                'query': """
                    SELECT COUNT(*) 
                    FROM ANALYTICS.FCT_MOVIE_RATINGS
                    WHERE MOVIE_KEY IS NULL OR DATE_KEY IS NULL
                """,
                'check': lambda x: x == 0
            },
            {
                'name': 'rating_bounds',
                'query': """
                    SELECT COUNT(*)
                    FROM ANALYTICS.FCT_MOVIE_RATINGS
                    WHERE AVERAGE_RATING < 0 OR AVERAGE_RATING > 10
                """,
                'check': lambda x: x == 0
            },
            {
                'name': 'genre_completeness',
                'query': """
                    SELECT COUNT(DISTINCT MOVIE_KEY)
                    FROM ANALYTICS.DIM_MOVIES m
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ANALYTICS.BRIDGE_MOVIE_GENRE bg
                        WHERE bg.MOVIE_KEY = m.MOVIE_KEY
                    )
                """,
                'check': lambda x: x < 100  # Allow some movies without genres
            }
        ]
        
        failed_checks = []
        with sf_hook.get_conn() as conn:
            cursor = conn.cursor()
            
            for check in quality_checks:
                cursor.execute(check['query'])
                result = cursor.fetchone()[0]
                
                if not check['check'](result):
                    failed_checks.append(f"{check['name']}: {result}")
                else:
                    logging.info(f"✓ {check['name']} passed")
        
        if failed_checks:
            raise ValueError(f"Quality checks failed: {', '.join(failed_checks)}")
        
        return "All quality checks passed"
    
    quality_checks = PythonOperator(
        task_id='run_quality_checks',
        python_callable=run_data_quality_checks,
        dag=dag
    )
    
    # 6. REFRESH TABLEAU EXTRACTS
    refresh_tableau = BashOperator(
        task_id='refresh_tableau_extracts',
        bash_command="""
            # Using Tableau's REST API or tabcmd
            tabcmd login -s https://tableau.company.com -u {{ var.value.tableau_user }} -p {{ var.value.tableau_password }}
            
            # Refresh the IMDB Dashboard
            tabcmd refreshextracts --datasource "IMDB_Analytics"
            
            # Alternatively, using REST API with Python
            python -c "
            import tableauserverclient as TSC
            
            tableau_auth = TSC.TableauAuth('{{ var.value.tableau_user }}', '{{ var.value.tableau_password }}')
            server = TSC.Server('https://tableau.company.com')
            server.auth.sign_in(tableau_auth)
            
            # Find and refresh datasource
            all_datasources, pagination_item = server.datasources.get()
            for datasource in all_datasources:
                if datasource.name == 'IMDB_Analytics':
                    server.datasources.refresh(datasource)
            
            server.auth.sign_out()
            "
        """,
        dag=dag
    )
    
    # 7. SEND SUCCESS NOTIFICATION
    def send_completion_report(**context):
        """Generate and send pipeline completion report"""
        sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Get pipeline metrics
        with sf_hook.get_conn() as conn:
            cursor = conn.cursor()
            
            # Get row counts
            cursor.execute("""
                SELECT 
                    'MOVIES' as ENTITY,
                    COUNT(*) as ROW_COUNT
                FROM ANALYTICS.DIM_MOVIES
                UNION ALL
                SELECT 
                    'RATINGS',
                    COUNT(*)
                FROM ANALYTICS.FCT_MOVIE_RATINGS
                UNION ALL
                SELECT 
                    'GENRES',
                    COUNT(*)
                FROM ANALYTICS.DIM_GENRES
            """)
            
            metrics = cursor.fetchall()
            
            # Get top movie
            cursor.execute("""
                SELECT TITLE, POPULARITY_SCORE
                FROM TABLEAU_SERVING.MV_TOP_MOVIES
                LIMIT 1
            """)
            
            top_movie = cursor.fetchone()
        
        # Format email
        email_content = f"""
        <h2>IMDB Pipeline Completed Successfully</h2>
        
        <h3>Pipeline Metrics</h3>
        <table border="1">
            <tr><th>Entity</th><th>Row Count</th></tr>
            {''.join([f'<tr><td>{m[0]}</td><td>{m[1]:,}</td></tr>' for m in metrics])}
        </table>
        
        <h3>Top Movie</h3>
        <p>{top_movie[0]} (Popularity: {top_movie[1]:.2f})</p>
        
        <h3>Next Steps</h3>
        <ul>
            <li>Tableau dashboards have been refreshed</li>
            <li>Data is available in TABLEAU_SERVING schema</li>
            <li>Quality checks passed successfully</li>
        </ul>
        
        <p>View Dashboard: <a href="https://tableau.company.com/#/site/analytics/workbooks/imdb">IMDB Analytics Dashboard</a></p>
        """
        
        # Send email (using Airflow email operator)
        from airflow.operators.email import EmailOperator
        
        email = EmailOperator(
            task_id='send_success_email',
            to=['ray@company.com', 'data-team@company.com'],
            subject=f'IMDB Pipeline Success - {context["ds"]}',
            html_content=email_content
        )
        
        return email.execute(context)
    
    send_report = PythonOperator(
        task_id='send_completion_report',
        python_callable=send_completion_report,
        trigger_rule='all_success',
        dag=dag
    )
    
    # DEFINE DEPENDENCIES
    setup_snowflake >> load_to_snowflake >> dbt_transformations
    dbt_transformations >> optimize_snowflake >> quality_checks
    quality_checks >> refresh_tableau >> send_report


# 9.3 DBT MODEL EXAMPLES FOR SNOWFLAKE
# -------------------------------------
"""
Example dbt models for the pipeline above
Save these in your dbt project folder
"""

# models/staging/stg_movies.sql
stg_movies_sql = """
{{ config(
    materialized='view',
    schema='staging'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw_imdb', 'title_basics') }}
),

cleaned AS (
    SELECT
        TCONST as MOVIE_ID,
        PRIMARY_TITLE as TITLE,
        ORIGINAL_TITLE,
        TRY_CAST(START_YEAR AS INTEGER) as RELEASE_YEAR,
        TRY_CAST(RUNTIME_MINUTES AS INTEGER) as RUNTIME_MINUTES,
        SPLIT(GENRES, ',') as GENRES_ARRAY,
        IS_ADULT,
        LOADED_AT
    FROM source
    WHERE TITLE_TYPE = 'movie'
        AND START_YEAR IS NOT NULL
        AND START_YEAR >= 2010
)

SELECT * FROM cleaned
"""

# models/marts/fct_movie_ratings.sql
fct_movie_ratings_sql = """
{{ config(
    materialized='incremental',
    unique_key='RATING_KEY',
    on_schema_change='fail',
    cluster_by=['RELEASE_YEAR', 'GENRE_KEY']
) }}

WITH movies AS (
    SELECT * FROM {{ ref('dim_movies') }}
),

ratings AS (
    SELECT * FROM {{ ref('stg_ratings') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_dates') }}
),

final AS (
    SELECT
        {{ dbt_utils.surrogate_key(['m.MOVIE_KEY', 'CURRENT_DATE']) }} as RATING_KEY,
        m.MOVIE_KEY,
        m.GENRE_KEY,
        d.DATE_KEY,
        r.AVERAGE_RATING,
        r.NUM_VOTES,
        -- Calculate popularity score
        r.AVERAGE_RATING * LN(r.NUM_VOTES + 1) as POPULARITY_SCORE,
        CURRENT_TIMESTAMP() as DW_INSERT_DATE
    FROM movies m
    INNER JOIN ratings r ON m.MOVIE_ID = r.MOVIE_ID
    INNER JOIN dates d ON m.RELEASE_YEAR = d.YEAR
    
    {% if is_incremental() %}
        WHERE DW_INSERT_DATE > (SELECT MAX(DW_INSERT_DATE) FROM {{ this }})
    {% endif %}
)

SELECT * FROM final
"""

# models/tableau/tableau_movie_dashboard.sql
tableau_dashboard_sql = """
{{ config(
    materialized='table',
    schema='tableau_serving',
    post_hook="GRANT SELECT ON {{ this }} TO ROLE TABLEAU_ROLE"
) }}

WITH movie_stats AS (
    SELECT
        m.TITLE,
        m.RELEASE_YEAR,
        m.RUNTIME_MINUTES,
        f.AVERAGE_RATING,
        f.NUM_VOTES,
        f.POPULARITY_SCORE,
        ARRAY_TO_STRING(m.GENRES_ARRAY, ', ') as GENRES,
        d.DECADE,
        -- Calculated fields for Tableau
        CASE 
            WHEN f.AVERAGE_RATING >= 8 THEN 'Excellent'
            WHEN f.AVERAGE_RATING >= 7 THEN 'Good'
            WHEN f.AVERAGE_RATING >= 6 THEN 'Average'
            ELSE 'Below Average'
        END as RATING_CATEGORY,
        NTILE(100) OVER (ORDER BY f.POPULARITY_SCORE) as POPULARITY_PERCENTILE
    FROM {{ ref('fct_movie_ratings') }} f
    JOIN {{ ref('dim_movies') }} m ON f.MOVIE_KEY = m.MOVIE_KEY
    JOIN {{ ref('dim_dates') }} d ON f.DATE_KEY = d.DATE_KEY
)

SELECT * FROM movie_stats
"""

# 9.4 ASTRONOMER SPECIFIC CONFIGURATION
# --------------------------------------
"""
Astronomer deployment configuration (astronomer.yml)
"""

astronomer_config = """
deployment:
  environment_variables:
    - AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
    - AIRFLOW__SCHEDULER__MAX_THREADS=4
    - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
  
  # Resource allocation
  scheduler:
    resources:
      requests:
        cpu: 500m
        memory: 1Gi
      limits:
        cpu: 1000m
        memory: 2Gi
  
  webserver:
    resources:
      requests:
        cpu: 500m
        memory: 1Gi
  
  workers:
    replicas: 2
    resources:
      requests:
        cpu: 1000m
        memory: 2Gi
      limits:
        cpu: 2000m
        memory: 4Gi

# requirements.txt for Astronomer
requirements:
  - apache-airflow-providers-snowflake>=3.0.0
  - apache-airflow-providers-dbt-cloud>=3.0.0
  - astronomer-cosmos>=1.0.0
  - pandas>=1.5.0
  - snowflake-connector-python>=3.0.0
  - dbt-snowflake>=1.5.0
  - tableauserverclient>=0.25
"""

# That's the complete modern data stack with Snowflake + dbt + Astronomer + Tableau!
# Production-ready patterns you can deploy immediately
# ¡Vamos a construir el futuro del data engineering! 🚀❄️📊