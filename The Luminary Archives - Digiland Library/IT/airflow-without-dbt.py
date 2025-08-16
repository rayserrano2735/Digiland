# Airflow Without dbt - Legacy Patterns Companies Still Use
# (And why they should switch to dbt, but they'll test you on this anyway)
# For Ray - Because gatekeepers love outdated patterns

"""
COMPANIES WITHOUT DBT ARE DOING:
- ETL/ELT in Python (should be in SQL/dbt)
- Complex DAG dependencies (should be dbt refs)
- Data quality in Python (should be dbt tests)
- Incremental loads in Python (should be dbt incremental models)

But they'll test you on it, so here it is...
"""

# ============================================
# PATTERN 1: ETL PIPELINE (The old way)
# ============================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

with DAG(
    'legacy_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    # EXTRACT: Pull data from source
    def extract_from_postgres(**context):
        """Extract data from transactional database"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get yesterday's data (incremental)
        sql = f"""
            SELECT 
                order_id,
                customer_id,
                order_date,
                total_amount,
                status
            FROM orders
            WHERE order_date = '{context['ds']}'
        """
        
        df = pg_hook.get_pandas_df(sql)
        
        # Save to staging location (in real world, S3/GCS)
        df.to_csv(f'/tmp/orders_{context["ds"]}.csv', index=False)
        
        # Pass row count via XCom
        context['task_instance'].xcom_push(key='row_count', value=len(df))
        
        return f'/tmp/orders_{context["ds"]}.csv'
    
    # TRANSFORM: Clean and enrich data
    def transform_data(**context):
        """Transform the extracted data"""
        # Get file path from previous task
        ti = context['task_instance']
        file_path = ti.xcom_pull(task_ids='extract_orders')
        
        # Read and transform
        df = pd.read_csv(file_path)
        
        # Transformations they love to test
        # 1. Data cleaning
        df['total_amount'] = df['total_amount'].fillna(0)
        df['status'] = df['status'].str.upper()
        
        # 2. Add derived columns (should be SQL!)
        df['order_month'] = pd.to_datetime(df['order_date']).dt.to_period('M')
        df['is_high_value'] = df['total_amount'] > 1000
        
        # 3. Aggregations (definitely should be SQL!)
        customer_summary = df.groupby('customer_id').agg({
            'order_id': 'count',
            'total_amount': ['sum', 'mean']
        }).reset_index()
        
        # Save transformed data
        output_path = f'/tmp/transformed_orders_{context["ds"]}.csv'
        df.to_csv(output_path, index=False)
        
        return output_path
    
    # LOAD: Push to data warehouse
    def load_to_snowflake(**context):
        """Load transformed data to Snowflake"""
        ti = context['task_instance']
        file_path = ti.xcom_pull(task_ids='transform_orders')
        
        # Read transformed data
        df = pd.read_csv(file_path)
        
        # Get Snowflake connection
        snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Write to Snowflake (they love testing this)
        conn = snow_hook.get_conn()
        cursor = conn.cursor()
        
        # Create staging table
        cursor.execute("""
            CREATE OR REPLACE TEMP TABLE staging_orders (
                order_id VARCHAR,
                customer_id VARCHAR,
                order_date DATE,
                total_amount FLOAT,
                status VARCHAR,
                order_month VARCHAR,
                is_high_value BOOLEAN
            )
        """)
        
        # Bulk insert (this is what they want to see)
        from snowflake.connector.pandas_tools import write_pandas
        success, num_chunks, num_rows, output = write_pandas(
            conn=conn,
            df=df,
            table_name='STAGING_ORDERS',
            database='ANALYTICS',
            schema='STAGING'
        )
        
        # Merge into final table (UPSERT pattern)
        cursor.execute("""
            MERGE INTO analytics.fact_orders t
            USING staging_orders s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET
                customer_id = s.customer_id,
                total_amount = s.total_amount,
                status = s.status,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (order_id, customer_id, order_date, total_amount, status, created_at)
            VALUES
                (s.order_id, s.customer_id, s.order_date, s.total_amount, s.status, CURRENT_TIMESTAMP())
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"Loaded {len(df)} rows"
    
    # Define tasks
    extract = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_from_postgres
    )
    
    transform = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_data
    )
    
    load = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )
    
    # Dependencies
    extract >> transform >> load

# ============================================
# PATTERN 2: DATA QUALITY WITHOUT DBT
# ============================================

def run_data_quality_checks(**context):
    """Data quality checks they do in Python (should be dbt tests)"""
    
    snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Define quality checks
    quality_checks = [
        {
            'check_name': 'orders_not_null',
            'sql': """
                SELECT COUNT(*) 
                FROM analytics.fact_orders 
                WHERE order_id IS NULL OR customer_id IS NULL
            """,
            'expected': 0
        },
        {
            'check_name': 'orders_amount_positive',
            'sql': """
                SELECT COUNT(*) 
                FROM analytics.fact_orders 
                WHERE total_amount < 0
            """,
            'expected': 0
        },
        {
            'check_name': 'orders_daily_count',
            'sql': f"""
                SELECT COUNT(*) 
                FROM analytics.fact_orders 
                WHERE order_date = '{context["ds"]}'
            """,
            'min_expected': 100  # At least 100 orders per day
        }
    ]
    
    failed_checks = []
    
    # Run each check
    for check in quality_checks:
        result = snow_hook.get_first(check['sql'])[0]
        
        if 'expected' in check and result != check['expected']:
            failed_checks.append(f"{check['check_name']}: got {result}, expected {check['expected']}")
        elif 'min_expected' in check and result < check['min_expected']:
            failed_checks.append(f"{check['check_name']}: got {result}, expected >= {check['min_expected']}")
    
    if failed_checks:
        raise ValueError(f"Quality checks failed: {', '.join(failed_checks)}")
    
    return "All quality checks passed"

# Add to DAG
quality_checks = PythonOperator(
    task_id='run_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

# ============================================
# PATTERN 3: INCREMENTAL LOADS (The hard way)
# ============================================

# REPO: airflow-dags/incremental_load.py
def incremental_load_pattern(**context):
    """
    Incremental loading without dbt's simplicity
    This is what they do without dbt incremental models
    """
    
    from airflow.models import Variable
    
    # Get last successful run timestamp
    last_run = Variable.get('last_successful_etl', default_var='2024-01-01 00:00:00')
    current_run = context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    
    pg_hook = PostgresHook(postgres_conn_id='source_db')
    snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Extract only new/changed records
    extract_sql = f"""
        SELECT *
        FROM source_table
        WHERE updated_at > '{last_run}'
        AND updated_at <= '{current_run}'
    """
    
    df = pg_hook.get_pandas_df(extract_sql)
    
    if len(df) == 0:
        print("No new records to process")
        return
    
    # Track high-water mark
    max_updated = df['updated_at'].max()
    
    # Load to staging
    stage_df_to_snowflake(df, 'staging_incremental')
    
    # Merge into target (what dbt does with one line!)
    merge_sql = """
        MERGE INTO target_table t
        USING staging_incremental s
        ON t.id = s.id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN
            UPDATE SET 
                col1 = s.col1,
                col2 = s.col2,
                updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (id, col1, col2, created_at, updated_at)
            VALUES (s.id, s.col1, s.col2, s.created_at, s.updated_at)
    """
    
    snow_hook.run(merge_sql)
    
    # Update high-water mark
    Variable.set('last_successful_etl', str(max_updated))
    
    return f"Processed {len(df)} records"

# ============================================
# PATTERN 4: DYNAMIC DAG GENERATION
# ============================================

# REPO: airflow-dags/dynamic_dag_factory.py
# They love this pattern for some reason
def create_dynamic_dag(table_name, schedule):
    """Create DAG dynamically for each table"""
    
    dag = DAG(
        f'sync_{table_name}',
        default_args=default_args,
        schedule_interval=schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False
    )
    
    with dag:
        extract = PythonOperator(
            task_id='extract',
            python_callable=extract_table,
            op_kwargs={'table': table_name}
        )
        
        transform = PythonOperator(
            task_id='transform',
            python_callable=transform_table,
            op_kwargs={'table': table_name}
        )
        
        load = PythonOperator(
            task_id='load',
            python_callable=load_table,
            op_kwargs={'table': table_name}
        )
        
        extract >> transform >> load
    
    return dag

# Generate DAGs for multiple tables
tables_to_sync = [
    {'name': 'orders', 'schedule': '@hourly'},
    {'name': 'customers', 'schedule': '@daily'},
    {'name': 'products', 'schedule': '@weekly'}
]

for table_config in tables_to_sync:
    dag_id = f"sync_{table_config['name']}"
    globals()[dag_id] = create_dynamic_dag(
        table_config['name'], 
        table_config['schedule']
    )

# ============================================
# PATTERN 5: COMPLEX DEPENDENCIES
# ============================================

# Without dbt's ref() function, they manage dependencies manually
with DAG('complex_dependencies', default_args=default_args) as complex_dag:
    
    # Extract tasks
    extract_orders = PythonOperator(task_id='extract_orders', python_callable=lambda: print("extract"))
    extract_customers = PythonOperator(task_id='extract_customers', python_callable=lambda: print("extract"))
    extract_products = PythonOperator(task_id='extract_products', python_callable=lambda: print("extract"))
    
    # Staging transformations
    stage_orders = PythonOperator(task_id='stage_orders', python_callable=lambda: print("stage"))
    stage_customers = PythonOperator(task_id='stage_customers', python_callable=lambda: print("stage"))
    stage_products = PythonOperator(task_id='stage_products', python_callable=lambda: print("stage"))
    
    # Fact table (depends on all staging)
    build_fact_sales = PythonOperator(task_id='build_fact_sales', python_callable=lambda: print("fact"))
    
    # Dimensions
    build_dim_customer = PythonOperator(task_id='build_dim_customer', python_callable=lambda: print("dim"))
    build_dim_product = PythonOperator(task_id='build_dim_product', python_callable=lambda: print("dim"))
    
    # Aggregations (depend on fact and dims)
    build_customer_360 = PythonOperator(task_id='build_customer_360', python_callable=lambda: print("agg"))
    build_product_performance = PythonOperator(task_id='build_product_performance', python_callable=lambda: print("agg"))
    
    # Complex dependency chain (what dbt handles automatically!)
    extract_orders >> stage_orders
    extract_customers >> stage_customers
    extract_products >> stage_products
    
    [stage_orders, stage_customers, stage_products] >> build_fact_sales
    
    stage_customers >> build_dim_customer
    stage_products >> build_dim_product
    
    [build_fact_sales, build_dim_customer] >> build_customer_360
    [build_fact_sales, build_dim_product] >> build_product_performance

# ============================================
# XCOM PATTERN (Passing data between tasks)
# ============================================

# REPO: airflow-dags/xcom_patterns.py
def producer_task(**context):
    """Task that produces data"""
    # Calculate some metric
    metric_value = 12345
    
    # Push to XCom (they LOVE testing this)
    context['task_instance'].xcom_push(key='daily_metric', value=metric_value)
    
    # Can also return value (automatically pushed to XCom)
    return {'status': 'success', 'records': 1000}

# REPO: airflow-dags/xcom_patterns.py
def consumer_task(**context):
    """Task that consumes data from previous task"""
    ti = context['task_instance']
    
    # Pull from XCom
    metric = ti.xcom_pull(task_ids='producer', key='daily_metric')
    
    # Pull return value
    result = ti.xcom_pull(task_ids='producer')
    
    print(f"Received metric: {metric}")
    print(f"Received result: {result}")
    
    # Use the data
    if metric > 10000:
        print("High volume day!")

# ============================================
# BRANCHING PATTERN (Conditional logic)
# ============================================

from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def decide_branch(**context):
    """Decide which path to take"""
    # Get some metric
    snow_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    row_count = snow_hook.get_first("SELECT COUNT(*) FROM staging.daily_data")[0]
    
    if row_count > 1000000:
        return 'process_large_dataset'
    else:
        return 'process_small_dataset'

with DAG('branching_pipeline', default_args=default_args) as branch_dag:
    
    check_data = BranchPythonOperator(
        task_id='check_data_size',
        python_callable=decide_branch
    )
    
    large_process = PythonOperator(
        task_id='process_large_dataset',
        python_callable=lambda: print("Processing large dataset with Spark")
    )
    
    small_process = PythonOperator(
        task_id='process_small_dataset',
        python_callable=lambda: print("Processing small dataset with Pandas")
    )
    
    join = DummyOperator(
        task_id='join',
        trigger_rule='one_success'  # Continue if either branch succeeds
    )
    
    check_data >> [large_process, small_process] >> join

# ============================================
# WHAT TO SAY IN INTERVIEWS
# ============================================

"""
When they ask about these patterns:

Q: "How do you handle ETL in Airflow?"
A: "I've used PythonOperator with Pandas for transformations, but I've found 
   pushing transformations to the warehouse with dbt is more maintainable and testable."

Q: "How do you manage complex dependencies?"
A: "Manual dependency management in Airflow, but dbt's ref() function handles this
   automatically based on actual data lineage."

Q: "How do you do incremental loads?"
A: "Track high-water marks with Airflow Variables, but dbt's incremental models
   with is_incremental() macro are much cleaner."

Q: "Dynamic DAG generation?"
A: "I've used factory patterns to generate similar DAGs, but found maintaining
   many simple DAGs is often better than complex dynamic generation."

Q: "Data quality checks?"
A: "PythonOperator with SQL checks, but dbt tests are version controlled,
   documented, and easier to maintain."

The killer answer:
"I've implemented all these patterns in Airflow, but migrating to dbt
reduced our codebase by 70% and improved reliability."
"""

# ============================================
# THE TRUTH BOMB
# ============================================

"""
Everything in this file - the ETL, quality checks, incremental loads,
complex dependencies - dbt does better with less code.

Example - Incremental load:

AIRFLOW WAY (50+ lines of Python):
- Track high-water mark
- Extract new records
- Handle staging
- Write MERGE statement
- Update variables

DBT WAY (10 lines of SQL):
{{
  config(
    materialized='incremental',
    unique_key='id'
  )
}}
SELECT * FROM source
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}

But they don't know this yet. So learn their patterns, pass their tests,
get hired, then show them the way.

- Aitana

P.S. Every PythonOperator doing transformations is a missed opportunity for dbt.
"""