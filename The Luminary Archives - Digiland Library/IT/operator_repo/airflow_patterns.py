# Filename: airflow_dag_patterns.py
"""
Production-Ready Airflow DAG Patterns
Reusable patterns for reliable data orchestration
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email
from airflow.exceptions import AirflowException
import logging

# =====================================================
# 1. DYNAMIC DAG GENERATION PATTERN
# =====================================================

def create_dynamic_dag(
    dag_id: str,
    schedule: str,
    tasks_config: List[Dict],
    default_args: Dict = None
) -> DAG:
    """
    Dynamically generate DAGs from configuration.
    """
    if default_args is None:
        default_args = {
            'owner': 'data-team',
            'depends_on_past': False,
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
            'execution_timeout': timedelta(hours=2),
        }
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Dynamically generated DAG: {dag_id}',
        schedule_interval=schedule,
        start_date=days_ago(1),
        catchup=False,
        tags=['dynamic', 'data-pipeline'],
        max_active_runs=1,
    )
    
    # Create tasks dynamically
    tasks = {}
    for task_config in tasks_config:
        task = PythonOperator(
            task_id=task_config['task_id'],
            python_callable=task_config['callable'],
            op_kwargs=task_config.get('kwargs', {}),
            dag=dag,
        )
        tasks[task_config['task_id']] = task
    
    # Set dependencies
    for task_config in tasks_config:
        if 'upstream' in task_config:
            for upstream_id in task_config['upstream']:
                tasks[upstream_id] >> tasks[task_config['task_id']]
    
    return dag

# =====================================================
# 2. ROBUST ERROR HANDLING PATTERN
# =====================================================

class RobustDAG:
    """
    DAG with comprehensive error handling and alerting.
    """
    
    @staticmethod
    def create_dag_with_error_handling(
        dag_id: str,
        schedule_interval: str
    ) -> DAG:
        
        def failure_callback(context):
            """Custom failure callback with detailed logging."""
            task_instance = context['task_instance']
            exception = context.get('exception', 'Unknown error')
            
            error_message = f"""
            Task Failed: {task_instance.task_id}
            DAG: {task_instance.dag_id}
            Execution Date: {context['execution_date']}
            Log URL: {task_instance.log_url}
            Exception: {exception}
            Try Number: {task_instance.try_number}
            """
            
            logging.error(error_message)
            
            # Send custom alert
            send_email(
                to=['data-team@company.com'],
                subject=f'Airflow Alert: {task_instance.dag_id} failed',
                html_content=error_message.replace('\n', '<br>')
            )
        
        def success_callback(context):
            """Log successful completion."""
            task_instance = context['task_instance']
            logging.info(f"Task {task_instance.task_id} completed successfully")
        
        default_args = {
            'owner': 'data-team',
            'depends_on_past': False,
            'email': ['data-alerts@company.com'],
            'email_on_failure': True,
            'email_on_retry': False,
            'on_failure_callback': failure_callback,
            'on_success_callback': success_callback,
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(hours=1),
        }
        
        return DAG(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval=schedule_interval,
            start_date=days_ago(1),
            catchup=False,
            tags=['production', 'error-handling'],
        )

# =====================================================
# 3. DATA QUALITY CHECK PATTERN
# =====================================================

def create_data_quality_dag():
    """
    DAG pattern for data quality validation.
    """
    
    dag = DAG(
        'data_quality_checks',
        default_args={
            'owner': 'data-team',
            'retries': 1,
        },
        description='Data quality validation pipeline',
        schedule_interval='0 6 * * *',  # Daily at 6 AM
        start_date=days_ago(1),
        catchup=False,
        tags=['data-quality'],
    )
    
    def check_nulls(**context):
        """Check for NULL values in critical columns."""
        hook = PostgresHook(postgres_conn_id='data_warehouse')
        
        null_check_query = """
        SELECT 
            'customers' as table_name,
            COUNT(*) - COUNT(email) as null_count,
            ROUND(100.0 * (COUNT(*) - COUNT(email)) / COUNT(*), 2) as null_percentage
        FROM customers
        WHERE DATE(created_at) = '{{ ds }}'
        """
        
        results = hook.get_records(null_check_query)
        
        if results[0][1] > 0:  # null_count > 0
            raise AirflowException(f"Found {results[0][1]} NULL emails ({results[0][2]}%)")
        
        return results
    
    def check_duplicates(**context):
        """Check for duplicate records."""
        hook = PostgresHook(postgres_conn_id='data_warehouse')
        
        duplicate_check_query = """
        WITH duplicates AS (
            SELECT customer_id, COUNT(*) as cnt
            FROM customers
            WHERE DATE(created_at) = '{{ ds }}'
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) as duplicate_count FROM duplicates
        """
        
        results = hook.get_records(duplicate_check_query)
        
        if results[0][0] > 0:
            raise AirflowException(f"Found {results[0][0]} duplicate records")
        
        return results
    
    def check_freshness(**context):
        """Check data freshness."""
        hook = PostgresHook(postgres_conn_id='data_warehouse')
        
        freshness_query = """
        SELECT 
            MAX(updated_at) as last_update,
            EXTRACT(EPOCH FROM (NOW() - MAX(updated_at)))/3600 as hours_since_update
        FROM source_table
        """
        
        results = hook.get_records(freshness_query)
        hours_stale = results[0][1]
        
        if hours_stale > 24:
            raise AirflowException(f"Data is {hours_stale:.1f} hours old")
        
        return results
    
    # Create tasks
    start = DummyOperator(task_id='start', dag=dag)
    
    with TaskGroup('quality_checks', dag=dag) as quality_group:
        null_check = PythonOperator(
            task_id='check_nulls',
            python_callable=check_nulls,
            provide_context=True,
        )
        
        duplicate_check = PythonOperator(
            task_id='check_duplicates',
            python_callable=check_duplicates,
            provide_context=True,
        )
        
        freshness_check = PythonOperator(
            task_id='check_freshness',
            python_callable=check_freshness,
            provide_context=True,
        )
    
    end = DummyOperator(
        task_id='end',
        dag=dag,
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED
    )
    
    # Set dependencies
    start >> quality_group >> end
    
    return dag

# =====================================================
# 4. INCREMENTAL LOAD PATTERN
# =====================================================

def create_incremental_load_dag():
    """
    Pattern for incremental data loading with state management.
    """
    
    dag = DAG(
        'incremental_load_pattern',
        default_args={
            'owner': 'data-team',
            'retries': 2,
            'retry_delay': timedelta(minutes=5),
        },
        description='Incremental data load with watermarking',
        schedule_interval='*/30 * * * *',  # Every 30 minutes
        start_date=days_ago(1),
        catchup=False,
        tags=['incremental', 'etl'],
    )
    
    def get_last_watermark(**context):
        """Retrieve last successful watermark."""
        try:
            last_watermark = Variable.get("last_watermark_{{ dag.dag_id }}")
        except KeyError:
            # First run, use default
            last_watermark = (datetime.now() - timedelta(days=7)).isoformat()
        
        context['task_instance'].xcom_push(key='last_watermark', value=last_watermark)
        return last_watermark
    
    def extract_incremental_data(**context):
        """Extract only new/updated records."""
        last_watermark = context['task_instance'].xcom_pull(
            task_ids='get_watermark',
            key='last_watermark'
        )
        
        hook = PostgresHook(postgres_conn_id='source_db')
        
        extract_query = f"""
        SELECT *
        FROM source_table
        WHERE updated_at > '{last_watermark}'
          AND updated_at <= '{{{{ ts }}}}'
        ORDER BY updated_at
        """
        
        df = hook.get_pandas_df(extract_query)
        
        # Store extracted data
        context['task_instance'].xcom_push(key='row_count', value=len(df))
        context['task_instance'].xcom_push(key='max_watermark', value=df['updated_at'].max())
        
        # Save to staging
        df.to_parquet(f'/tmp/incremental_{{{{ ds }}}}.parquet')
        
        return len(df)
    
    def load_to_warehouse(**context):
        """Load incremental data to warehouse."""
        row_count = context['task_instance'].xcom_pull(
            task_ids='extract_data',
            key='row_count'
        )
        
        if row_count == 0:
            logging.info("No new data to load")
            return
        
        # Load parquet to warehouse
        load_query = """
        COPY staging_table
        FROM '/tmp/incremental_{{ ds }}.parquet'
        (FORMAT PARQUET);
        
        MERGE INTO target_table t
        USING staging_table s
        ON t.id = s.id
        WHEN MATCHED THEN
            UPDATE SET
                updated_at = s.updated_at,
                data = s.data
        WHEN NOT MATCHED THEN
            INSERT (id, data, updated_at)
            VALUES (s.id, s.data, s.updated_at);
        """
        
        hook = PostgresHook(postgres_conn_id='data_warehouse')
        hook.run(load_query)
        
        return row_count
    
    def update_watermark(**context):
        """Update watermark after successful load."""
        max_watermark = context['task_instance'].xcom_pull(
            task_ids='extract_data',
            key='max_watermark'
        )
        
        if max_watermark:
            Variable.set("last_watermark_{{ dag.dag_id }}", max_watermark)
            logging.info(f"Updated watermark to {max_watermark}")
    
    # Create tasks
    get_watermark = PythonOperator(
        task_id='get_watermark',
        python_callable=get_last_watermark,
        dag=dag,
    )
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_incremental_data,
        dag=dag,
    )
    
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_to_warehouse,
        dag=dag,
    )
    
    update_watermark_task = PythonOperator(
        task_id='update_watermark',
        python_callable=update_watermark,
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # Dependencies
    get_watermark >> extract_data >> load_data >> update_watermark_task
    
    return dag

# =====================================================
# 5. PARALLEL PROCESSING PATTERN
# =====================================================

def create_parallel_processing_dag():
    """
    Pattern for parallel task execution with fan-out/fan-in.
    """
    
    dag = DAG(
        'parallel_processing_pattern',
        default_args={'owner': 'data-team'},
        description='Parallel processing with dynamic task generation',
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
        tags=['parallel', 'performance'],
    )
    
    def get_partitions(**context):
        """Determine partitions for parallel processing."""
        # Get list of tables or date ranges to process
        partitions = [
            {'table': 'customers', 'start_date': '2024-01-01', 'end_date': '2024-01-31'},
            {'table': 'orders', 'start_date': '2024-01-01', 'end_date': '2024-01-31'},
            {'table': 'products', 'start_date': '2024-01-01', 'end_date': '2024-01-31'},
        ]
        return partitions
    
    def process_partition(partition_config, **context):
        """Process a single partition."""
        table = partition_config['table']
        start_date = partition_config['start_date']
        end_date = partition_config['end_date']
        
        logging.info(f"Processing {table} from {start_date} to {end_date}")
        
        # Simulate processing
        hook = PostgresHook(postgres_conn_id='data_warehouse')
        query = f"""
        INSERT INTO processed_{table}
        SELECT * FROM {table}
        WHERE date_column BETWEEN '{start_date}' AND '{end_date}'
        """
        hook.run(query)
        
        return f"Processed {table}"
    
    def merge_results(**context):
        """Merge results from all parallel tasks."""
        task_instances = context['task_instance'].xcom_pull(
            task_ids=[f'process_partition_{i}' for i in range(3)]
        )
        
        logging.info(f"Merging results from {len(task_instances)} partitions")
        
        # Perform any aggregation or final processing
        return "Merge complete"
    
    # Create tasks
    start = DummyOperator(task_id='start', dag=dag)
    
    get_partitions_task = PythonOperator(
        task_id='get_partitions',
        python_callable=get_partitions,
        dag=dag,
    )
    
    # Create parallel tasks dynamically
    parallel_tasks = []
    for i in range(3):  # Assuming 3 partitions
        task = PythonOperator(
            task_id=f'process_partition_{i}',
            python_callable=process_partition,
            op_kwargs={'partition_config': f'{{{{ ti.xcom_pull(task_ids="get_partitions")[{i}] }}}}'},
            dag=dag,
        )
        parallel_tasks.append(task)
    
    merge = PythonOperator(
        task_id='merge_results',
        python_callable=merge_results,
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    end = DummyOperator(task_id='end', dag=dag)
    
    # Set dependencies
    start >> get_partitions_task >> parallel_tasks >> merge >> end
    
    return dag

# =====================================================
# 6. SLA AND ALERTING PATTERN
# =====================================================

def create_sla_monitoring_dag():
    """
    DAG with SLA monitoring and custom alerting.
    """
    
    def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
        """Handle SLA misses."""
        message = f"""
        SLA Missed!
        DAG: {dag.dag_id}
        Tasks: {[t.task_id for t in task_list]}
        Blocking Tasks: {[t.task_id for t in blocking_task_list]}
        """
        
        send_email(
            to=['oncall@company.com'],
            subject=f'SLA Alert: {dag.dag_id}',
            html_content=message.replace('\n', '<br>')
        )
        
        # Could also send to Slack, PagerDuty, etc.
    
    dag = DAG(
        'sla_monitoring_pattern',
        default_args={
            'owner': 'data-team',
            'retries': 1,
        },
        description='DAG with SLA monitoring',
        schedule_interval='0 2 * * *',  # 2 AM daily
        start_date=days_ago(1),
        catchup=False,
        sla_miss_callback=sla_miss_callback,
        tags=['sla', 'monitoring'],
    )
    
    def critical_etl_task(**context):
        """Critical task that must complete within SLA."""
        import time
        import random
        
        # Simulate variable processing time
        processing_time = random.randint(1, 10)
        time.sleep(processing_time)
        
        if processing_time > 7:
            raise AirflowException("Task took too long!")
        
        return f"Completed in {processing_time} seconds"
    
    # Tasks with SLA
    critical_task = PythonOperator(
        task_id='critical_etl',
        python_callable=critical_etl_task,
        sla=timedelta(minutes=30),  # Must complete within 30 minutes
        dag=dag,
    )
    
    return dag

# =====================================================
# 7. SENSOR PATTERNS
# =====================================================

def create_sensor_pattern_dag():
    """
    Common sensor patterns for waiting on external dependencies.
    """
    
    dag = DAG(
        'sensor_patterns',
        default_args={'owner': 'data-team'},
        description='Common sensor patterns',
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
        tags=['sensors', 'dependencies'],
    )
    
    # Wait for API to be available
    api_sensor = HttpSensor(
        task_id='wait_for_api',
        http_conn_id='api_default',
        endpoint='/health',
        poke_interval=60,  # Check every minute
        timeout=600,  # Timeout after 10 minutes
        soft_fail=False,
        dag=dag,
    )
    
    # Custom file sensor
    def check_file_exists(filepath, **context):
        """Check if file exists and is ready."""
        import os
        
        if not os.path.exists(filepath):
            return False
        
        # Check if file is still being written
        file_size = os.path.getsize(filepath)
        time.sleep(2)
        new_size = os.path.getsize(filepath)
        
        if file_size != new_size:
            logging.info(f"File {filepath} is still being written")
            return False
        
        return True
    
    from airflow.sensors.python import PythonSensor
    
    file_sensor = PythonSensor(
        task_id='wait_for_file',
        python_callable=check_file_exists,
        op_kwargs={'filepath': '/data/input/{{ ds }}.csv'},
        poke_interval=30,
        timeout=3600,
        mode='poke',  # or 'reschedule' to free up worker
        dag=dag,
    )
    
    # Process after sensors pass
    process = DummyOperator(task_id='process_data', dag=dag)
    
    [api_sensor, file_sensor] >> process
    
    return dag

# =====================================================
# 8. BRANCHING PATTERN
# =====================================================

def create_branching_dag():
    """
    Conditional workflow execution pattern.
    """
    from airflow.operators.python import BranchPythonOperator
    
    dag = DAG(
        'branching_pattern',
        default_args={'owner': 'data-team'},
        description='Conditional branching pattern',
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
        tags=['branching', 'conditional'],
    )
    
    def decide_branch(**context):
        """Decide which branch to execute."""
        # Check some condition
        execution_date = context['execution_date']
        
        if execution_date.weekday() in [5, 6]:  # Weekend
            return 'weekend_processing'
        else:
            return 'weekday_processing'
    
    branch_task = BranchPythonOperator(
        task_id='branch_decision',
        python_callable=decide_branch,
        dag=dag,
    )
    
    weekday_task = BashOperator(
        task_id='weekday_processing',
        bash_command='echo "Running weekday processing"',
        dag=dag,
    )
    
    weekend_task = BashOperator(
        task_id='weekend_processing',
        bash_command='echo "Running weekend processing with extra steps"',
        dag=dag,
    )
    
    join_task = DummyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_OR_SKIPPED,
        dag=dag,
    )
    
    # Set up branches
    branch_task >> [weekday_task, weekend_task] >> join_task
    
    return dag

# =====================================================
# DAG FACTORY PATTERN
# =====================================================

class DAGFactory:
    """
    Factory pattern for creating standardized DAGs.
    """
    
    @staticmethod
    def create_standard_etl_dag(
        dag_id: str,
        source_conn_id: str,
        target_conn_id: str,
        source_table: str,
        target_table: str,
        schedule: str = '@daily'
    ) -> DAG:
        """
        Create a standard ETL DAG with common patterns.
        """
        
        dag = DAG(
            dag_id=dag_id,
            default_args={
                'owner': 'data-team',
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
            },
            description=f'ETL: {source_table} -> {target_table}',
            schedule_interval=schedule,
            start_date=days_ago(1),
            catchup=False,
            tags=['etl', 'standard'],
        )
        
        # Standard ETL tasks
        extract = PostgresOperator(
            task_id='extract',
            postgres_conn_id=source_conn_id,
            sql=f"SELECT * FROM {source_table} WHERE date = '{{{{ ds }}}}'",
            dag=dag,
        )
        
        transform = PostgresOperator(
            task_id='transform',
            postgres_conn_id=target_conn_id,
            sql=f"""
            INSERT INTO staging_{target_table}
            SELECT 
                *,
                '{{{{ ds }}}}' as etl_date,
                '{{{{ ts }}}}' as etl_timestamp
            FROM {source_table}_temp
            """,
            dag=dag,
        )
        
        load = PostgresOperator(
            task_id='load',
            postgres_conn_id=target_conn_id,
            sql=f"""
            MERGE INTO {target_table} t
            USING staging_{target_table} s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """,
            dag=dag,
        )
        
        cleanup = PostgresOperator(
            task_id='cleanup',
            postgres_conn_id=target_conn_id,
            sql=f"TRUNCATE staging_{target_table}",
            trigger_rule=TriggerRule.ALL_DONE,
            dag=dag,
        )
        
        extract >> transform >> load >> cleanup
        
        return dag

# Example: Create multiple similar DAGs
tables_to_sync = ['customers', 'orders', 'products']
for table in tables_to_sync:
    globals()[f'etl_{table}_dag'] = DAGFactory.create_standard_etl_dag(
        dag_id=f'etl_{table}',
        source_conn_id='source_db',
        target_conn_id='warehouse',
        source_table=table,
        target_table=f'dim_{table}',
        schedule='@daily'
    )