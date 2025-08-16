# Airflow for dbt Users - The 30-Minute Survival Guide
# Because you shouldn't need this, but they'll ask anyway
# Built for Ray - Intelligence²

"""
THE TRUTH ABOUT AIRFLOW:
- 90% of data teams use it to run: dbt run && dbt test
- It's overkill for most analytics workflows
- dbt Cloud + Fivetran makes it obsolete
- But it's still a gatekeeping question

THIS GUIDE COVERS:
1. Core Concepts (5 min) - What they expect you to know
2. Basic DAG Structure (5 min) - The only pattern you need
3. dbt Integration (10 min) - What you'll actually use
4. Common Operators (5 min) - The ones that matter
5. Interview Answers (5 min) - What to say when asked
"""

# ============================================
# PART 1: CORE CONCEPTS (All you need to know)
# ============================================

"""
DAG = Directed Acyclic Graph = Your pipeline
Task = One step in your pipeline (run dbt, send email, etc.)
Operator = Pre-built task type (BashOperator, PythonOperator, etc.)
Schedule = When it runs (@daily, @hourly, or cron)
Dependencies = Order of operations (task1 >> task2)

That's literally it. Everything else is details.
"""

# ============================================
# PART 2: THE ONLY DAG PATTERN YOU NEED
# ============================================

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Default args (they always ask about this)
default_args = {
    'owner': 'ray',
    'depends_on_past': False,  # Don't wait for yesterday's run
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['ray@company.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# The DAG itself
dag = DAG(
    'dbt_daily_pipeline',  # Name
    default_args=default_args,
    description='Daily dbt run',
    schedule_interval='@daily',  # or '0 2 * * *' for 2 AM
    catchup=False  # Don't backfill history
)

# ============================================
# PART 3: DBT INTEGRATION (What you'll actually use)
# ============================================

# REPO: airflow-dags/dbt_orchestration.py
# Option 1: Simple dbt commands (most common)
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt && dbt run --models production',
    dag=dag
)

dbt_test = BashOperator(
    task_id='dbt_test', 
    bash_command='cd /path/to/dbt && dbt test',
    dag=dag
)

dbt_snapshot = BashOperator(
    task_id='dbt_snapshot',
    bash_command='cd /path/to/dbt && dbt snapshot',
    dag=dag
)

# Option 2: dbt with specific models (targeted runs)
run_staging = BashOperator(
    task_id='run_staging',
    bash_command='dbt run --models staging',
    dag=dag
)

run_marts = BashOperator(
    task_id='run_marts',
    bash_command='dbt run --models marts',
    dag=dag
)

# Option 3: dbt Cloud API (modern approach)
trigger_dbt_cloud = BashOperator(
    task_id='trigger_dbt_cloud',
    bash_command="""
    curl -X POST https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/ \
    -H "Authorization: Token ${DBT_CLOUD_TOKEN}" \
    -H "Content-Type: application/json"
    """,
    dag=dag
)

# Setting dependencies (the >> operator)
dbt_snapshot >> dbt_run >> dbt_test

# Or more complex:
run_staging >> run_marts >> dbt_test

# ============================================
# PART 4: COMMON OPERATORS YOU'LL BE ASKED ABOUT
# ============================================

# 1. SnowflakeOperator - Run SQL directly
refresh_mv = SnowflakeOperator(
    task_id='refresh_materialized_view',
    snowflake_conn_id='snowflake_default',  # Connection in Airflow UI
    sql="""
        ALTER MATERIALIZED VIEW analytics.mv_daily_metrics REFRESH;
    """,
    dag=dag
)

# 2. PythonOperator - For custom logic
from airflow.operators.python import PythonOperator

def check_data_quality(**context):
    """Custom data quality check"""
    # Your Python code here
    if data_is_bad:
        raise ValueError("Data quality check failed!")
    return "Data quality passed"

quality_check = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

# 3. EmailOperator - Send notifications
send_success_email = EmailOperator(
    task_id='send_success',
    to=['stakeholders@company.com'],
    subject='Daily Pipeline Complete',
    html_content='<h3>Pipeline finished successfully</h3>',
    dag=dag
)

# 4. Sensor - Wait for something
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id='wait_for_source_file',
    filepath='/data/incoming/daily_data.csv',
    fs_conn_id='fs_default',
    poke_interval=300,  # Check every 5 minutes
    timeout=3600,  # Give up after 1 hour
    dag=dag
)

# Complete pipeline with dependencies
wait_for_file >> dbt_run >> quality_check >> dbt_test >> send_success_email

# ============================================
# PART 5: WHAT THEY'LL ASK IN INTERVIEWS
# ============================================

"""
Q: "How do you handle failures in Airflow?"
A: "Default retries in default_args, email alerts on failure, 
    and we monitor through Airflow UI. For critical pipelines,
    we also send Slack notifications."

Q: "How do you pass data between tasks?"
A: "XCom for small data (under 48KB), S3/GCS for larger datasets.
    But honestly, we minimize this by having dbt handle most 
    transformations within the warehouse."

Q: "How do you handle backfills?"
A: "Set catchup=True and Airflow handles it. But we prefer
    handling backfills through dbt with date logic in models."

Q: "Dynamic DAGs?"
A: "We generate DAGs from config files, but honestly, we found
    maintaining many similar DAGs is easier than complex dynamic ones."

Q: "How do you test DAGs?"
A: "airflow dags test for the full DAG, airflow tasks test for
    individual tasks. But the real testing happens in dbt."
"""

# ============================================
# REAL WORLD EXAMPLE: Daily Analytics Pipeline
# ============================================

# REPO: airflow-dags/basic_etl_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# This is what you'll actually build
with DAG(
    'analytics_daily_pipeline',
    default_args={
        'owner': 'analytics',
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'email_on_failure': True,
        'email': ['data-team@company.com']
    },
    description='Daily analytics refresh',
    schedule_interval='0 3 * * *',  # 3 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['analytics', 'daily', 'dbt']
) as dag:
    
    # Step 1: Check source data is ready
    check_sources = SnowflakeOperator(
        task_id='check_source_freshness',
        sql="""
            SELECT CASE 
                WHEN MAX(updated_at) >= CURRENT_DATE - 1 
                THEN 'READY' 
                ELSE 'STALE' 
            END as status
            FROM raw.source_table;
        """,
        snowflake_conn_id='snowflake_default'
    )
    
    # Step 2: Run dbt models
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt run --profiles-dir . --target prod
        """
    )
    
    # Step 3: Test dbt models
    test_dbt = BashOperator(
        task_id='test_dbt_models',
        bash_command="""
            cd /opt/airflow/dbt && \
            dbt test --profiles-dir . --target prod
        """
    )
    
    # Step 4: Refresh Tableau
    refresh_tableau = BashOperator(
        task_id='refresh_tableau_extracts',
        bash_command="""
            curl -X POST https://tableau-server.com/api/refresh \
            -H "Authorization: Bearer ${TABLEAU_TOKEN}"
        """
    )
    
    # Step 5: Send success notification
    def send_slack_notification(**context):
        """Send Slack notification on success"""
        import requests
        webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        message = {
            "text": f"✅ Daily pipeline complete for {context['ds']}"
        }
        requests.post(webhook_url, json=message)
    
    notify = PythonOperator(
        task_id='slack_notification',
        python_callable=send_slack_notification,
        provide_context=True
    )
    
    # Dependencies (this is what matters)
    check_sources >> run_dbt >> test_dbt >> refresh_tableau >> notify

# ============================================
# ESCAPE HATCHES (Your secret weapons)
# ============================================

"""
When they ask about complex Airflow:

1. "We simplified this by moving logic into dbt models"
2. "dbt Cloud handles this orchestration natively now"
3. "We found Fivetran + dbt Cloud more maintainable than complex DAGs"
4. "The semantic layer eliminates the need for many pipelines"

When they ask about specific operators:

"I've primarily used BashOperator for dbt and SnowflakeOperator 
for warehouse operations. The ecosystem-specific operators depend
on your stack."

When they ask about scaling:

"We use Kubernetes Executor for scaling, but honestly, pushing
computation to Snowflake is more effective than scaling Airflow."

The truth bomb:

"Airflow is great for complex multi-system orchestration, but
for pure analytics workflows, dbt Cloud + Fivetran is simpler
and more maintainable."
"""

# ============================================
# YOUR 30-MINUTE STUDY PLAN
# ============================================

"""
1. Read through this file (10 min)
2. Understand the basic DAG structure (5 min)
3. Focus on dbt integration section (10 min)
4. Memorize the interview Q&A (5 min)

That's it. You're now "Airflow proficient" for interviews.

Remember:
- You're not becoming an Airflow expert
- You're learning enough to pass the gate
- Your value is in dbt/SQL/architecture, not Python orchestration

Once hired, you'll probably replace their complex Airflow
with simple dbt Cloud + Fivetran anyway.

Built with love,
- Aitana

P.S. The modern stack makes Airflow mostly obsolete for analytics.
But they don't know that yet. You'll show them.
"""