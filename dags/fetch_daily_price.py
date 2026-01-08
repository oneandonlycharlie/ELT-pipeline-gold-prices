
from airflow import DAG
from airflow.sdk import task
from airflow.providers.microsoft.azure.operators.container_instances import AzureContainerInstancesOperator
from datetime import timedelta

with DAG(
    dag_id = "fetch_daily_price",
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=10)
    },
    schedule='@hourly',
    catchup=False,
    tags=['azure', 'container_instance'],
) as dag:
    
    task = AzureContainerInstancesOperator(
        ci_conn_id='azure_container_instances_default',
        task_id='run_gold_price_pipeline',
        image='charlieeeegu/gold-pipeline:1.0',
        resource_group='rg-gold-pipeline-prod',
        name="gold-pipeline-prod-{{ ts_nodash | lower }}",
        region="switzerlandnorth",
        environment_variables={
            'PYTHONUNBUFFERED': '1'
        },
        command=["python", "-m", "ingestor.main", "--date", "{{ ds }}"]
    )
    
    