import sys

sys.path.insert(0, "/root/Neptune-Gremlin-Pipeline/helper_funcs")
sys.path.insert(0, "/root/Neptune-Gremlin-Pipeline/graph_funcs")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from s3_funcs import read_file_from_s3
from read_config import read_yaml

config = read_yaml("/root/Neptune-Gremlin-Pipeline/config/config.yaml")
config = config['development']

# Define default_args dictionary to specify the default parameters for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    default_args=default_args,
    dag_id="neptune_dag_v1",
    description="A DAG to read data from S3, preprocess it and load it to amazon neptune"
) as dag:
    
    download_from_s3_task = PythonOperator(
        task_id = "download_from_s3_task",
        python_callable=read_file_from_s3,
        op_kwargs={
            's3_conn_id': config['s3_conn_id'],
            's3_bucket_name': config['s3_bucket_name'],
            's3_nodes_file_name': config['s3_nodes_file_name'],
            'local_nodes_file_path': config['nodes_local_path'],
            's3_edges_file_name': config['s3_edges_file_name'],
            'local_edges_file_path': config['edges_local_path']
        }
    )

    # rename_file = PythonOperator(
    #     task_id = "rename_file",
    #     python_callable=rename_file,
    #     op_kwargs={
    #         'new_nodes_name': config['nodes_file_name'],
    #         'new_edges_name': config['edges_file_name']
    #     }
    # )

    # preprocess_nodes = PythonOperator(
    #     task_id="preprocess_nodes",
    #     python_callable=preprocess_nodes,
    #     op_kwargs={
    #         'local_nodes_file_path': config['nodes_local_path'],
    #         'new_nodes_name': config['nodes_file_name']
    #     }

    # )

    download_from_s3_task



    
