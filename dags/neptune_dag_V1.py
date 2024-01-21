import sys

sys.path.insert(0, "/root/Neptune-Gremlin-Pipeline/helper_funcs")
sys.path.insert(0, "/root/Neptune-Gremlin-Pipeline/graph_funcs")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from s3_funcs import read_file_from_s3 ,load_file_to_s3
from read_config import read_yaml
from preprocess_funcs import preprocess_nodes ,preprocess_edges

from load_data import load_data_neptune
from shortest_path import shortest_path

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

    nodes_preprocess_task = PythonOperator(
        task_id = "nodes_preprocess_task",
        python_callable=preprocess_nodes,
        op_kwargs={
            'nodes_local_path': config['nodes_local_path'],
            's3_nodes_file_name': config['s3_nodes_file_name'],
            'preprocessed_nodes_file_name': config['preprocessed_nodes_file_name']
        }

    )

    edges_preprocess_task = PythonOperator(
        task_id = "edges_preprocess_task",
        python_callable=preprocess_edges,
        op_kwargs={
            'edges_local_path': config['edges_local_path'],
            's3_edges_file_name': config['s3_edges_file_name'],
            'preprocessed_edges_file_name': config['preprocessed_edges_file_name']
        }
    )

    load_to_s3_task = PythonOperator(
        task_id = "load_to_s3_task",
        python_callable=load_file_to_s3,
        op_kwargs={
            's3_conn_id': config['s3_conn_id'],
            's3_bucket_name': config['s3_bucket_name'],
            'nodes_local_path': config['nodes_local_path'],
            'preprocessed_nodes_file_name': config['preprocessed_nodes_file_name'],
            'edges_local_path': config['edges_local_path'],
            'preprocessed_edges_file_name': config['preprocessed_edges_file_name']
        }
    )

    load_to_neptune = PythonOperator(
        task_id = "load_to_neptune",
        python_callable=load_data_neptune,
        op_kwargs={
            's3_bucket_name': config['s3_bucket_name'],
            'preprocessed_nodes_file_name': config['preprocessed_nodes_file_name'],
            'server': config['server'],
            'port': config['port'],
            'loading_endpoint': config['loading_endpoint'],
            'iam_role': config['iam_role']
        }
    )

    shortest_path_task = PythonOperator(
        task_id = "shortest_path_task",
        python_callable=shortest_path,
        op_kwargs={
            'nodes_local_path': config['nodes_local_path'],
            'preprocessed_nodes_file_name': config['preprocessed_nodes_file_name'],
            'server': config['server'],
            'port': config['port'],
            'gremlin_endpoint': config['gremlin_endpoint']
        }
    )

    download_from_s3_task >> [nodes_preprocess_task, edges_preprocess_task]
    [nodes_preprocess_task, edges_preprocess_task] >> load_to_s3_task
    load_to_s3_task >> load_to_neptune
    load_data_neptune >> shortest_path_task



    
