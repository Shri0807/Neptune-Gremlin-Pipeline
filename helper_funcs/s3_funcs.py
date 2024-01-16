from airflow.hooks.S3_hook import S3Hook
import os

# Define the function to read the file from S3
def read_file_from_s3(s3_conn_id, s3_bucket_name, s3_nodes_file_name, local_nodes_file_path, s3_edges_file_name, local_edges_file_path, ti):

    # Create an S3Hook to interact with Amazon S3
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    # Download the file from S3 to the local file system
    s3_hook.download_file(s3_bucket_name, s3_nodes_file_name, local_nodes_file_path)
    ti.xcom_push(key="local_nodes_file_path", value=local_nodes_file_path)

    s3_hook.download_file(s3_bucket_name, s3_edges_file_name, local_edges_file_path)
    ti.xcom_push(key="local_edges_file_path", value=local_edges_file_path)

    return local_nodes_file_path, local_edges_file_path

def rename_file(new_nodes_name, new_edges_name, ti):
    downloaded_file_name = ti.xcom_pull(task_ids='download_from_s3_task', key='local_nodes_file_path')
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_nodes_name}")

    downloaded_file_name = ti.xcom_pull(task_ids='download_from_s3_task', key='local_edges_file_path')
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_edges_name}")


    
