from airflow.hooks.S3_hook import S3Hook
import os

# Define the function to read the file from S3
def read_file_from_s3(**kwargs):
    # Create an S3Hook to interact with Amazon S3
    s3_hook = S3Hook(aws_conn_id=kwargs["s3_conn_id"])

    # Download the file from S3 to the local file system
    try:
        s3_hook.download_file(kwargs["s3_bucket_name"], kwargs["s3_nodes_file_name"], kwargs["local_nodes_file_path"])
    except Exception as e:
        print("Nodes Download Failed")

    try:
        s3_hook.download_file(kwargs["s3_bucket_name"], kwargs["s3_edges_file_name"], kwargs["local_edges_file_path"])
    except Exception as e:
        print("Edges Download Failed")

def rename_file(new_nodes_name, new_edges_name, ti):
    downloaded_file_name = ti.xcom_pull(task_ids='download_from_s3_task', key='local_nodes_file_path')
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_nodes_name}")

    downloaded_file_name = ti.xcom_pull(task_ids='download_from_s3_task', key='local_edges_file_path')
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_edges_name}")


    
