import pandas as pd
import numpy as np

def label_creation(row):
    if row["Bankruptcy"] == "Bankruptcy":
        return row["Bankruptcy"]
    else:
        return row["~label"]

def preprocess_nodes(**kwargs):
    """
    Description:
        A function to preprocess and transform raw node data, creating a labeled and refined dataset
        for subsequent operations in a Neptune Database.
    
    Parameters:
        nodes_local_path: Path to the nodes data directory
        s3_nodes_file_name: Name of the raw nodes CSV file in the S3 bucket.
        preprocessed_nodes_file_name: Name of the preprocessed nodes CSV file to be saved
    
    Returns:
        None
    """
    nodes_path = kwargs["nodes_local_path"] + kwargs["s3_nodes_file_name"]
    nodes_final_path = kwargs["nodes_local_path"] + kwargs["preprocessed_nodes_file_name"]
    df_nodes = pd.read_csv(nodes_path)
    df_nodes["~label"] = df_nodes["~label"] + '_' + df_nodes["groupRole"] + '_' + df_nodes["BusinessGroupID"].astype(str)
    df_nodes["~label"] = df_nodes.apply(label_creation, axis=1)
    df_nodes["~id"] = df_nodes["~id"].astype(str)
    df_nodes = df_nodes[["~id", "CustomerName", "~label", "BusinessGroupName"]]
    df_nodes.to_csv(nodes_final_path, index=False)

def preprocess_edges(**kwargs):
    """
    Description:
        A function to preprocess and refine raw edge data, ensuring data consistency and preparing
        it for subsequent operations in a Neptune Database.
    
    Parameters:
        edges_local_path: Path to the edges data directory
        s3_edges_file_name: Name of the raw edges CSV file in the S3 bucket.
        preprocessed_edges_file_name: Name of the preprocessed edges CSV file to be saved

    Returns:
        None  
    """
    edges_path = kwargs["edges_local_path"] + kwargs["s3_edges_file_name"]
    edges_final_path = kwargs["edges_local_path"] + kwargs["preprocessed_edges_file_name"]
    df_edges = pd.read_csv(edges_path)
    
    df_edges = df_edges.replace(to_replace=r'\s+', value=' ', regex=True)
    df_edges["~from"] = df_edges["~from"].astype(str)
    df_edges["~to"] = df_edges["~to"].astype(str)
    df_edges.to_csv(edges_final_path, index=False)

def final_df_creation(**kwargs):
    """
    Description:
        A function to create the final DataFrame by merging results from different tasks.
        It merges the DataFrame obtained from the 'shortest_path_task' with the DataFrame
        obtained from the 'community_detection_task' on the '~id' column.
    
    Parameters:
        ti (TaskInstance): Airflow task instance providing access to XCom values.
        local_file_path: Local path to the directory for storing the final CSV file.
        output_file_name: Name of the final output CSV file.
    
    Returns:
        None
    """

    df_nodes = kwargs['ti'].xcom_pull(task_ids='shortest_path_task')
    new_df = kwargs['ti'].xcom_pull(task_ids='community_detection_task')

    new_df = pd.merge(new_df, df_nodes, on="~id", how="left")

    final_path = kwargs['local_file_path'] + kwargs['output_file_name']

    new_df.to_csv(final_path, index=False)
