import pandas as pd
import numpy as np

def label_creation(row):
    if row["Bankruptcy"] == "Bankruptcy":
        return row["Bankruptcy"]
    else:
        return row["~label"]

def preprocess_nodes(**kwargs):
    nodes_path = kwargs["nodes_local_path"] + kwargs["s3_nodes_file_name"]
    nodes_final_path = kwargs["nodes_local_path"] + kwargs["preprocessed_nodes_file_name"]
    df_nodes = pd.read_csv(nodes_path)
    df_nodes["~label"] = df_nodes["~label"] + '_' + df_nodes["groupRole"] + '_' + df_nodes["BusinessGroupID"].astype(str)
    df_nodes["~label"] = df_nodes.apply(label_creation, axis=1)
    df_nodes.to_csv(nodes_final_path, index=False)
