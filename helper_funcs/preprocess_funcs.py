import pandas as pd
import numpy as np

def label_creation(row):
    if row["Bankruptcy"] == "Bankruptcy":
        return row["Bankruptcy"]
    else:
        return row["~label"]

def preprocess_nodes(local_nodes_file_path, new_nodes_name):
    downloaded_file_path = '/'.join(local_nodes_file_path.split('/')[:-1])
    downloaded_file_path = downloaded_file_path + '/' + new_nodes_name
    df_nodes = pd.read_csv(downloaded_file_path)
    df_nodes["~label"] = df_nodes["~label"] + '_' + df_nodes["groupRole"] + '_' + df_nodes["BusinessGroupID"].astype(str)
    df_nodes["~label"] = df_nodes.apply(label_creation, axis=1)
    df_nodes.to_csv(downloaded_file_path, index=False)
