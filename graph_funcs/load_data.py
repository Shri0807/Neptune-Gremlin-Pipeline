import pandas as pd
import requests, json

#port = 8182
#server = "neptune-database-1.cluster-c7w9wbbkmgi2.us-east-2.neptune.amazonaws.com"
#endpoint = f"https://{server}:{port}/loader"

#iam_role = "arn:aws:iam::118852682248:role/NeptuneLoadFromS3"

# def load_data(source, endpoint = "https://neptune-database-1.cluster-c7w9wbbkmgi2.us-east-2.neptune.amazonaws.com:8182/loader", iam_role = "arn:aws:iam::118852682248:role/NeptuneLoadFromS3"):
def load_data_neptune(**kwargs):
    """
    Parameters:
        endpoint: Endpoint of Neptune Database
        source: S3 File URI
        iam_role: arm of IAM Role for Read Data from S3

    Returns:
        status_code: 200 = success
    """
    # source = "s3://shrikant-neptune-testing/Data_02_09_2023/nodes.csv"
    source = f"s3://{kwargs['s3_bucket_name']}/{kwargs['preprocessed_nodes_file_name']}"
    loading_endpoint = f"https://{kwargs['server']}:{int(kwargs['port'])}/{kwargs['loading_endpoint']}"

    header = {'Content-Type': 'application/json'}
    data = {
        "source" : source,
        "format" : "csv",
        "iamRoleArn" : kwargs['iam_role'],
        "region" : "us-east-2",
        "failOnError" : "FALSE",
        "parallelism" : "MEDIUM",
        "updateSingleCardinalityProperties" : "TRUE",
        "queueRequest" : "TRUE"
    }
    
    resp = requests.post(loading_endpoint, data=json.dumps(data), headers=header)
    
    if resp.status_code == 200:
        print("File Loaded")
    
    else:
        print("File Not Loaded")