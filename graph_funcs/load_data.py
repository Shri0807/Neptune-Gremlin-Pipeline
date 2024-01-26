import pandas as pd
import requests, json

def load_data_neptune(**kwargs):
    """
    Description:
        A function to load data into Neptune Database from an S3 source. 
    
    Parameters:
        s3_bucket_name: Bucket Name of S3 Bucket
        preprocessed_nodes_file_name: Name of the file to be loaded on Neptune
        server: Server Name of Neptune Database
        port: Port Number of Neptune Database (Default=8182)
        loading_endpoint: Loading Endpoint of Neptune Database (/loader)
        source: S3 File URI
        iam_role: arn of IAM Role for Read Data from S3

    Returns:
        status_code: HTTP status code indicating the success of the data loading process.
                     200 = Success, Other codes = Failure.
    """

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