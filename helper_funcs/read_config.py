import yaml

def read_yaml(file):
    stream = open(file, 'r')
    config = yaml.safe_load(stream)

    return config

print(read_yaml(r"F:\Data_Engineering\Apache_Airflow\Neptune_Airflow\config\config.yaml"))