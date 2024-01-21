import yaml

def read_yaml(file):
    stream = open(file, 'r')
    config = yaml.safe_load(stream)

    return config
