import yaml

def read_yaml(file):
    """
    Description:
        A function to load YAML File and return a Dictionary
    
    Parameters:
        file: Path of YAML File
    
    Returns:
        config: Dictionary of Values in YAML File
    """
    stream = open(file, 'r')
    config = yaml.safe_load(stream)

    return config
