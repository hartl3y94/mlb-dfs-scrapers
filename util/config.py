import os
import yaml

def get_config(path='config.yml'):
    
    if not os.path.exists(path):
        raise Exception("Missing config file %s" % path)

    with open(path, 'r') as f:
        return yaml.load(path)
