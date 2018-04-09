import os
import yaml

def get_config(path='config.yml'):
	""" Load a yaml config file with name path
		from the configs directory
	"""
	path = os.path.join('configs', path)
    
    if not os.path.exists(path):
        raise Exception("Missing config file %s" % path)

    with open(path, 'r') as f:
        return yaml.load(f)
