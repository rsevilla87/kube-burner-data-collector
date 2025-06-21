import yaml


class Config:
    def __init__(self, config_file):
        """Init method for instance variables"""
        self.config_file = config_file

    def parse(self):
        """loads the YAML file for parsing"""
        with open(self.config_file) as f:
            return yaml.safe_load(f)
