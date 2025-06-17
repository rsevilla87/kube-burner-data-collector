import yaml


class Config:
    def __init__(self, config_file):
        self.config_file = config_file

    def parse(self):
        with open(self.config_file) as f:
            return yaml.safe_load(f)
