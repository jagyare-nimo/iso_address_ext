import yaml
from pathlib import Path


class Config:
    def __init__(self, path: str = None):
        config_path = Path(path) if path else Path(__file__).resolve().parents[1] / 'resources' / 'config.yml'
        if not config_path.is_file():
            raise FileNotFoundError(f"Config not found at {config_path}")
        config_file = yaml.safe_load(config_path.read_text())

        self.input_dir = config_file['input_dir']
        self.extracted_dir = config_file['extracted_dir']
        self.processed_dir = config_file['processed_dir']
        self.archive_input_dir = config_file['archive']['input_dir']
        self.archive_processed_dir = config_file['archive']['processed_dir']

        datasource = config_file.get('datasource', {})
        self.datasource_url = datasource.get('url')
        self.datasource_driver = datasource.get('driverClassName')
        self.datasource_username = datasource.get('username')
        self.datasource_password = datasource.get('password')

        database = config_file.get('database', {})
        self.table_name = database.get('table_name')
