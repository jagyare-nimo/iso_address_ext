import yaml
from pathlib import Path


class Config:

    def __init__(self, path: str = None):
        project_root = Path(__file__).resolve().parents[1]
        if path:
            config_path = Path(path)
        else:
            config_path = project_root / 'resources' / 'config.yml'
        if not config_path.is_file():
            raise FileNotFoundError(f"Config not found at {config_path}")

        config_file = yaml.safe_load(config_path.read_text()) or {}

        def _resolve(key):
            val = config_file.get(key, '')
            # If val is an empty string, return empty string, otherwise resolve the path
            return str((project_root / val).resolve()) if val else ''

        self.input_dir = _resolve('input_dir')
        self.extracted_dir = _resolve('extracted_dir')
        self.processed_dir = _resolve('processed_dir')

        archive = config_file.get('archive', {}) or {}

        # FIX: Apply the same conditional resolution logic to archive paths
        archive_input_val = archive.get('input_dir', '')
        self.archive_input_dir = str((project_root / archive_input_val).resolve()) if archive_input_val else ''

        archive_processed_val = archive.get('processed_dir', '')
        self.archive_processed_dir = str(
            (project_root / archive_processed_val).resolve()) if archive_processed_val else ''

        datasource = config_file.get('datasource', {}) or {}
        self.datasource_url = datasource.get('url', '')
        self.datasource_driver = datasource.get('driverClassName', '')
        self.datasource_username = datasource.get('username', '')
        self.datasource_password = datasource.get('password', '')

        database = config_file.get('database', {}) or {}
        self.database_url = database.get('url', '')
        self.table_name = database.get('table_name', '')

    def __repr__(self):
        return (
            f"<Config input_dir={self.input_dir!r}, extracted_dir={self.extracted_dir!r}, "
            f"processed_dir={self.processed_dir!r}, archive_input_dir={self.archive_input_dir!r}, "
            f"archive_processed_dir={self.archive_processed_dir!r}, datasource_url={self.datasource_url!r}, "
            f"db_url={self.database_url!r}, table_name={self.table_name!r}>"
        )
