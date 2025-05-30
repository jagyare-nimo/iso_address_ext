import yaml
from pathlib import Path


class Config:
    """
    Loads configuration from a config file, focusing exclusively on uni_fen_dir paths.
    """

    def __init__(self, path: str = None):
        self.project_root = Path(__file__).resolve().parents[1]

        if path:
            config_path = Path(path)
        else:
            config_path = self.project_root / 'resources' / 'config.yml'

        if not config_path.is_file():
            raise FileNotFoundError(f"Config file not found at '{config_path}'")

        config_file = yaml.safe_load(config_path.read_text()) or {}

        def _resolve_path(relative_path_str: str) -> str:
            if relative_path_str:
                return str((self.project_root / relative_path_str).resolve())
            return ''

        uni_fen_dir_config = config_file.get('uni_fen_dir', {}) or {}

        self.uni_fen_dir_raw = _resolve_path(uni_fen_dir_config.get('raw', ''))
        self.uni_fen_dir_processed = _resolve_path(uni_fen_dir_config.get('processed', ''))
        self.uni_fen_dir_archive = _resolve_path(uni_fen_dir_config.get('archive', ''))  # NEW: Archive path

    def __repr__(self):
        return (
            f"<Config uni_fen_dir_raw={self.uni_fen_dir_raw!r}, "
            f"uni_fen_dir_processed={self.uni_fen_dir_processed!r}, "
            f"uni_fen_dir_archive={self.uni_fen_dir_archive!r}>"  # NEW: Added to repr
        )