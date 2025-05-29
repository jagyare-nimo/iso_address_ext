import os
import shutil
from pathlib import Path


class Archiver:
    def __init__(self, input_dir: str, archive_input_dir: str, archive_processed_dir: str):
        self.input_dir = input_dir
        self.archive_input_dir = archive_input_dir
        self.archive_processed_dir = archive_processed_dir
        os.makedirs(self.archive_input_dir, exist_ok=True)
        os.makedirs(self.archive_processed_dir, exist_ok=True)

    def archive(self, original: str, processed_file: str):
        # pull the same timestamp
        safe_ts = Path(processed_file).stem.split("_")[-1]

        # rename raw to <stem>_<ts><ext>
        stem, ext = Path(original).stem, Path(original).suffix
        new_raw = f"{stem}_{safe_ts}{ext}"

        shutil.move(
            os.path.join(self.input_dir, original),
            os.path.join(self.archive_input_dir, new_raw)
        )
        shutil.move(processed_file, self.archive_processed_dir)
