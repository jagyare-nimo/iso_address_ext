import os
import shutil


class Archiver:
    def __init__(self, input_dir: str, archive_input_dir: str, archive_processed_dir: str):
        self.input_dir = input_dir
        self.archive_input_dir = archive_input_dir
        self.archive_processed_dir = archive_processed_dir
        os.makedirs(self.archive_input_dir, exist_ok=True)
        os.makedirs(self.archive_processed_dir, exist_ok=True)

    def archive(self, original_raw_name: str, processed_file: str):
        # 1) Move raw only if it's still there
        raw_src = os.path.join(self.input_dir, original_raw_name)
        raw_dst = os.path.join(self.archive_input_dir, original_raw_name)
        try:
            if os.path.exists(raw_src):
                shutil.move(raw_src, raw_dst)
        except Exception:
            # swallow any error moving raw (e.g. already moved)
            pass

        # 2) Move the processed chunk
        try:
            shutil.move(processed_file, self.archive_processed_dir)
        except Exception as e:
            # if something goes wrong here, let it bubble
            raise