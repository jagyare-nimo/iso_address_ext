import os
import shutil
import logging

logger = logging.getLogger(__name__)


class Archiver:
    def __init__(self, input_dir: str, archive_input_dir: str, archive_processed_dir: str):
        self.input_dir = input_dir
        self.archive_input_dir = archive_input_dir
        self.archive_processed_dir = archive_processed_dir
        os.makedirs(self.archive_input_dir, exist_ok=True)
        os.makedirs(self.archive_processed_dir, exist_ok=True)

    def archive_raw(self, raw_filename: str):
        """
        Archives the original raw input file.
        """
        raw_src = os.path.join(self.input_dir, raw_filename)
        raw_dst = os.path.join(self.archive_input_dir, raw_filename)
        try:
            if os.path.exists(raw_src):
                shutil.move(raw_src, raw_dst)
                logger.info(f"Archived raw file: {raw_src} -> {raw_dst}")
            else:
                logger.warning(f"Raw file not found for archiving: {raw_src}")
        except Exception as e:
            logger.error(f"Error archiving raw file {raw_src}: {e}")
            # Do not re-raise, as the flow might continue even if raw archiving fails
            pass

    def archive_processed(self, processed_file_path: str):
        """
        Archives a processed chunk file.
        """
        processed_file_name = os.path.basename(processed_file_path)
        processed_dst = os.path.join(self.archive_processed_dir, processed_file_name)
        try:
            shutil.move(processed_file_path, processed_dst)
            logger.info(f"Archived processed chunk: {processed_file_path} -> {processed_dst}")
        except Exception as e:
            # If something goes wrong here, it's a critical archiving failure for the chunk
            logger.error(f"Error archiving processed file {processed_file_path}: {e}")
            raise  # Re-raise this exception as it indicates a problem with the processed file
