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
        except shutil.Error as e:  # Catch shutil.Error specifically
            if "already exists" in str(e):
                logger.warning(f"Processed file already exists in archive, skipping move: {processed_dst}")
                # Optionally, you could delete the source file here if you're certain it's a duplicate
                # and you want to remove it from the extracted directory.
                # os.remove(processed_file_path)
            else:
                logger.error(f"Error archiving processed file {processed_file_path}: {e}")
                raise  # Re-raise other shutil errors

        except Exception as e:  # Catch any other general exceptions
            logger.error(f"Unexpected error archiving processed file {processed_file_path}: {e}")
            raise  # Re-raise unexpected errors
