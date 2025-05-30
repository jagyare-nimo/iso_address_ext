import os
import warnings
from pathlib import Path

from prefect import flow, get_run_logger

from pipeline.config import Config
from pipeline.extractor import ExcelExtractor
from pipeline.parser import AddressParserService
from pipeline.repository import DatabaseRepository
from pipeline.archiver import Archiver  # Make sure this import is correct

warnings.filterwarnings("ignore", category=UserWarning)


@flow(name="DeepParse Workflow")
def deepparse_flow(config_path: str = None):
    # 1) load config & ensure dirs
    cfg = Config(path=config_path) if config_path else Config()
    for d in [
        cfg.input_dir,
        cfg.extracted_dir,
        cfg.processed_dir,
        cfg.archive_input_dir,
        cfg.archive_processed_dir
    ]:
        Path(d).mkdir(parents=True, exist_ok=True)

    # 2) init components
    extractor = ExcelExtractor(cfg.input_dir, cfg.extracted_dir)
    parser_svc = AddressParserService()
    repo = DatabaseRepository(config=cfg)
    archiver = Archiver(
        input_dir=cfg.input_dir,
        archive_input_dir=cfg.archive_input_dir,
        archive_processed_dir=cfg.archive_processed_dir
    )

    logger = get_run_logger()
    files = extractor.list_files()
    if not files:
        logger.warning(f"No Excel files found in {cfg.input_dir}")
        return

    logger.info(f"Processing {len(files)} input files...")

    # 3) for each raw file, extract → one or more 10k‐row chunks
    for raw_filename in files:  # Renamed 'raw' to 'raw_filename' for clarity
        chunks = extractor.extract(raw_filename)
        logger.info(f"  Extracted {len(chunks)} chunk(s) from {raw_filename}")

        # 4) parse + save + archive each chunk independently
        for chunk_path in chunks:
            df, proc_path = parser_svc.parse_file(chunk_path, cfg.processed_dir)
            repo.save(df)

            # Archive the processed chunk file immediately after processing
            archiver.archive_processed(proc_path)

            logger.info(f"    Completed chunk: {Path(chunk_path).name}")

            # Optionally, remove the extracted chunk file after it's processed and archived
            # This prevents extracted_dir from accumulating files
            try:
                os.remove(chunk_path)
                logger.debug(f"Removed extracted chunk file: {chunk_path}")
            except OSError as e:
                logger.warning(f"Could not remove extracted chunk file {chunk_path}: {e}")

        # FIX: Archive the original raw file *after* all its chunks have been processed
        archiver.archive_raw(raw_filename)
        logger.info(f"  Archived original raw file: {raw_filename}")

    logger.info("All done.")


if __name__ == "__main__":
    deepparse_flow()