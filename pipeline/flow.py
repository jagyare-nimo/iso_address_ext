import warnings
from pathlib import Path

from prefect import flow, get_run_logger

from pipeline.config import Config
from pipeline.extractor import ExcelExtractor
from pipeline.parser import AddressParserService
from pipeline.repository import DatabaseRepository
from pipeline.archiver import Archiver

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
    repo       = DatabaseRepository(config=cfg)
    archiver   = Archiver(
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
    for raw in files:
        chunks = extractor.extract(raw)
        logger.info(f"  Extracted {len(chunks)} chunk(s) from {raw}")

        # 4) parse + save + archive each chunk independently
        for chunk_path in chunks:
            df, proc_path = parser_svc.parse_file(chunk_path, cfg.processed_dir)
            repo.save(df)
            # move original raw (only once per raw file) and each chunk’s processed
            # we archive raw under its original name only once:
            archiver.archive(Path(chunk_path).name, proc_path)
            logger.info(f"    Completed chunk: {Path(chunk_path).name}")

    logger.info("All done.")


if __name__ == "__main__":
    deepparse_flow()