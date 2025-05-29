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
    config_dir = Config(path=config_path) if config_path else Config()

    for dirs in [
        config_dir.input_dir,
        config_dir.extracted_dir,
        config_dir.processed_dir,
        config_dir.archive_input_dir,
        config_dir.archive_processed_dir
    ]:
        Path(dirs).mkdir(parents=True, exist_ok=True)

    extractor = ExcelExtractor(config_dir.input_dir, config_dir.extracted_dir)
    parser_svc = AddressParserService()
    repo = DatabaseRepository(config=config_dir)
    archiver = Archiver(
        input_dir=config_dir.input_dir,
        archive_input_dir=config_dir.archive_input_dir,
        archive_processed_dir=config_dir.archive_processed_dir
    )

    logger = get_run_logger()
    files = extractor.list_files()
    if not files:
        logger.warning(f"No Excel files found in {config_dir.input_dir}")
    else:
        logger.info(f"Processing {len(files)} files...")

    for filename in files:
        extracted_path = extractor.extract(filename)
        parsed_df, processed_path = parser_svc.parse_file(extracted_path, config_dir.processed_dir)
        repo.save(parsed_df)

        archiver.archive(filename, processed_path)

        logger.info(f"Completed file: {filename}")


if __name__ == "__main__":
    deepparse_flow()
