import os
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
    logger = get_run_logger()

    config_file = (
        Path(config_path)
        if config_path
        else Path(__file__).resolve().parents[1] / "resources" / "config.yml"
    )
    config_path = Config(str(config_file))

    for dirs in (
            config_path.input_dir,
            config_path.extracted_dir,
            config_path.processed_dir,
            config_path.archive_input_dir,
            config_path.archive_processed_dir,
    ):
        os.makedirs(dirs, exist_ok=True)

    extractor = ExcelExtractor(config_path.input_dir, config_path.extracted_dir)
    parser_svc = AddressParserService()
    repo = DatabaseRepository(
        table_name=config_path.table_name,
        datasource_url=config_path.datasource_url,
        driver_class=config_path.datasource_driver,
        username=config_path.datasource_username,
        password=config_path.datasource_password,
        jar_path=str(Path(__file__).resolve().parents[1] / "resources" / "data" / "h2-2.3.232.jar")
    )
    archiver = Archiver(
        config_path.input_dir,
        config_path.archive_input_dir,
        config_path.archive_processed_dir,
    )

    files = extractor.list_files()
    logger.info(f"Processing {len(files)} files...")

    for file in files:
        extracted_path = extractor.extract(file)
        parsed_df, processed_path = parser_svc.parse_file(
            extracted_path,
            config_path.processed_dir,
        )

        logger.info("ISO data for %s:\n%s", file, parsed_df.to_string(index=False))

        repo.save(parsed_df)
        archiver.archive(file, processed_path)
        logger.info(f"Completed {file}")


if __name__ == "__main__":
    deepparse_flow()
