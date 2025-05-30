import os
from pathlib import Path
from datetime import datetime
import shutil
import yaml
from config import Config
from converter import UnixLFConverter


def setup_directories(config: Config):
    print(f"Ensuring directory: {config.uni_fen_dir_raw}")
    Path(config.uni_fen_dir_raw).mkdir(parents=True, exist_ok=True)
    print(f"Ensuring directory: {config.uni_fen_dir_processed}")
    Path(config.uni_fen_dir_processed).mkdir(parents=True, exist_ok=True)
    print(f"Ensuring directory: {config.uni_fen_dir_archive}")  # NEW: Ensure archive dir
    Path(config.uni_fen_dir_archive).mkdir(parents=True, exist_ok=True)


def run_conversion_process():
    print("Starting the conversion process...")

    try:
        cfg = Config()
        print(f"Configuration loaded: {cfg}")
    except FileNotFoundError as e:
        print(f"Configuration error: {e}")
        print("Please ensure 'config.yml' is located at 'unix_lf_converter/resources/config.yml'")
        return
    except yaml.YAMLError as e:
        print(f"Error parsing config.yml: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while loading config: {e}")
        return

    try:
        setup_directories(cfg)
        print("Required directories ensured.")
    except Exception as e:
        print(f"Error setting up directories: {e}")
        return

    converter = UnixLFConverter()

    input_files = []
    if Path(cfg.uni_fen_dir_raw).exists():
        for f in Path(cfg.uni_fen_dir_raw).iterdir():
            if f.is_file() and f.suffix.lower() in ('.csv', '.txt'):
                input_files.append(f)

    if not input_files:
        print(f"No .csv or .txt files found in input directory: {cfg.uni_fen_dir_raw}")
        print("Please place your input CSV files in this directory.")
        return

    print(f"Found {len(input_files)} file(s) to process.")

    for input_file_path in input_files:
        timestamp = datetime.now().strftime("_%Y%m%d%H%M%S")
        output_file_name = input_file_path.stem + timestamp + "_converted.txt"
        output_file_path = Path(cfg.uni_fen_dir_processed) / output_file_name

        print(f"\nProcessing '{input_file_path.name}'...")
        success = converter.convert_csv_to_text(str(input_file_path), str(output_file_path))

        if success:
            print(f"Successfully processed '{input_file_path.name}'.")
            archive_raw_path = Path(cfg.uni_fen_dir_archive) / input_file_path.name
            try:
                shutil.move(str(input_file_path), str(archive_raw_path))
                print(f"Archived raw file: '{input_file_path.name}' to '{archive_raw_path}'")
            except Exception as e:
                print(f"Error archiving raw file '{input_file_path.name}': {e}")
        else:
            print(f"Failed to process '{input_file_path.name}'. Raw file not moved to archive.")

    print("\nConversion process finished.")

if __name__ == "__main__":
    run_conversion_process()
