import datetime
import os
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=UserWarning)


class ExcelExtractor:
    REQUIRED_COLUMNS = ['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'ADDRESSLINE3']

    def __init__(self, input_dir: str, extracted_dir: str):
        self.input_dir = input_dir
        self.extracted_dir = extracted_dir
        os.makedirs(self.extracted_dir, exist_ok=True)

    def list_files(self) -> list[str]:
        return [
            f for f in os.listdir(self.input_dir)
            if f.lower().endswith(('.xls', '.xlsx'))
        ]

    def extract(self, filename: str) -> str:
        # 1) read
        in_path = os.path.join(self.input_dir, filename)
        df = pd.read_excel(in_path, engine='openpyxl')[self.REQUIRED_COLUMNS]

        # 2) compute one timestamp for this file
        ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")

        # 3) write out as <stem>_<ts>_extracted.xlsx
        stem, ext = Path(filename).stem, Path(filename).suffix
        out_name = f"{stem}_{ts}_extracted{ext}"
        out_path = os.path.join(self.extracted_dir, out_name)
        df.to_excel(out_path, index=False)

        return out_path
