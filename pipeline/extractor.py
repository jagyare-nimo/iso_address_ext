import os
import datetime
from pathlib import Path

import pandas as pd


class ExcelExtractor:
    """
    Reads large Excel sheets and slices them into 10 000‐row chunks,
    writing each out as <stem>_<timestamp>_part<N>_extracted.xlsx.
    """

    REQUIRED_COLUMNS = ['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'ADDRESSLINE3']

    def __init__(self, input_dir: str, extracted_dir: str):
        self.input_dir = Path(input_dir)
        self.extracted_dir = Path(extracted_dir)
        self.extracted_dir.mkdir(parents=True, exist_ok=True)

    def list_files(self) -> list[str]:
        return [
            f.name for f in self.input_dir.iterdir()
            if f.suffix.lower() in ('.xls', '.xlsx')
        ]

    def extract(self, filename: str) -> list[str]:
        """
        Returns a list of one or more chunk‐file paths under self.extracted_dir.
        """
        src = self.input_dir / filename
        df = pd.read_excel(src, engine='openpyxl')[self.REQUIRED_COLUMNS]

        ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        stem, ext = src.stem, src.suffix

        out_paths = []
        # slice into 10k‐row pieces
        for part_idx, start in enumerate(range(0, len(df), 10_000), start=1):
            chunk = df.iloc[start:start + 10_000]
            out_name = f"{stem}_{ts}_part{part_idx}_extracted{ext}"
            out_path = self.extracted_dir / out_name
            chunk.to_excel(out_path, index=False)
            out_paths.append(str(out_path))

        return out_paths
