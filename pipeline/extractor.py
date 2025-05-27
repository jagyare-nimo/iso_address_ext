import os
import pandas as pd


class ExcelExtractor:
    REQUIRED_COLUMNS = ['ID', 'ADDRESSLINE1', 'ADDRESSLINE2', 'ADDRESSLINE3']

    def __init__(self, input_dir: str, extracted_dir: str):
        self.input_dir = input_dir
        self.extracted_dir = extracted_dir
        os.makedirs(self.extracted_dir, exist_ok=True)

    def list_files(self) -> list[str]:
        return [ici_file for ici_file in os.listdir(self.input_dir)
                if ici_file.lower().endswith(('.xls', '.xlsx'))]

    def extract(self, filename: str) -> str:
        in_path = os.path.join(self.input_dir, filename)
        ici_file_data = pd.read_excel(in_path, engine='openpyxl')
        data_extracted = ici_file_data[self.REQUIRED_COLUMNS].copy()
        data_extracted = data_extracted.rename(columns={
            'ID': 'ID',
            'ADDRESSLINE1': 'ADDRESSLINE1',
            'ADDRESSLINE2': 'ADDRESSLINE2',
            'ADDRESSLINE3': 'ADDRESSLINE3'
        })

        out_file = os.path.join(self.extracted_dir, f'extracted_{filename}')
        data_extracted.to_excel(out_file, index=False)
        return out_file
