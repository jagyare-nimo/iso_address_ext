import os
import re
import datetime
import warnings
import pandas as pd
from deepparse.parser import AddressParser
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

warnings.filterwarnings("ignore", category=UserWarning)

_COUNTRY_MAP = {
    'us': 'USA', 'usa': 'USA', 'united states': 'USA', 'united states of america': 'USA',
    'ca': 'CAN', 'can': 'CAN', 'canada': 'CAN',
    'uk': 'GB', 'gb': 'GB', 'great britain': 'GB', 'united kingdom': 'GB',
    'ch': 'CH', 'switzerland': 'CH',
    'bm': 'BM', 'bermuda': 'BM',
    'gt': 'GT', 'guatemala': 'GT',
    'il': 'IL', 'israel': 'IL',
    'ky': 'KY', 'cayman islands': 'KY', 'kentucky': 'KY',
    'no': 'NO', 'norway': 'NO',
    'pa': 'PA', 'panama': 'PA',
}

_US_STATES = {s.lower() for s in [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI",
    "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC",
    "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
    "VT", "VA", "WA", "WV", "WI", "WY"
]}

_CA_PROVINCES = {p.lower() for p in [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
]}


class AddressParserService:

    def __init__(self, workers: int = None):
        self.workers = workers or os.cpu_count()

    def _parse_batch(self, batch_rows):
        texts, idxs = batch_rows
        parser = AddressParser(offline=True)
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        try:
            from prefect import get_run_logger
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        df = pd.read_excel(extracted_path, engine='openpyxl')
        df['full_address'] = (
            df[['ADDRESSLINE1', 'ADDRESSLINE2', 'ADDRESSLINE3']]
            .fillna('').agg(' '.join, axis=1)
        )

        texts = df['full_address'].tolist()
        idxs = list(df.index)

        batch_size = 5000
        batches = [
            (texts[i:i + batch_size], idxs[i:i + batch_size])
            for i in range(0, len(texts), batch_size)
        ]

        all_dicts = {}
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = [executor.submit(self._parse_batch, b) for b in batches]
            for fut in futures:
                for idx, parsed_dict in fut.result():
                    all_dicts[idx] = parsed_dict

        timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')
        orig = Path(extracted_path).name
        stem, ext = Path(orig).stem, Path(orig).suffix
        filename = f"{stem}_{timestamp}{ext}"

        records = []
        for i, row in df.iterrows():
            d = all_dicts.get(i, {})
            state_raw = (d.get('state') or d.get('Province') or '').strip()
            country_raw = (d.get('country') or d.get('Country') or '').strip().lower()

            country = _COUNTRY_MAP.get(country_raw, '')
            if not country:
                if state_raw.lower() in _US_STATES:
                    country = 'USA'
                elif state_raw.lower() in _CA_PROVINCES:
                    country = 'CAN'
                elif re.search(r'\b[A-Z]{1,2}\d', row['full_address'], re.I):
                    country = 'GB'

            records.append({
                'ID': row['ID'],
                'full_address': row['full_address'],
                'house_number': d.get('house_number') or d.get('StreetNumber'),
                'road': d.get('road') or d.get('StreetName'),
                'city': d.get('city') or d.get('Municipality'),
                'state': state_raw,
                'postcode': d.get('postcode') or d.get('PostalCode'),
                'country': country,
                'filename': filename,
                'processed_timestamp': timestamp,
            })

        result_df = pd.DataFrame(records)

        os.makedirs(processed_dir, exist_ok=True)
        processed_file = Path(processed_dir) / filename
        result_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} rows -> {processed_file}")
        return result_df, str(processed_file)
