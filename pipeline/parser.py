import os
import re
import datetime
import warnings
import pandas as pd
import socket
import getpass
from deepparse.parser import AddressParser
from pathlib import Path
from prefect import get_run_logger
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore", category=UserWarning)

_COUNTRY_MAP = {
    'us': 'USA', 'usa': 'USA', 'united states': 'USA', 'united states of america': 'US',
    'ca': 'CAN', 'can': 'CAN', 'canada': 'CA',
    'uk': 'GB', 'gb': 'GB', 'great britain': 'GB', 'united kingdom': 'GB',
    'de': 'DE', 'ger': 'DE', 'germany': 'DE',
    'fr': 'FR', 'fra': 'FR', 'france': 'FR',
    'ch': 'CH', 'switzerland': 'CH',
    'bm': 'BM', 'bermuda': 'BM',
    'gt': 'GT', 'guatemala': 'GT',
    'il': 'IL', 'israel': 'IL',
    'ky': 'KY', 'cayman islands': 'KY', 'kentucky': 'KY',
    'no': 'NO', 'norway': 'NO',
    'pa': 'PA', 'panama': 'PA',
}

_US_STATES = {s.lower() for s in [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]}

_CA_PROVINCES = {p.lower() for p in [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
]}


class AddressParserService:
    def __init__(self, workers: int = None, extracted_by: str = None):
        self.workers = workers or os.cpu_count()
        if extracted_by:
            self.extracted_by = extracted_by
        else:
            try:
                self.extracted_by = socket.gethostname()
            except Exception:
                self.extracted_by = getpass.getuser()
        # warm the model cache
        try:
            AddressParser()
        except Exception:
            pass

    @staticmethod
    def _parse_batch(batch):
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str):
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        # 1) load original extracted columns
        df = pd.read_excel(extracted_path, engine='openpyxl')

        # 2) build full_address
        def join_lines(r):
            parts = [
                str(r.get('ADDRESSLINE1', '')).strip(),
                str(r.get('ADDRESSLINE2', '')).strip(),
                str(r.get('ADDRESSLINE3', '')).strip()
            ]
            return ', '.join(p for p in parts if p)

        df['full_address'] = df.apply(join_lines, axis=1)

        # 3) batch & parallel parse
        texts = df['full_address'].tolist()
        idxs = list(df.index)
        batch_size = 5_000
        batches = [
            (texts[i:i + batch_size], idxs[i:i + batch_size])
            for i in range(0, len(texts), batch_size)
        ]
        all_dicts = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for i, d in fut.result():
                    all_dicts[i] = d

        # 4) assemble records with status
        records = []
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        orig = Path(extracted_path).name
        uk_pc = re.compile(r'\b[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}\b', re.I)

        for i, row in df.iterrows():
            d = all_dicts.get(i, {})
            id_val = row.get('ID')
            lines = [
                row.get('ADDRESSLINE1', ''),
                row.get('ADDRESSLINE2', ''),
                row.get('ADDRESSLINE3', '')
            ]
            # compute status
            if pd.isna(id_val) or not str(id_val).strip() \
                    or all(not str(x).strip() for x in lines):
                status = 'INVALID'
            elif all(str(x).strip() for x in lines):
                status = 'PERFECT'
            else:
                status = 'PARTIAL'

            state_raw = (d.get('state') or d.get('Province') or '').strip()
            country_raw = (d.get('country') or d.get('Country') or '').strip().lower()

            # map country
            country = _COUNTRY_MAP.get(country_raw, '')
            if not country and row.get('ADDRESSLINE3'):
                country = _COUNTRY_MAP.get(str(row['ADDRESSLINE3']).strip().lower(), '')
            if not country:
                if state_raw.lower() in _US_STATES:
                    country = 'US'
                elif state_raw.lower() in _CA_PROVINCES:
                    country = 'CA'
            if not country and uk_pc.search(row['full_address']):
                country = 'GB'

            records.append({
                'ID': str(id_val),
                'full_address': row['full_address'],
                'house_number': d.get('house_number') or d.get('StreetNumber'),
                'road': d.get('road') or d.get('StreetName'),
                'city': d.get('city') or d.get('Municipality'),
                'state': state_raw,
                'postcode': d.get('postcode') or d.get('PostalCode'),
                'country': country,
                'filename': f"{orig}_{ts}",
                'processed_timestamp': ts,
                'extracted_by': self.extracted_by,
                'status': status,  # ← new
            })

        out_df = pd.DataFrame(records)

        # 5) write Excel with timestamp‐suffix
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        stem, ext = Path(orig).stem, Path(orig).suffix
        safe_ts = ts.replace('-', '').replace(':', '')
        out_name = f"{stem}_{safe_ts}{ext}"
        processed_file = str(Path(processed_dir) / out_name)

        out_df.to_excel(processed_file, index=False)
        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file
