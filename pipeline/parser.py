import os
import re
import datetime
import ssl
import warnings
import pandas as pd
import socket
import getpass
from pathlib import Path
from prefect import get_run_logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from deepparse.parser import AddressParser
from urllib3.exceptions import SSLError
from requests.exceptions import SSLError as RequestsSSLError

warnings.filterwarnings("ignore", category=UserWarning)

try:
    ssl._create_default_https_context = ssl._create_unverified_context
except Exception:
    pass

# Map various country inputs to ISO codes
_COUNTRY_MAP = {
    'us': 'USA', 'usa': 'USA', 'united states': 'USA', 'united states of america': 'US',
    'ca': 'CAN', 'canada': 'CA',
    'uk': 'GB', 'gb': 'GB', 'great britain': 'GB', 'united kingdom': 'GB',
    'de': 'DE', 'germany': 'DE',
    'fr': 'FR', 'france': 'FR',
    'ch': 'CH', 'switzerland': 'CH',
    'bm': 'BM', 'bermuda': 'BM',
    'gt': 'GT', 'guatemala': 'GT',
    'il': 'IL', 'israel': 'IL',
    'ky': 'KY', 'cayman islands': 'KY', 'kentucky': 'KY',
    'no': 'NO', 'norway': 'NO',
    'pa': 'PA', 'panama': 'PA',
}

# Helpers for fallback by state/province
_US_STATES = {s.lower() for s in [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]}

_CA_PROVINCES = {p.lower() for p in [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
]}

# UK postcode heuristic
_UK_PC = re.compile(r'\b[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}\b', re.I)


class AddressParserService:

    def __init__(self, workers: int = None, extracted_by: str = None):
        # who extracted
        if extracted_by:
            self.extracted_by = extracted_by.upper()
        else:
            try:
                self.extracted_by = socket.gethostname().upper()
            except Exception:
                self.extracted_by = getpass.getuser().upper()

        # ensure deepparse cache dir exists (fixes Windows missing bpemb.ckpt)
        cache_dir = Path.home() / ".cache" / "deepparse"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # force an online download of bpemb.ckpt if needed
        try:
            AddressParser(offline=False)
        except (SSLError, RequestsSSLError):
            pass
        except Exception:
            pass

        # create offline parser instance
        self._parser = AddressParser(offline=True)
        self.workers = workers or os.cpu_count()

    @staticmethod
    def _parse_batch(batch):
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        # get logger (Prefect or stdlib)
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        # 1) Load extracted Excel
        df = pd.read_excel(extracted_path, engine='openpyxl')

        # 2) Build 'full_address'
        def join_lines(r):
            parts = [
                str(r.get('ADDRESSLINE1', '')).strip(),
                str(r.get('ADDRESSLINE2', '')).strip(),
                str(r.get('ADDRESSLINE3', '')).strip()
            ]
            return ', '.join(p for p in parts if p)

        df['full_address'] = df.apply(join_lines, axis=1)

        # 3) Batch up for multithreading
        texts = df['full_address'].tolist()
        idxs = list(df.index)
        chunk = 5000
        batches = [
            (texts[i:i + chunk], idxs[i:i + chunk])
            for i in range(0, len(texts), chunk)
        ]

        # 4) Parse in parallel
        all_dicts = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for idx, pdict in fut.result():
                    all_dicts[idx] = pdict

        # 5) Assemble records, *override* city/state/postcode from raw second line
        records = []
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        stem = Path(extracted_path).stem
        suffix = Path(extracted_path).suffix
        safe_ts = ts.replace('-', '').replace(':', '')
        filename_ts = f"{stem}_{safe_ts}".upper()

        for i, row in df.iterrows():
            d = all_dicts.get(i, {})

            # status logic
            id_val = row.get('ID')
            lines = [row.get('ADDRESSLINE1', ''),
                     row.get('ADDRESSLINE2', ''),
                     row.get('ADDRESSLINE3', '')]
            if pd.isna(id_val) or not str(id_val).strip() \
                    or all(not str(x).strip() for x in lines):
                status = 'INVALID'
            elif all(str(x).strip() for x in lines):
                status = 'PERFECT'
            else:
                status = 'PARTIAL'

            # first map country (from parsed or 3rd line)
            country_raw = (d.get('country') or d.get('Country') or '').strip().lower()
            country = _COUNTRY_MAP.get(country_raw, '')
            if not country and row.get('ADDRESSLINE3'):
                country = _COUNTRY_MAP.get(str(row['ADDRESSLINE3']).strip().lower(), '')
            # fallback by state code if still empty
            state_guess = (d.get('state') or d.get('Province') or '').strip().lower()
            if not country and state_guess in _US_STATES:
                country = 'US'
            if not country and state_guess in _CA_PROVINCES:
                country = 'CA'
            # UK‐postcode heuristic
            if not country and _UK_PC.search(row['full_address']):
                country = 'GB'

            # now override city/state/postcode from raw ADDRESSLINE2
            raw2 = str(row.get('ADDRESSLINE2', '')).strip()
            if ',' in raw2:
                city_part, rest = [p.strip() for p in raw2.split(',', 1)]
                parts = rest.split(None, 1)
                state_part = parts[0] if parts else ''
                pcode_part = parts[1] if len(parts) > 1 else ''
            else:
                # e.g. UK/DE/FR = "City PCode"
                parts = raw2.rsplit(None, 1)
                if len(parts) == 2:
                    city_part, pcode_part = parts
                    state_part = ''
                else:
                    city_part = raw2
                    state_part = ''
                    pcode_part = ''

            # uppercase everything
            rec = {
                'ID': str(id_val).upper(),
                'full_address': row['full_address'].upper(),
                'house_number': (d.get('house_number') or d.get('StreetNumber') or '').upper(),
                'road': (d.get('road') or d.get('StreetName') or '').upper(),
                'city': city_part.upper(),
                'state': state_part.upper(),
                'postcode': pcode_part.upper(),
                'country': country.upper(),
                'filename': filename_ts,
                'processed_timestamp': ts.upper(),
                'extracted_by': self.extracted_by,
                'status': status,
            }
            records.append(rec)

        out_df = pd.DataFrame(records)

        # 6) Write processed Excel
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        processed_file = str(Path(processed_dir) / f"{stem}_{safe_ts}{suffix}")
        out_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file
