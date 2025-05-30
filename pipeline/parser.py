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

# Fix for Windows corporate environments where bpemb.ckpt may not download correctly
import deepparse.weights_tools as _wt
_orig_handle = _wt.handle_weights_upload


def _fixed_handle_weights_upload(path_to_model_to_upload, device="cpu"):
    try:
        return _orig_handle(path_to_model_to_upload, device)
    except FileNotFoundError as e:
        msg = str(e)
        if "AWS S3 URI" in msg:
            raise FileNotFoundError(f"The file {path_to_model_to_upload} was not found.") from e
        raise


_wt.handle_weights_upload = _fixed_handle_weights_upload

from deepparse.parser import AddressParser
from urllib3.exceptions import SSLError
from requests.exceptions import SSLError as RequestsSSLError

warnings.filterwarnings("ignore", category=UserWarning)

# Allow unverified HTTPS (for corporate SSL proxies)
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

# Map various country inputs to ISO codes
_COUNTRY_MAP = {
    'us': 'USA', 'usa': 'USA', 'united states': 'US', 'united states of america': 'US',
    'ca': 'CAN', 'canada': 'CA',
    'uk': 'GB', 'gb': 'GB', 'great britain': 'GB', 'united kingdom': 'GB',
    'de': 'DE', 'germany': 'DE',
    'fr': 'FR', 'france': 'FR',
    'ch': 'CH', 'switzerland': 'CH',
    'bm': 'BM', 'bermuda': 'BM',
    'gt': 'GT', 'guatemala': 'GT',
    'il': 'IL', 'israel': 'IL',
    'ky': 'KY', 'cayman islands': 'KY',
    'no': 'NO', 'norway': 'NO',
    'pa': 'PA', 'panama': 'PA',
}

# Helpers for fallback by state/province
_US_STATES = {s.lower() for s in [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]}

_CA_PROVINCES = {p.lower() for p in [
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"
]}

# UK postcode heuristic
_UK_PC = re.compile(r'\b[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}\b', re.I)


class AddressParserService:
    """
    Parallel address parsing with ISO country normalization,
    status column, fallback to raw ADDRESSLINE2/3,
    offline-model caching (Windows & Mac), uppercase outputs.
    """

    def __init__(self, workers: int = None, extracted_by: str = None):
        # Who extracted (uppercase)
        if extracted_by:
            self.extracted_by = extracted_by.upper()
        else:
            try:
                self.extracted_by = socket.gethostname().upper()
            except Exception:
                self.extracted_by = getpass.getuser().upper()

        # Ensure deepparse cache dir exists (fixes Windows missing bpemb.ckpt)
        cache_dir = Path.home() / ".cache" / "deepparse"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Force an online download of bpemb.ckpt if needed
        try:
            AddressParser(offline=False)
        except (SSLError, RequestsSSLError, FileNotFoundError):
            pass
        except Exception:
            pass

        # Create offline parser instance
        self._parser = AddressParser(offline=True)
        self.workers = workers or os.cpu_count()

    @staticmethod
    def _parse_batch(batch):
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        # Logger
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        # 1) Load extracted Excel
        df = pd.read_excel(extracted_path, engine='openpyxl')

        # 2) Build 'full_address' by joining non-empty lines
        def join_lines(r):
            parts = [
                str(r.get('ADDRESSLINE1','')).strip(),
                str(r.get('ADDRESSLINE2','')).strip(),
                str(r.get('ADDRESSLINE3','')).strip()
            ]
            return ', '.join(p for p in parts if p)

        df['full_address'] = df.apply(join_lines, axis=1)

        # 3) Batch up for multithreading
        texts = df['full_address'].tolist()
        idxs  = list(df.index)
        chunk = 5000
        batches = [(texts[i:i+chunk], idxs[i:i+chunk])
                   for i in range(0, len(texts), chunk)]

        # 4) Parse in parallel
        all_dicts = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for idx, pdict in fut.result():
                    all_dicts[idx] = pdict

        # 5) Assemble records with fallback for city/state/postcode
        records = []
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        stem   = Path(extracted_path).stem
        suffix = Path(extracted_path).suffix
        safe_ts    = ts.replace('-', '').replace(':', '')
        filename_ts = f"{stem}_{safe_ts}".upper()

        for i, row in df.iterrows():
            d = all_dicts.get(i, {})

            # Status logic
            id_val = row.get('ID')
            lines  = [
                row.get('ADDRESSLINE1',''),
                row.get('ADDRESSLINE2',''),
                row.get('ADDRESSLINE3','')
            ]
            if pd.isna(id_val) or not str(id_val).strip() \
               or all(not str(x).strip() for x in lines):
                status = 'INVALID'
            elif all(str(x).strip() for x in lines):
                status = 'PERFECT'
            else:
                status = 'PARTIAL'

            # Deepparse outputs
            house = d.get('house_number') or d.get('StreetNumber') or ''
            road  = d.get('road')         or d.get('StreetName')   or ''
            city  = d.get('city')         or d.get('Municipality') or ''
            state = (d.get('state') or d.get('Province') or '').strip()
            pcode = d.get('postcode')     or d.get('PostalCode')   or ''

            # Country normalization
            country_raw = (d.get('country') or d.get('Country') or '').strip().lower()
            country = _COUNTRY_MAP.get(country_raw, '')
            if not country and row.get('ADDRESSLINE3'):
                country = _COUNTRY_MAP.get(str(row['ADDRESSLINE3']).strip().lower(), '')
            if not country:
                sg = state.lower()
                if sg in _US_STATES:
                    country = 'US'
                elif sg in _CA_PROVINCES:
                    country = 'CA'
            if not country and _UK_PC.search(row['full_address']):
                country = 'GB'

            # FALLBACK: override city/state/postcode from raw lines
            raw2 = str(row.get('ADDRESSLINE2','')).strip()
            raw3 = str(row.get('ADDRESSLINE3','')).strip()

            if not city and raw2:
                if ',' in raw2:
                    city = raw2.split(',',1)[0].strip()
                else:
                    city = raw2.rsplit(' ', 2)[0].strip()

            if (not state or not pcode) and raw3:
                parts = raw3.split(None,1)
                if parts:
                    state = parts[0]
                    if len(parts) > 1:
                        pcode = parts[1]

            # Build uppercase record
            rec = {
                'ID':                   str(id_val or '').upper(),
                'full_address':        row['full_address'].upper(),
                'house_number':        house.upper(),
                'road':                road.upper(),
                'city':                city.upper(),
                'state':               state.upper(),
                'postcode':            pcode.upper(),
                'country':             country.upper(),
                'filename':            filename_ts,
                'processed_timestamp': ts.upper(),
                'extracted_by':        self.extracted_by,
                'status':              status.upper(),
            }
            records.append(rec)

        out_df = pd.DataFrame(records)

        # 6) Write processed Excel
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        processed_file = str(Path(processed_dir) / f"{stem}_{safe_ts}{suffix}")
        out_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file