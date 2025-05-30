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

# allow corporate proxies
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass

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

_US_STATES = {s.lower() for s in [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS",
    "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY",
    "NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"
]}
_CA_PROVINCES = {p.lower() for p in [
    "AB","BC","MB","NB","NL","NS","NT","NU","ON","PE","QC","SK","YT"
]}
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

        # ensure cache for deepparse
        cache_dir = Path.home() / ".cache" / "deepparse"
        cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            AddressParser(offline=False)
        except (SSLError, RequestsSSLError, FileNotFoundError):
            pass
        except Exception:
            pass

        self._parser = AddressParser(offline=True)
        self.workers = workers or os.cpu_count()

    @staticmethod
    def _parse_batch(batch):
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        df = pd.read_excel(extracted_path, engine='openpyxl')

        # helper to normalize None/NaN → ""
        def norm(v):
            return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)

        # 1) build full_address without commas
        def join_lines(r):
            parts = [
                norm(r.get('ADDRESSLINE1')),
                norm(r.get('ADDRESSLINE2')),
                norm(r.get('ADDRESSLINE3')),
            ]
            return " ".join(p.strip() for p in parts if p.strip())

        df['full_address'] = df.apply(join_lines, axis=1)

        # 2) batch for parallel parse
        texts = df['full_address'].tolist()
        idxs = list(df.index)
        chunk = 5000
        batches = [(texts[i:i+chunk], idxs[i:i+chunk])
                   for i in range(0, len(texts), chunk)]

        all_dicts = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for idx, pdict in fut.result():
                    all_dicts[idx] = pdict

        # 3) assemble
        records = []
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        stem, suffix = Path(extracted_path).stem, Path(extracted_path).suffix
        safe_ts = ts.replace('-', '').replace(':', '')
        filename_ts = f"{stem}_{safe_ts}".upper()

        for i, row in df.iterrows():
            d = all_dicts.get(i, {})

            # determine status
            id_val = norm(row.get('ID'))
            lines = [norm(row.get(c)) for c in ('ADDRESSLINE1','ADDRESSLINE2','ADDRESSLINE3')]
            if not id_val or all(not L.strip() for L in lines):
                status = 'INVALID'
            elif all(L.strip() for L in lines):
                status = 'PERFECT'
            else:
                status = 'PARTIAL'

            # take parsed values (already cleaned of None/NaN)
            house = norm(d.get('house_number') or d.get('StreetNumber'))
            road  = norm(d.get('road')         or d.get('StreetName'))
            city  = norm(d.get('city')         or d.get('Municipality'))
            state = norm(d.get('state')        or d.get('Province'))
            pcode = norm(d.get('postcode')     or d.get('PostalCode'))

            # map country
            raw_ctry = (d.get('country') or d.get('Country') or "").strip().lower()
            country = _COUNTRY_MAP.get(raw_ctry, "")
            if not country and row.get('ADDRESSLINE3'):
                country = _COUNTRY_MAP.get(norm(row['ADDRESSLINE3']).strip().lower(), "")
            sg = state.strip().lower()
            if not country and sg in _US_STATES:
                country = 'US'
            if not country and sg in _CA_PROVINCES:
                country = 'CA'
            if not country and _UK_PC.search(row['full_address']):
                country = 'GB'

            # smart override from ADDRESSLINE2 tokens
            raw2 = norm(row.get('ADDRESSLINE2')).strip()
            tokens = raw2.split()
            city_o = state_o = pcode_o = None
            for ti, tok in enumerate(tokens):
                lo = tok.lower()
                if lo in _US_STATES or lo in _CA_PROVINCES:
                    city_o = " ".join(tokens[:ti])
                    state_o = tok
                    pcode_o = " ".join(tokens[ti+1:])
                    break
            if city_o:
                city, state, pcode = city_o, state_o or state, pcode_o or pcode

            # fallback from ADDRESSLINE3 only if needed
            raw3 = norm(row.get('ADDRESSLINE3')).strip()
            if (not state or not pcode) and raw3:
                parts3 = raw3.split(None,1)
                if parts3:
                    if not state:
                        state = parts3[0]
                    if not pcode and len(parts3)>1:
                        pcode = parts3[1]

            # uppercase everything now
            rec = {
                'ID':                   id_val.upper(),
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

        # 4) write processed Excel
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        processed_file = str(Path(processed_dir) / f"{stem}_{safe_ts}{suffix}")
        out_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file