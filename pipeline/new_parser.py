import os
import re
import ssl
import datetime
import warnings
import requests
import pandas as pd
import socket
import getpass

from pathlib import Path
from prefect import get_run_logger
from concurrent.futures import ThreadPoolExecutor, as_completed

# 1) Disable SSL certificate checks (Windows corporate)
try:
    ssl._create_default_https_context = ssl._create_unverified_context
except AttributeError:
    pass
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# 2) Ensure bpemb.ckpt in cache, auto-download if missing
CACHE_DIR = Path.home() / ".cache" / "deepparse"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
BPEMB_CKPT = CACHE_DIR / "bpemb.ckpt"
if not BPEMB_CKPT.exists():
    BPEMB_URL = "https://graal.ift.ulaval.ca/public/deepparse/bpemb.ckpt"
    try:
        resp = requests.get(BPEMB_URL, verify=False, timeout=120)
        resp.raise_for_status()
        BPEMB_CKPT.write_bytes(resp.content)
        print(f"Downloaded bpemb.ckpt → {BPEMB_CKPT}")
    except Exception as e:
        warnings.warn(f"Could not auto-download bpemb.ckpt ({e}); seq2seq may fail.", UserWarning)

# 3) Import DeepParse AFTER ensuring the checkpoint
from deepparse.parser import AddressParser

# country/state maps & postcode heuristic
_COUNTRY_MAP = {
    'us':'USA','usa':'USA','united states':'US','united states of america':'US',
    'ca':'CAN','canada':'CA',
    'uk':'GB','gb':'GB','great britain':'GB','united kingdom':'GB',
    'de':'DE','germany':'DE',
    'fr':'FR','france':'FR',
    'ch':'CH','switzerland':'CH',
    'bm':'BM','bermuda':'BM',
    'gt':'GT','guatemala':'GT',
    'il':'IL','israel':'IL',
    'ky':'KY','cayman islands':'KY','kentucky':'KY',
    'no':'NO','norway':'NO',
    'pa':'PA','panama':'PA',
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
    """
    Batch, multithreaded address parsing (seq2seq) with:
     - corporate‐SSL override + automatic bpemb.ckpt download
     - raw second-line overriding for city/state/postcode
     - all fields in UPPERCASE
    """

    def __init__(self, workers: int = None, extracted_by: str = None):
        # who extracted?
        if extracted_by:
            self.extracted_by = extracted_by.upper()
        else:
            try:
                self.extracted_by = socket.gethostname().upper()
            except Exception:
                self.extracted_by = getpass.getuser().upper()

        # instantiate seq2seq parser offline
        self._parser = AddressParser(offline=True)
        self.workers = workers or os.cpu_count()

    @staticmethod
    def _parse_batch(batch):
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        # logger
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        # 1) load
        df = pd.read_excel(extracted_path, engine='openpyxl')

        # 2) build full_address
        def join_lines(r):
            parts = [
                str(r.get('ADDRESSLINE1','')).strip(),
                str(r.get('ADDRESSLINE2','')).strip(),
                str(r.get('ADDRESSLINE3','')).strip()
            ]
            return ', '.join(p for p in parts if p)
        df['full_address'] = df.apply(join_lines, axis=1)

        # 3) batch → multithread
        texts = df['full_address'].tolist()
        idxs  = list(df.index)
        chunk = 5000
        batches = [(texts[i:i+chunk], idxs[i:i+chunk])
                   for i in range(0, len(texts), chunk)]

        all_dicts = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for idx, pdict in fut.result():
                    all_dicts[idx] = pdict

        # 4) assemble + override raw2
        records = []
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        stem = Path(extracted_path).stem
        suffix = Path(extracted_path).suffix
        safe_ts = ts.replace('-','').replace(':','')
        filename_ts = f"{stem}_{safe_ts}".upper()

        for i, row in df.iterrows():
            d = all_dicts.get(i, {})
            # status
            id_val = row.get('ID')
            lines  = [row.get('ADDRESSLINE1',''),
                      row.get('ADDRESSLINE2',''),
                      row.get('ADDRESSLINE3','')]
            if pd.isna(id_val) or not str(id_val).strip() or all(not str(x).strip() for x in lines):
                status = 'INVALID'
            elif all(str(x).strip() for x in lines):
                status = 'PERFECT'
            else:
                status = 'PARTIAL'

            # country fallback
            country_raw = (d.get('country') or d.get('Country') or '').strip().lower()
            country = _COUNTRY_MAP.get(country_raw, '')
            if not country and row.get('ADDRESSLINE3'):
                country = _COUNTRY_MAP.get(str(row['ADDRESSLINE3']).strip().lower(), '')
            state_guess = (d.get('state') or d.get('Province') or '').strip().lower()
            if not country and state_guess in _US_STATES:
                country = 'USA'
            if not country and state_guess in _CA_PROVINCES:
                country = 'CAN'
            if not country and _UK_PC.search(row['full_address']):
                country = 'GB'

            # override from raw2
            raw2 = str(row.get('ADDRESSLINE2','')).strip()
            if ',' in raw2:
                city_p, rest = [p.strip() for p in raw2.split(',',1)]
                parts = rest.split(None,1)
                state_p = parts[0] if parts else ''
                pcode_p = parts[1] if len(parts)>1 else ''
            else:
                parts = raw2.rsplit(None,1)
                if len(parts)==2:
                    city_p, pcode_p = parts
                    state_p = ''
                else:
                    city_p, state_p, pcode_p = raw2, '', ''

            rec = {
                'ID':                str(id_val).upper(),
                'full_address':      row['full_address'].upper(),
                'house_number':      (d.get('house_number') or d.get('StreetNumber') or '').upper(),
                'road':              (d.get('road') or d.get('StreetName') or '').upper(),
                'city':              city_p.upper(),
                'state':             state_p.upper(),
                'postcode':          pcode_p.upper(),
                'country':           country.upper(),
                'filename':          filename_ts,
                'processed_timestamp': ts.upper(),
                'extracted_by':      self.extracted_by,
                'status':            status,
            }
            records.append(rec)

        out_df = pd.DataFrame(records)

        # 5) write
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        processed_file = str(Path(processed_dir) / f"{stem}_{safe_ts}{suffix}")
        out_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file