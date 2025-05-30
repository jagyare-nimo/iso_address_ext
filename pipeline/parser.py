import os
import re
import datetime
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

_COUNTRY_MAP = {
    'us': 'USA', 'usa': 'US', 'united states': 'US', 'united states of america': 'US',
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

        # ensure deepparse cache dir exists
        cache_dir = Path.home() / ".cache" / "deepparse"
        cache_dir.mkdir(parents=True, exist_ok=True)
        try:
            AddressParser(offline=False)
        except (SSLError, RequestsSSLError, FileNotFoundError):
            pass

        self._parser = AddressParser(offline=True)
        self.workers = workers or os.cpu_count()

    @staticmethod
    def _parse_batch(batch):
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(i, pa.to_dict()) for i, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str):
        # setup logger
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        # read extracted input
        df = pd.read_excel(extracted_path, engine="openpyxl")

        # helper to clean NaN/None → ""
        def clean(val):
            if pd.isna(val) or val is None:
                return ""
            return str(val).strip()

        # build full_address
        def join_lines(row):
            parts = [
                clean(row.get("ADDRESSLINE1")),
                clean(row.get("ADDRESSLINE2")),
                clean(row.get("ADDRESSLINE3")),
            ]
            return ", ".join(p for p in parts if p)

        df["full_address"] = df.apply(join_lines, axis=1)

        # ——— drop any rows where full_address is empty ———
        mask = df["full_address"].str.strip().astype(bool)
        df = df.loc[mask]
        if df.empty:
            logger.warning("No non‐empty addresses to parse; returning empty DataFrame")
            return pd.DataFrame(), ""

        # prepare batching of non‐empty addresses
        idxs = df.index.tolist()
        texts = df["full_address"].tolist()
        batches = []
        chunk = 5000
        for i in range(0, len(texts), chunk):
            batches.append((texts[i:i+chunk], idxs[i:i+chunk]))

        # parallel parse
        parsed_map = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for idx, pdict in fut.result():
                    parsed_map[idx] = pdict

        # assemble output records
        records = []
        ts = datetime.datetime.utcnow().isoformat() + "Z"
        stem = Path(extracted_path).stem
        suffix = Path(extracted_path).suffix
        safe_ts = ts.replace("-", "").replace(":", "")
        filename_ts = f"{stem}_{safe_ts}".upper()

        for i, row in df.iterrows():
            d = parsed_map.get(i, {})

            # compute status
            parts = [
                clean(row.get("ADDRESSLINE1")),
                clean(row.get("ADDRESSLINE2")),
                clean(row.get("ADDRESSLINE3")),
            ]
            status = "PERFECT" if all(parts) else "PARTIAL"

            # extract parsed fields
            house_number = clean(d.get("house_number") or d.get("StreetNumber"))
            road = clean(d.get("road") or d.get("StreetName"))
            city = clean(d.get("city") or d.get("Municipality"))
            state = clean(d.get("state") or d.get("Province"))
            postcode = clean(d.get("postcode") or d.get("PostalCode"))

            # normalize country
            c_raw = (d.get("country") or d.get("Country") or "").strip().lower()
            country = _COUNTRY_MAP.get(c_raw, "")
            if not country and row.get("ADDRESSLINE3"):
                country = _COUNTRY_MAP.get(clean(row["ADDRESSLINE3"]).lower(), "")
            sg = state.lower()
            if not country and sg in _US_STATES:
                country = "US"
            if not country and sg in _CA_PROVINCES:
                country = "CA"
            if not country and _UK_PC.search(row["full_address"]):
                country = "GB"

            records.append({
                "ID": clean(row.get("ID")),
                "full_address": row["full_address"].upper(),
                "house_number": house_number.upper(),
                "road": road.upper(),
                "city": city.upper(),
                "state": state.upper(),
                "postcode": postcode.upper(),
                "country": country.upper(),
                "filename": filename_ts,
                "processed_timestamp": ts.upper(),
                "extracted_by": self.extracted_by,
                "status": status.upper(),
            })

        out_df = pd.DataFrame(records)

        # write to Excel
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        processed_file = str(Path(processed_dir) / f"{stem}_{safe_ts}{suffix}")
        out_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file