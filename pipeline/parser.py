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

# ISO-code mapping
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
    'ky': 'KY', 'cayman islands': 'KY', 'kentucky': 'KY',
    'no': 'NO', 'norway': 'NO',
    'pa': 'PA', 'panama': 'PA',
}

# Fallback by US state / CA province
_US_STATES = {s.lower() for s in [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]}
_CA_PROVINCES = {p.lower() for p in [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
]}

# Heuristic to detect UK postcodes
_UK_PC = re.compile(r'\b[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}\b', re.I)


class AddressParserService:
    """
    Parallel address parsing with ISO‐normalization, status ("PERFECT"/"PARTIAL"/"INVALID"),
    and a short‐circuit to never feed blank addresses into DeepParse.
    """

    def __init__(self, workers: int = None, extracted_by: str = None):
        # who ran it?
        if extracted_by:
            self.extracted_by = extracted_by.upper()
        else:
            try:
                self.extracted_by = socket.gethostname().upper()
            except Exception:
                self.extracted_by = getpass.getuser().upper()

        # ensure model cache dir (fixes Windows corporate SSL issues)
        cache_dir = Path.home() / ".cache" / "deepparse"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # force an online download of bpemb.ckpt if needed
        try:
            AddressParser(offline=False)
        except (SSLError, RequestsSSLError, FileNotFoundError):
            pass

        # now safe to build offline parser
        self._parser = AddressParser(offline=True)
        self.workers = workers or os.cpu_count()

    @staticmethod
    def _parse_batch(batch: tuple[list[str], list[int]]) -> list[tuple[int, dict]]:
        """Helper for ThreadPoolExecutor: parse a batch of texts."""
        parser = AddressParser(offline=True)
        texts, idxs = batch
        parsed = parser(texts)
        return [(idx, pa.to_dict()) for idx, pa in zip(idxs, parsed)]

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        # load logger
        try:
            logger = get_run_logger()
        except Exception:
            import logging
            logger = logging.getLogger(__name__)

        # 1) read & join lines into full_address
        df = pd.read_excel(extracted_path, engine="openpyxl")

        def join_lines(r):
            parts = [
                str(r.get("ADDRESSLINE1", "")).strip(),
                str(r.get("ADDRESSLINE2", "")).strip(),
                str(r.get("ADDRESSLINE3", "")).strip(),
            ]
            return ", ".join(p for p in parts if p)

        df["full_address"] = df.apply(join_lines, axis=1)

        # 2) split into “to-parse” vs “empty”
        non_empty_mask = df["full_address"].str.strip().astype(bool)
        non_empty_idxs = df.index[non_empty_mask].tolist()

        # 3) batch & parse only non-empty
        batches: list[tuple[list[str], list[int]]] = []
        texts = df.loc[non_empty_idxs, "full_address"].tolist()
        chunk = 5000
        for i in range(0, len(texts), chunk):
            batches.append((texts[i: i + chunk], non_empty_idxs[i: i + chunk]))

        parsed_map: dict[int, dict] = {}
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = [pool.submit(self._parse_batch, b) for b in batches]
            for fut in as_completed(futures):
                for idx, pdict in fut.result():
                    parsed_map[idx] = pdict

        # 4) assemble final records (including invalid/empty)
        records = []
        ts = datetime.datetime.utcnow().isoformat() + "Z"
        stem = Path(extracted_path).stem
        suffix = Path(extracted_path).suffix
        safe_ts = ts.replace("-", "").replace(":", "")
        filename_ts = f"{stem}_{safe_ts}".upper()

        for i, row in df.iterrows():
            full_addr = row["full_address"]
            # INVALID if blank
            if not str(full_addr).strip():
                status = "INVALID"
                parsed = {}
            else:
                # PERFECT vs PARTIAL
                lines = [
                    row.get("ADDRESSLINE1", ""),
                    row.get("ADDRESSLINE2", ""),
                    row.get("ADDRESSLINE3", ""),
                ]
                if all(str(x).strip() for x in lines):
                    status = "PERFECT"
                else:
                    status = "PARTIAL"
                parsed = parsed_map.get(i, {})

            # pull fields from parsed (or "" if missing)
            hn = (parsed.get("house_number") or parsed.get("StreetNumber") or "")
            rd = (parsed.get("road") or parsed.get("StreetName") or "")
            city = (parsed.get("city") or parsed.get("Municipality") or "")
            st = (parsed.get("state") or parsed.get("Province") or "")
            pcode = (parsed.get("postcode") or parsed.get("PostalCode") or "")

            # normalize country
            c_raw = (parsed.get("country") or parsed.get("Country") or "").strip().lower()
            ctry = _COUNTRY_MAP.get(c_raw, "")
            if not ctry and row.get("ADDRESSLINE3"):
                ctry = _COUNTRY_MAP.get(str(row["ADDRESSLINE3"]).strip().lower(), "")
            sg = st.strip().lower()
            if not ctry and sg in _US_STATES:
                ctry = "US"
            if not ctry and sg in _CA_PROVINCES:
                ctry = "CA"
            if not ctry and _UK_PC.search(full_addr):
                ctry = "GB"

            # build final uppercase record
            records.append({
                "ID": str(row.get("ID", "")).upper(),
                "full_address": full_addr.upper(),
                "house_number": hn.upper(),
                "road": rd.upper(),
                "city": city.upper(),
                "state": st.upper(),
                "postcode": pcode.upper(),
                "country": ctry.upper(),
                "filename": filename_ts,
                "processed_timestamp": ts.upper(),
                "extracted_by": self.extracted_by,
                "status": status,
            })

        out_df = pd.DataFrame(records)

        # 5) write Excel & return
        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        out_name = f"{stem}_{safe_ts}{suffix}"
        processed_file = str(Path(processed_dir) / out_name)
        out_df.to_excel(processed_file, index=False)

        logger.info(f"Parsed {len(records)} addresses → {processed_file}")
        return out_df, processed_file
