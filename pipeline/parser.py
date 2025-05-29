import re
import datetime
import warnings
import pandas as pd
import socket
import getpass
from deepparse.parser import AddressParser
from pathlib import Path
from prefect import get_run_logger

warnings.filterwarnings("ignore", category=UserWarning)

_COUNTRY_MAP = {
    'us': 'USA', 'usa': 'USA', 'united states': 'US',
    'ca': 'CAN', 'canada': 'CA',
    'uk': 'GB', 'great britain': 'GB', 'united kingdom': 'GB',
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
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]}

_CA_PROVINCES = {p.lower() for p in [
    "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"
]}

_UK_PC = re.compile(r'\b[A-Z]{1,2}\d{1,2}\s*\d[A-Z]{2}\b', re.I)


class AddressParserService:
    """
    Wraps Deepparse AddressParser for batch address parsing,
    emits a status column and normalizes ISO country codes.
    """

    def __init__(self, workers: int = None, extracted_by: str = None):
        if extracted_by:
            self.extracted_by = extracted_by
        else:
            try:
                self.extracted_by = socket.gethostname()
            except Exception:
                self.extracted_by = getpass.getuser()

        self._parser = AddressParser()

    def parse_file(self, extracted_path: str, processed_dir: str) -> tuple[pd.DataFrame, str]:
        df = pd.read_excel(extracted_path, engine='openpyxl')

        def join_lines(r):
            parts = [
                str(r.get('ADDRESSLINE1', '')).strip(),
                str(r.get('ADDRESSLINE2', '')).strip(),
                str(r.get('ADDRESSLINE3', '')).strip()
            ]
            return ', '.join(p for p in parts if p)

        df['full_address'] = df.apply(join_lines, axis=1)

        parsed_objs = self._parser(list(df['full_address']))
        parsed_map = {i: obj.to_dict() for i, obj in enumerate(parsed_objs)}

        records = []
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        orig = Path(extracted_path).name

        for i, row in df.iterrows():
            d = parsed_map.get(i, {})

            id_val = row.get('ID')
            lines = [
                row.get('ADDRESSLINE1', ''),
                row.get('ADDRESSLINE2', ''),
                row.get('ADDRESSLINE3', '')
            ]
            if pd.isna(id_val) or not str(id_val).strip() \
                    or all(not str(x).strip() for x in lines):
                status = 'INVALID'
            elif all(str(x).strip() for x in lines):
                status = 'PERFECT'
            else:
                status = 'PARTIAL'

            state_raw = (d.get('state') or d.get('Province') or '').strip()
            country_raw = (d.get('country') or d.get('Country') or '').strip().lower()

            country = _COUNTRY_MAP.get(country_raw, '')
            if not country and row.get('ADDRESSLINE3'):
                country = _COUNTRY_MAP.get(str(row['ADDRESSLINE3']).strip().lower(), '')
            if not country:
                if state_raw.lower() in _US_STATES:
                    country = 'US'
                elif state_raw.lower() in _CA_PROVINCES:
                    country = 'CA'
            if not country and _UK_PC.search(row['full_address']):
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
                'status': status,
            })

        result_df = pd.DataFrame(records)

        Path(processed_dir).mkdir(parents=True, exist_ok=True)
        stem, ext = Path(orig).stem, Path(orig).suffix
        safe_ts = ts.replace('-', '').replace(':', '')
        out_name = f"{stem}_{safe_ts}{ext}"
        processed_file = str(Path(processed_dir) / out_name)
        result_df.to_excel(processed_file, index=False)

        try:
            logger = get_run_logger()
            logger.info(f"Parsed {len(result_df)} addresses → {processed_file}")
        except Exception:
            pass

        return result_df, processed_file
