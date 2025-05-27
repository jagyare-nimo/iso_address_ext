import jaydebeapi
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


class DatabaseRepository:

    def __init__(
            self,
            table_name: str,
            datasource_url: str,
            driver_class: str,
            username: str,
            password: str,
            jar_path: str = None
    ):
        project_root = Path(__file__).resolve().parents[1]
        jar = Path(jar_path) if jar_path else project_root / 'resources' / 'data' / 'h2-2.3.232.jar'
        jar = str(jar)

        prefix = 'jdbc:h2:file:'
        raw = datasource_url[len(prefix):] if datasource_url.startswith(prefix) else datasource_url
        parts = raw.split(';', 1)
        file_part = parts[0]
        params = ';' + parts[1] if len(parts) > 1 else ''
        db_file = project_root / file_part
        url = f'jdbc:h2:file:{db_file}{params}'

        conn = jaydebeapi.connect(driver_class, url, [username, password] if username or password else None, jar)
        self.conn = conn

        try:
            self.cursor = conn.cursor()
        except TypeError:
            self.cursor = conn.cursor

        self.table_name = table_name
        self._init_table()

    def _init_table(self):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            ID VARCHAR(36) PRIMARY KEY,
            full_address VARCHAR(1024),
            house_number VARCHAR(64),
            road VARCHAR(255),
            city VARCHAR(128),
            state VARCHAR(128),
            postcode VARCHAR(32),
            country VARCHAR(64),
            filename VARCHAR(255),
            processed_timestamp VARCHAR(32)
        )
        """
        self.cursor.execute(ddl)
        self.conn.commit()

    def save(self, df: pd.DataFrame, batch_size: int = 1000):

        ids = df['ID'].fillna('').tolist()
        if ids:
            placeholders = ','.join('?' for _ in ids)
            delete_sql = f"DELETE FROM {self.table_name} WHERE ID IN ({placeholders})"
            self.cursor.execute(delete_sql, ids)

        insert_sql = (
            f"INSERT INTO {self.table_name} "
            "(ID, full_address, house_number, road, city, state, postcode, country, filename, processed_timestamp) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        def _insert_chunk(chunk_df):
            data = [
                (
                    row['ID'],
                    row['full_address'] or '',
                    row.get('house_number') or '',
                    row.get('road') or '',
                    row.get('city') or '',
                    row.get('state') or '',
                    row.get('postcode') or '',
                    row.get('country') or '',
                    row['filename'],
                    row['processed_timestamp']
                )
                for _, row in chunk_df.iterrows()
            ]
            if data:
                self.cursor.executemany(insert_sql, data)

        chunks = [
            df.iloc[i:i + batch_size]
            for i in range(0, len(df), batch_size)
        ]
        with ThreadPoolExecutor(max_workers=4) as pool:
            pool.map(_insert_chunk, chunks)

        self.conn.commit()
