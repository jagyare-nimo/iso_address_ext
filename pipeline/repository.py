import pandas as pd
from sqlalchemy import create_engine, text, bindparam
from pipeline.schema import create_iso_address_table
from pipeline.config import Config


class DatabaseRepository:
    def __init__(self, config: Config):
        self.engine = create_engine(config.database_url)
        self.table_name = config.table_name
        create_iso_address_table(self.engine)

    def save(self, data_frame: pd.DataFrame, batch_size: int = 1000):
        df = data_frame.rename(columns={"ID": "id"})
        ids = [str(i) for i in df['id'].dropna().unique()]
        if ids:
            delete_stmt = (
                text(f'DELETE FROM "{self.table_name}" WHERE id IN :ids')
                .bindparams(bindparam("ids", expanding=True))
            )
            with self.engine.begin() as conn:
                conn.execute(delete_stmt, {"ids": ids})

        df.to_sql(
            name=self.table_name,
            con=self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=batch_size
        )
