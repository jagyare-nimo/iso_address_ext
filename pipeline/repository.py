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
        # FIX: Ensure 'ID' column exists in the DataFrame before renaming.
        # This directly addresses the KeyError if the incoming data_frame unexpectedly lacks 'ID'.
        if "ID" not in data_frame.columns:
            # Add an 'ID' column with pandas' default NA value if it's missing.
            # This ensures 'ID' is always present for the rename operation.
            data_frame["ID"] = pd.NA

            # Rename 'ID' (from AddressParserService output) to 'id' (for schema compliance)
        df = data_frame.rename(columns={"ID": "id"})

        # Extract unique, non-null IDs. Converting to str as per schema.
        # This handles cases where 'id' might be all pd.NA or empty strings.
        ids = [str(i) for i in df['id'].dropna().unique()]

        if ids:
            # FIX: Use specific bind parameters for the IN clause for better robustness
            # and compatibility with various database drivers, as opposed to `expanding=True`.
            placeholders = ", ".join([f":id_{i}" for i in range(len(ids))])
            delete_stmt = text(f'DELETE FROM "{self.table_name}" WHERE id IN ({placeholders})')

            # Create a dictionary of parameters where keys match the placeholders
            bind_params = {f"id_{i}": id_val for i, id_val in enumerate(ids)}

            with self.engine.begin() as conn:
                conn.execute(delete_stmt, bind_params)

        # Save the DataFrame to the SQL table
        df.to_sql(
            name=self.table_name,
            con=self.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=batch_size
        )