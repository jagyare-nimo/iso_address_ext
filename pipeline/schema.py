from sqlalchemy import MetaData, Table, Column, Integer, String


def create_iso_address_table(engine):
    metadata = MetaData()
    Table(
        'iso_address', metadata,
        Column('record_id', Integer, primary_key=True, autoincrement=True),
        Column('id', String(36)),
        Column('full_address', String(500)),
        Column('house_number', String(10)),
        Column('road', String(128)),
        Column('city', String(50)),
        Column('state', String(20)),
        Column('postcode', String(20)),
        Column('country', String(6)),
        Column('filename', String(255)),
        Column('processed_timestamp', String(32)),
        Column('extracted_by', String(50)),
        Column('status', String(16), nullable=False),
    )
    metadata.create_all(engine)
