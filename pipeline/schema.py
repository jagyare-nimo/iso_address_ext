from sqlalchemy import MetaData, Table, Column, String


def create_iso_address_table(engine):
    metadata = MetaData()
    Table(
        'iso_address', metadata,
        Column('ID', String(36), primary_key=True),
        Column('full_address', String(500)),
        Column('house_number', String(10)),
        Column('road', String(128)),
        Column('city', String(50)),
        Column('state', String(20)),
        Column('postcode', String(20)),
        Column('country', String(6)),
        Column('filename', String(255)),
        Column('processed_timestamp', String(32)),
    )
    metadata.create_all(engine)
