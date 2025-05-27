from sqlalchemy import Column, String, Numeric, TIMESTAMP, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData

# Define MetaData with schema
metadata = MetaData(schema="bronze")
Base = declarative_base(metadata=metadata)

class EnergyRecords(Base):
    __tablename__ = "csv_energy_records"
    
    energy_id = Column(String(20), primary_key=True, index=True)
    power_plant_id = Column(String(10), index=True)
    datetime = Column(String)  # TEXT in SQL, so String here
    energy_generated = Column(Numeric)
    unit_of_measurement = Column(String(10))
    create_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
