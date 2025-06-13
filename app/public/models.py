from sqlalchemy import Column, String, Numeric, TIMESTAMP, func, Double, SmallInteger, Date, TEXT, BOOLEAN, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from enum import Enum

# Define MetaData with schema
metadata = MetaData(schema="public")
Base = declarative_base(metadata=metadata)

#============================ Checker Status ============================
class RecordStatus(Base):
    __tablename__ = "record_status"
    
    cs_id = Column(String(20), primary_key=True, index=True)
    record_id = Column(String(20))  # TEXT in SQL, so String here
    status_id = Column(String(3))
    status_timestamp = Column(TIMESTAMP, server_default=func.current_timestamp())
    remarks = Column(String)