from sqlalchemy import Column, String, Numeric, TIMESTAMP, func, Double, SmallInteger, Date, TEXT, BOOLEAN, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from enum import Enum

# Define MetaData with schema
metadata = MetaData(schema="ref")
Base = declarative_base(metadata=metadata)

#============================ Checker Status ============================
class CompanyMain(Base):
    __tablename__ = "company_main"
    
    company_id = Column(String(10), primary_key=True, index=True)
    company_name = Column(String(225))  # TEXT in SQL, so String here
    parent_company_id = Column(String(10))
    address = Column(String)