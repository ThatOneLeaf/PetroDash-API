from sqlalchemy import Text,Column, String, Numeric, TIMESTAMP, func, Double, SmallInteger, Date, TEXT, BOOLEAN, Integer
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

class AuditTrail(Base):
    __tablename__ = "audit_trail"

    audit_id = Column(String(26), primary_key=True, index=True)
    account_id = Column(String(26), nullable=False)
    target_table = Column(String(20), nullable=False)
    record_id = Column(String(20), nullable=False)
    action_type = Column(String(10), nullable=False)
    old_value = Column(TEXT, nullable=False)
    new_value = Column(TEXT, nullable=False)
    audit_timestamp = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    description = Column(TEXT, nullable=False)


class Account(Base):
    __tablename__ = "account"

    account_id = Column(String(26), primary_key=True, index=True)  # ULID
    email = Column(String(254), nullable=False)
    password = Column(Text, nullable=False)  # Store hashed password
    account_role = Column(String(3), nullable=False)
    power_plant_id = Column(String(10), nullable=False)
    company_id = Column(String(10), nullable=False)
    account_status = Column(String(10), nullable=False)
    date_created = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    date_updated = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

class UserProfile(Base):
    __tablename__ = "user_profile"

    emp_id = Column(String(20))
    account_id = Column(String(26), primary_key=True, index=True)  # ULID
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    middle_name = Column(String(50))
    suffix = Column(String(5))
    contact_number = Column(String(20))
    address = Column(Text)
    birthdate = Column(Date)
    gender = Column(String(10))
    profile_created = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp())
    profile_updated = Column(TIMESTAMP, nullable=False, server_default=func.current_timestamp(), onupdate=func.current_timestamp())
