from sqlalchemy import Column, String, Numeric, TIMESTAMP, func, Double, SmallInteger, Date, TEXT, BOOLEAN, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData
from enum import Enum

# Define MetaData with schema
metadata = MetaData(schema="bronze")
Base = declarative_base(metadata=metadata)
#============================POWER PLANT ENERGY============================
class EnergyRecords(Base):
    __tablename__ = "csv_energy_records"
    
    energy_id = Column(String(20), primary_key=True, index=True)
    power_plant_id = Column(String(10), index=True)
    datetime = Column(String)  # TEXT in SQL, so String here
    energy_generated = Column(Numeric)
    unit_of_measurement = Column(String(10))
    create_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

#============================CSR/HELP============================
class CSRActivity(Base):
    __tablename__ = "csr_activity"

    csr_id = Column(String(15), primary_key=True, index=True)
    company_id = Column(String(20), index=True)
    project_id = Column(String(20), index=True)
    project_year = Column(SmallInteger)
    csr_report = Column(Numeric)
    project_expenses = Column(Numeric)

class CSRProject(Base):
    __tablename__ = "csr_project"

    project_id = Column(String(20), primary_key=True, index=True)
    program_id = Column(String(20), index=True)
    project_name = Column(String(20))
    project_metrics = Column(String(50))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

class CSRProgram(Base):
    __tablename__ = "csr_program"

    program_id = Column(String(5), primary_key=True, index=True)
    program_name = Column(String(20))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

#============================ENVIRONMENTAL=================================
class EnviCompanyProperty(Base):
    __tablename__ = "envi_company_property"

    cp_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    cp_name = Column(String(30))
    cp_type = Column(String(15))

class EnviWaterAbstraction(Base):
    __tablename__ = "envi_water_abstraction"

    wa_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    year = Column(SmallInteger)
    month = Column(String(10))
    quarter = Column(String(2))
    volume = Column(Numeric)  # DOUBLE PRECISION
    unit_of_measurement = Column(String(15))

class EnviWaterDischarge(Base):
    __tablename__ = "envi_water_discharge"

    wd_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    year = Column(SmallInteger)
    quarter = Column(String(2))
    volume = Column(Numeric)
    unit_of_measurement = Column(String(15))

class EnviWaterConsumption(Base):
    __tablename__ = "envi_water_consumption"

    wc_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    year = Column(SmallInteger)
    quarter = Column(String(2))
    volume = Column(Numeric)
    unit_of_measurement = Column(String(15))

class EnviDieselConsumption(Base):
    __tablename__ = "envi_diesel_consumption"

    dc_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    cp_id = Column(String(20), index=True)
    unit_of_measurement = Column(String(15))
    consumption = Column(Numeric)
    date = Column(Date)

class EnviElectricConsumption(Base):
    __tablename__ = "envi_electric_consumption"

    ec_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    source = Column(String(20))
    unit_of_measurement = Column(String(15))
    consumption = Column(Numeric)
    quarter = Column(String(2))
    year = Column(SmallInteger)

class EnviNonHazardWaste(Base):
    __tablename__ = "envi_non_hazard_waste"

    nhw_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    metrics = Column(String(50))
    unit_of_measurement = Column(String(15))
    waste = Column(Numeric)
    month = Column(String(10))
    quarter = Column(String(2))
    year = Column(SmallInteger)

class EnviHazardWasteGenerated(Base):
    __tablename__ = "envi_hazard_waste_generated"

    hwg_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    metrics = Column(String(50))
    unit_of_measurement = Column(String(15))
    waste_generated = Column(Numeric)
    quarter = Column(String(2))
    year = Column(SmallInteger)

class EnviHazardWasteDisposed(Base):
    __tablename__ = "envi_hazard_waste_disposed"

    hwd_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(10), index=True)
    metrics = Column(String(50))
    unit_of_measurement = Column(String(15))
    waste_disposed = Column(Numeric)
    year = Column(SmallInteger)

# Used for the excel file template generation for ENVI
class TableType(str, Enum):
    COMPANY_PROPERTY = "company_property"
    WATER_ABSTRACTION = "water_abstraction"
    WATER_DISCHARGE = "water_discharge"
    WATER_CONSUMPTION = "water_consumption"
    DIESEL_CONSUMPTION = "diesel_consumption"
    ELECTRIC_CONSUMPTION = "electric_consumption"
    NON_HAZARD_WASTE = "non_hazard_waste"
    HAZARD_WASTE_GENERATED = "hazard_waste_generated"
    HAZARD_WASTE_DISPOSED = "hazard_waste_disposed"
    ALL = "all"
    
#============================HUMAN RESOURCES=================================
metadata_silver = MetaData(schema="silver")
Base_silver = declarative_base(metadata=metadata_silver)
class HRDemographics(Base):
    __tablename__ = "hr_demographics"
    
    employee_id = Column(String(20), primary_key=True, index=True)
    gender = Column(String(1))
    birthdate = Column(TIMESTAMP)
    position_id = Column(String(2))
    p_np = Column(String(2))
    company_id = Column(String(10))
    employment_status = Column(String(20))

class HRTenure(Base):
    __tablename__ = "hr_tenure"
    
    employee_id = Column(String(20), primary_key=True, index=True)
    start_date = Column(TIMESTAMP, primary_key=True, index=True)
    end_date = Column(TIMESTAMP, nullable=True)
    
class HRTraining(Base_silver):
    __tablename__ = "hr_training"
    
    training_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(20), primary_key=True, index=True)
    date = Column(String(50), primary_key=True, index=True)
    training_title = Column(TEXT, primary_key=True, index=True)
    training_hours = Column(TIMESTAMP)
    number_of_participants = Column(Numeric)
    
class HRSafetyWorkdata(Base_silver):
    __tablename__ = "hr_safety_workdata"
    
    safety_workdata_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(20), primary_key=True, index=True)
    contractor = Column(SmallInteger, primary_key=True, index=True)
    date = Column(String(10), primary_key=True, index=True)
    manpower = Column(Integer)
    manhours = Column(Integer)
    
class HRParentalLeave(Base_silver):
    __tablename__ = "hr_parental_leave"
    
    parental_leave_id = Column(String(20), primary_key=True, index=True)
    employee_id = Column(String(20), primary_key=True, index=True)
    type_of_leave = Column(String(50))
    date = Column(TIMESTAMP)
    days = Column(Integer)
    
class HROsh(Base_silver):
    __tablename__ = "hr_occupational_safety_health"
    
    osh_id = Column(String(20), primary_key=True, index=True)
    company_id = Column(String(20), primary_key=True, index=True)
    workforce_type = Column(TEXT, primary_key=True, index=True)
    lost_time = Column(BOOLEAN, primary_key=True, index=True)
    date = Column(TIMESTAMP, primary_key=True, index=True)
    incident_type = Column(TEXT, primary_key=True, index=True)
    incident_title = Column(TEXT, primary_key=True, index=True)
    incident_count = Column(Integer)

# HR Excel file template generation
# class HRTableType(str, Enum):
#     DEMOGRAPHICS = "demographics"
#     TENURE = "tenure"
#     SAFETY_WORKDATA = "safety_workdata"
#     TRAINING = "training"
#     PARENTAL_LEAVE = "parental_leave"
#     OCCUPATIONAL_SAFETY_HEALTH = "occupational_safety_health"
#     ALL = "all"