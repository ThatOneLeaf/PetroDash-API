from pydantic import BaseModel, ConfigDict
from datetime import datetime as dt
from typing import Optional
#====================POWER PLANT ENERGY=================================
class EnergyRecordOut(BaseModel):
    energy_id: str
    power_plant_id: Optional[str]
    datetime: Optional[str]
    energy_generated: Optional[float]
    unit_of_measurement: Optional[str]
    create_at: Optional[dt]
    updated_at: Optional[dt]

    model_config = ConfigDict(from_attributes=True)

#====================ENVIRONMENTAL=================================
class EnviWaterAbstractionOut(BaseModel):
    wa_id: str
    company_id: Optional[str]
    year: Optional[int]
    month: Optional[str]
    quarter: Optional[str]
    volume: Optional[float]
    unit_of_measurement: Optional[str]

class HRDemographicsOut(BaseModel):
    employee_id: str
    gender: str
    birthdate: Optional[dt]
    position_id: Optional[str]
    p_np: Optional[str]
    company_id: Optional[str]
    create_at: Optional[dt]
    updated_at: Optional[dt]
    
    model_config = ConfigDict(from_attributes=True)

class EnviWaterDischargeOut(BaseModel):
    wd_id: str
    company_id: Optional[str]
    year: Optional[int]
    quarter: Optional[str]
    volume: Optional[float]
    unit_of_measurement: Optional[str]

    model_config = ConfigDict(from_attributes=True)

class EnviWaterConsumptionOut(BaseModel):
    wc_id: str
    company_id: Optional[str]
    year: Optional[int]
    quarter: Optional[str]
    volume: Optional[float]
    unit_of_measurement: Optional[str]

    model_config = ConfigDict(from_attributes=True)

class EnviDieselConsumptionOut(BaseModel):
    dc_id: str
    company_id: Optional[str]
    cp_id: Optional[str]
    unit_of_measurement: Optional[str]
    consumption: Optional[float]
    date: Optional[dt]

    model_config = ConfigDict(from_attributes=True)

class EnviElectricConsumptionOut(BaseModel):
    ec_id: str
    company_id: Optional[str]
    source: Optional[str]
    unit_of_measurement: Optional[str]
    consumption: Optional[float]
    quarter: Optional[str]
    year: Optional[int]

    model_config = ConfigDict(from_attributes=True)

class EnviNonHazardWasteOut(BaseModel):
    nhw_id: str
    company_id: Optional[str]
    metrics: Optional[str]
    unit_of_measurement: Optional[str]
    waste: Optional[float]
    month: Optional[str]
    quarter: Optional[str]
    year: Optional[int]

    model_config = ConfigDict(from_attributes=True)

class EnviHazardWasteGeneratedOut(BaseModel):
    hwg_id: str
    company_id: Optional[str]
    metrics: Optional[str]
    unit_of_measurement: Optional[str]
    waste_generated: Optional[float]
    quarter: Optional[str]
    year: Optional[int]

    model_config = ConfigDict(from_attributes=True)

class EnviHazardWasteDisposedOut(BaseModel):
    hwd_id: str
    company_id: Optional[str]
    metrics: Optional[str]
    unit_of_measurement: Optional[str]
    waste_disposed: Optional[float]
    year: Optional[int]

    model_config = ConfigDict(from_attributes=True)