from pydantic import BaseModel, ConfigDict
from datetime import datetime as dt, date
from typing import Optional, List, Dict, Any
#====================POWER PLANT ENERGY=================================
class EnergyRecordOut(BaseModel):
    energy_id: str
    power_plant_id: Optional[str]
    datetime: Optional[date]  # Use date, not datetime
    energy_generated: Optional[float]
    unit_of_measurement: Optional[str]
    create_at: Optional[dt]  
    updated_at: Optional[dt]

    model_config = ConfigDict(from_attributes=True)

class AddEnergyRecord(BaseModel):
    power_plant_id: Optional[str]
    datetime: Optional[dt]
    energy_generated: Optional[float]
    unit_of_measurement: Optional[str]
    
#====================CSR/HELP=================================
class CSRActivity(BaseModel):
    csr_id: str
    company_id: Optional[str]
    project_id: Optional[str]
    project_year: Optional[int]
    csr_report: Optional[int]
    project_expenses: Optional[float]
    created_at: Optional[dt]
    updated_at: Optional[dt]

    model_config = ConfigDict(from_attributes=True)

class CSRProject(BaseModel):
    project_id: str
    program_id: Optional[str]
    project_name: Optional[str]
    project_metrics: Optional[str]
    created_at: Optional[dt]
    updated_at: Optional[dt]

    model_config = ConfigDict(from_attributes=True)

class CSRProgram(BaseModel):
    program_id: str
    program_name: Optional[str]
    created_at: Optional[dt]
    updated_at: Optional[dt]

    model_config = ConfigDict(from_attributes=True)

#====================ENVIRONMENTAL=================================
class EnviCompanyPropertyOut(BaseModel):
    cp_id: str
    company_id: Optional[str]
    cp_name: Optional[str]
    cp_type: Optional[str]

class EnviWaterAbstractionOut(BaseModel):
    wa_id: str
    company_id: Optional[str]
    year: Optional[int]
    month: Optional[str]
    quarter: Optional[str]
    volume: Optional[float]
    unit_of_measurement: Optional[str]

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
    
#====================Human Resources=================================

class HRDemographicsOut(BaseModel):
    employee_id: str
    gender: Optional[str]
    birthdate: Optional[dt]
    position_id: Optional[str]
    p_np: Optional[str]
    company_id: Optional[str]
    employment_status: Optional[str]
    
    model_config = ConfigDict(from_attributes=True)

class HRTenureOut(BaseModel):
    employee_id: str
    start_date: dt
    end_date: Optional[dt]
    
    model_config = ConfigDict(from_attributes=True)
    
class HRSafetyWorkdataOut(BaseModel):
    company_id: str
    contractor: str
    date: dt
    manpower: int
    manhours: int
    
    model_config = ConfigDict(from_attributes=True)
    
class HRParentalLeaveOut(BaseModel):
    employee_id: str
    type_of_leave: str
    date: dt
    days: int
    
    model_config = ConfigDict(from_attributes=True)
    
class HROshOut(BaseModel):
    company_id: str
    workforce_type: str
    lost_time: bool
    date: dt
    incident_type: str
    incident_title: str
    incident_count: int
    
    model_config = ConfigDict(from_attributes=True)

class HRTrainingOut(BaseModel):
    company_id: str
    date: str
    training_title: str
    training_hours: dt
    number_of_participants: int
    
    model_config = ConfigDict(from_attributes=True)
    
class EmployabilityCombinedOut(BaseModel):
    demographics: HRDemographicsOut
    tenure: HRTenureOut

# Define the full request model
class FilteredDataRequest(BaseModel):
    filteredData: List[EnviElectricConsumptionOut]