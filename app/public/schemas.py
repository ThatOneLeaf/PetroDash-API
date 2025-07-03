from pydantic import BaseModel, ConfigDict,EmailStr
from datetime import datetime as dt, date
from typing import Optional, List, Dict, Any

# Pydantic Schemas
class UserProfileCreate(BaseModel):
    emp_id: Optional[str] = None
    first_name: str
    last_name: str
    middle_name: Optional[str] = None
    suffix: Optional[str] = None
    contact_number: Optional[str] = None
    address: Optional[str] = None
    birthdate: Optional[str] = None
    gender: Optional[str] = None

class AccountCreate(BaseModel):
    email: EmailStr
    account_role: str
    power_plant_id: str
    company_id: str
    account_status: str = "active"
    profile: UserProfileCreate



class AccountProfileOut(BaseModel):
    account_id: str
    email: str
    account_role: str
    power_plant_id: Optional[str]=None
    company_id: Optional[str]=None
    account_status: str
    first_name: str
    last_name: str
    middle_name: Optional[str] = None
    suffix: Optional[str] = None
    contact_number: Optional[str] = None
    address: Optional[str] = None
    birthdate: Optional[date] = None
    gender: Optional[str] = None

    class Config:
        from_attributes = True