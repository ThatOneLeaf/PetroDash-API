from pydantic import BaseModel, ConfigDict
from datetime import datetime as dt
from typing import Optional

class EnergyRecordOut(BaseModel):
    energy_id: str
    power_plant_id: Optional[str]
    datetime: Optional[str]
    energy_generated: Optional[float]
    unit_of_measurement: Optional[str]
    create_at: Optional[dt]
    updated_at: Optional[dt]

    model_config = ConfigDict(from_attributes=True)


