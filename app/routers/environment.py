from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Union
import logging
import traceback
from fastapi.responses import StreamingResponse
import io
from io import BytesIO
from datetime import datetime
from typing import Optional, List
from app.dependencies import get_db
from app.crud.base import get_all, get_many_filtered, get_one
from app.bronze.crud import (
    EnviWaterAbstraction, 
    EnviWaterDischarge, 
    EnviWaterConsumption,
    EnviDieselConsumption,
    EnviElectricConsumption,
    EnviNonHazardWaste,
    EnviHazardWasteGenerated,
    EnviHazardWasteDisposed,
    bulk_create_water_abstractions
)
from app.bronze.schemas import (
    EnviWaterAbstractionOut,
    EnviWaterDischargeOut,
    EnviWaterConsumptionOut,
    EnviDieselConsumptionOut,
    EnviElectricConsumptionOut,
    EnviNonHazardWasteOut,
    EnviHazardWasteGeneratedOut,
    EnviHazardWasteDisposedOut
)
from ..dependencies import get_db
from ..bronze.models import TableType
from ..template.envi_template_config import TEMPLATE_DEFINITIONS
from ..utils.envi_template_utils import create_excel_template, create_all_templates, get_table_mapping

router = APIRouter()

# For Validations
VALID_MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
]
VALID_QUARTERS = {"Q1", "Q2", "Q3", "Q4"}
CURRENT_YEAR = datetime.now().year

#======================================================RETRIEVING-TYPE APIs======================================================
#=================RETRIEVE ALL ENVIRONMENTAL DATA (GOLD)=================
"""
This can be used for retrieving all environmental data to display these in the tables.
"""
# Water Abstraction Endpoint
@router.get("/water_abstraction", response_model=Union[List[Dict], Dict])
@router.get("/water_abstraction/{wa_id}", response_model=Dict)
def get_water_abstraction(db: Session = Depends(get_db), wa_id: Optional[str] = None):
    """
    Get water abstraction data from vw_environment_water_abstraction view
    - If wa_id is provided: returns specific record
    - If wa_id is None: returns all records
    """
    try:
        if wa_id is not None:
            logging.info(f"Executing water abstraction query for ID: {wa_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_water_abstraction 
                WHERE water_abstraction_id = :wa_id
            """), {"wa_id": wa_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No water abstraction data found for ID: {wa_id}")
                raise HTTPException(status_code=404, detail=f"Water abstraction with ID {wa_id} not found")

            data = {
                "wa_id": row.water_abstraction_id,
                "company_name": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "unit": row.unit,
                "quarter": row.quarter,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found water abstraction data for ID: {wa_id}")
            return data
        else:
            logging.info("Executing water abstraction query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_water_abstraction"""))

            data = [
                {
                    "wa_id": row.water_abstraction_id,
                    "company_name": row.company_name,
                    "volume": float(row.volume) if row.volume is not None else 0,
                    "unit": row.unit,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No water abstraction data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching water abstraction data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Water Discharge Endpoint
@router.get("/water_discharge", response_model=Union[List[Dict], Dict])
@router.get("/water_discharge/{wd_id}", response_model=Dict)
def get_water_discharge(db: Session = Depends(get_db), wd_id: Optional[str] = None):
    """
    Get water discharge data from vw_environment_water_discharge view
    - If wd_id is provided: returns specific record
    - If wd_id is None: returns all records
    """
    try:
        if wd_id is not None:
            logging.info(f"Executing water discharge query for ID: {wd_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_water_discharge 
                WHERE water_discharge_id = :wd_id
            """), {"wd_id": wd_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No water discharge data found for ID: {wd_id}")
                raise HTTPException(status_code=404, detail=f"Water discharge with ID {wd_id} not found")

            data = {
                "wd_id": row.water_discharge_id,
                "company_name": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "unit": row.unit,
                "quarter": row.quarter,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found water discharge data for ID: {wd_id}")
            return data
        else:
            logging.info("Executing water discharge query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_water_discharge"""))

            data = [
                {
                    "wd_id": row.water_discharge_id,
                    "company_name": row.company_name,
                    "volume": float(row.volume) if row.volume is not None else 0,
                    "unit": row.unit,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No water discharge data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching water discharge data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Water Consumption Endpoint
@router.get("/water_consumption", response_model=Union[List[Dict], Dict])
@router.get("/water_consumption/{wc_id}", response_model=Dict)
def get_water_consumption(db: Session = Depends(get_db), wc_id: Optional[str] = None):
    """
    Get water consumption data from vw_environment_water_consumption view
    - If wc_id is provided: returns specific record
    - If wc_id is None: returns all records
    """
    try:
        if wc_id is not None:
            logging.info(f"Executing water consumption query for ID: {wc_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_water_consumption 
                WHERE water_consumption_id = :wc_id
            """), {"wc_id": wc_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No water consumption data found for ID: {wc_id}")
                raise HTTPException(status_code=404, detail=f"Water consumption with ID {wc_id} not found")

            data = {
                "wc_id": row.water_consumption_id,
                "company_name": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "unit": row.unit,
                "quarter": row.quarter,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found water consumption data for ID: {wc_id}")
            return data
        else:
            logging.info("Executing water consumption query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_water_consumption"""))

            data = [
                {
                    "wc_id": row.water_consumption_id,
                    "company_name": row.company_name,
                    "volume": float(row.volume) if row.volume is not None else 0,
                    "unit": row.unit,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No water consumption data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching water consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Diesel Consumption Endpoint
@router.get("/diesel_consumption", response_model=Union[List[Dict], Dict])
@router.get("/diesel_consumption/{dc_id}", response_model=Dict)
def get_diesel_consumption(db: Session = Depends(get_db), dc_id: Optional[str] = None):
    """
    Get diesel consumption data from vw_environment_diesel_consumption view
    - If dc_id is provided: returns specific record
    - If dc_id is None: returns all records
    """
    try:
        if dc_id is not None:
            logging.info(f"Executing diesel consumption query for ID: {dc_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_diesel_consumption 
                WHERE diesel_consumption_id = :dc_id
            """), {"dc_id": dc_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No diesel consumption data found for ID: {dc_id}")
                raise HTTPException(status_code=404, detail=f"Diesel consumption with ID {dc_id} not found")

            data = {
                "dc_id": row.diesel_consumption_id,
                "company_name": row.company_name,
                "company_property_name": row.company_property_name,
                "company_property_type": row.company_property_type,
                "unit_of_measurement": row.unit_of_measurement,
                "consumption": float(row.consumption) if row.consumption is not None else 0,
                "month": row.month,
                "year": row.year,
                "quarter": row.quarter,
                "date": row.date,
                "status_name": row.status_name
            }
            
            logging.info(f"Found diesel consumption data for ID: {dc_id}")
            return data
        else:
            logging.info("Executing diesel consumption query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_diesel_consumption"""))

            data = [
                {
                    "dc_id": row.diesel_consumption_id,
                    "company_name": row.company_name,
                    "company_property_name": row.company_property_name,
                    "company_property_type": row.company_property_type,
                    "unit_of_measurement": row.unit_of_measurement,
                    "consumption": float(row.consumption) if row.consumption is not None else 0,
                    "month": row.month,
                    "year": row.year,
                    "quarter": row.quarter,
                    "date": row.date,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No diesel consumption data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching diesel consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Electric Consumption Endpoint
@router.get("/electric_consumption", response_model=Union[List[Dict], Dict])
@router.get("/electric_consumption/{ec_id}", response_model=Dict)
def get_electric_consumption(db: Session = Depends(get_db), ec_id: Optional[str] = None):
    """
    Get electric consumption data from vw_environment_electric_consumption view
    - If ec_id is provided: returns specific record
    - If ec_id is None: returns all records
    """
    try:
        if ec_id is not None:
            logging.info(f"Executing electric consumption query for ID: {ec_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_electric_consumption 
                WHERE electric_consumption_id = :ec_id
            """), {"ec_id": ec_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No electric consumption data found for ID: {ec_id}")
                raise HTTPException(status_code=404, detail=f"Electric consumption with ID {ec_id} not found")

            data = {
                "ec_id": row.electric_consumption_id,
                "company_name": row.company_name,
                "source": row.consumption_source,
                "unit_of_measurement": row.unit_of_measurement,
                "consumption": float(row.consumption) if row.consumption is not None else 0,
                "quarter": row.quarter,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found electric consumption data for ID: {ec_id}")
            return data
        else:
            logging.info("Executing electric consumption query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_electric_consumption"""))

            data = [
                {
                    "ec_id": row.electric_consumption_id,
                    "company_name": row.company_name,
                    "source": row.consumption_source,
                    "unit_of_measurement": row.unit_of_measurement,
                    "consumption": float(row.consumption) if row.consumption is not None else 0,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No electric consumption data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching electric consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Non-Hazardous Waste Endpoint
@router.get("/non_hazard_waste", response_model=Union[List[Dict], Dict])
@router.get("/non_hazard_waste/{nhw_id}", response_model=Dict)
def get_non_hazard_waste(db: Session = Depends(get_db), nhw_id: Optional[str] = None):
    """
    Get non hazard waste data from vw_environment_non_hazard_waste view
    - If nhw_id is provided: returns specific record
    - If nhw_id is None: returns all records
    """
    try:
        if nhw_id is not None:
            logging.info(f"Executing non hazard waste query for ID: {nhw_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_non_hazard_waste 
                WHERE non_hazardous_waste_id = :nhw_id
            """), {"nhw_id": nhw_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No non hazard waste data found for ID: {nhw_id}")
                raise HTTPException(status_code=404, detail=f"Non hazard waste with ID {nhw_id} not found")

            data = {
                "nhw_id": row.non_hazardous_waste_id,
                "company_name": row.company_name,
                "metrics": row.metrics,
                "unit_of_measurement": row.unit_of_measurement,
                "waste": float(row.waste) if row.waste is not None else 0,
                "quarter": row.quarter,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found non hazard waste data for ID: {nhw_id}")
            return data
        else:
            logging.info("Executing non hazard waste query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_non_hazard_waste"""))

            data = [
                {
                    "nhw_id": row.non_hazardous_waste_id,
                    "company_name": row.company_name,
                    "metrics": row.metrics,
                    "unit_of_measurement": row.unit_of_measurement,
                    "waste": float(row.waste) if row.waste is not None else 0,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No non hazard waste data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching non hazard waste data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Hazardous Waste Generated Endpoint
@router.get("/hazard_waste_generated", response_model=Union[List[Dict], Dict])
@router.get("/hazard_waste_generated/{hwg_id}", response_model=Dict)
def get_hazard_waste_generated(db: Session = Depends(get_db), hwg_id: Optional[str] = None):
    """
    Get hazard waste generated data from vw_environment_hazard_waste_generated view
    - If hwg_id is provided: returns specific record
    - If hwg_id is None: returns all records
    """
    try:
        if hwg_id is not None:
            logging.info(f"Executing hazard waste generated query for ID: {hwg_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_hazard_waste_generated 
                WHERE hazard_waste_generated_id = :hwg_id
            """), {"hwg_id": hwg_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No hazard waste generated data found for ID: {hwg_id}")
                raise HTTPException(status_code=404, detail=f"Hazard waste generated with ID {hwg_id} not found")

            data = {
                "hwg_id": row.hazard_waste_generated_id,
                "company_name": row.company_name,
                "waste_type": row.waste_type,
                "unit": row.unit,
                "waste_generated": float(row.waste_generated) if row.waste_generated is not None else 0,
                "quarter": row.quarter,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found hazard waste generated data for ID: {hwg_id}")
            return data
        else:
            logging.info("Executing hazard waste generated query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_hazard_waste_generated"""))

            data = [
                {
                    "hwg_id": row.hazard_waste_generated_id,
                    "company_name": row.company_name,
                    "waste_type": row.waste_type,
                    "unit": row.unit,
                    "waste_generated": float(row.waste_generated) if row.waste_generated is not None else 0,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No hazard waste generated data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching hazard waste generated data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Hazardous Waste Disposed Endpoint
@router.get("/hazard_waste_disposed", response_model=Union[List[Dict], Dict])
@router.get("/hazard_waste_disposed/{hwd_id}", response_model=Dict)
def get_hazard_waste_disposed(db: Session = Depends(get_db), hwd_id: Optional[str] = None):
    """
    Get hazard waste disposed data from vw_environment_hazard_waste_disposed view
    - If hwd_id is provided: returns specific record
    - If hwd_id is None: returns all records
    """
    try:
        if hwd_id is not None:
            logging.info(f"Executing hazard waste disposed query for ID: {hwd_id}")
            
            result = db.execute(text("""
                SELECT * FROM gold.vw_environment_hazard_waste_disposed 
                WHERE hazard_waste_disposed_id = :hwd_id
            """), {"hwd_id": hwd_id})

            row = result.fetchone()
            
            if not row:
                logging.warning(f"No hazard waste disposed data found for ID: {hwd_id}")
                raise HTTPException(status_code=404, detail=f"Hazard waste disposed with ID {hwd_id} not found")

            data = {
                "hwd_id": row.hazard_waste_disposed_id,
                "company_name": row.company_name,
                "waste_type": row.waste_type,
                "unit": row.unit,
                "waste_disposed": float(row.waste_disposed) if row.waste_disposed is not None else 0,
                "year": row.year,
                "status_name": row.status_name
            }
            
            logging.info(f"Found hazard waste disposed data for ID: {hwd_id}")
            return data
        else:
            logging.info("Executing hazard waste disposed query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_hazard_waste_disposed"""))

            data = [
                {
                    "hwd_id": row.hazard_waste_disposed_id,
                    "company_name": row.company_name,
                    "waste_type": row.waste_type,
                    "unit": row.unit,
                    "waste_disposed": float(row.waste_disposed) if row.waste_disposed is not None else 0,
                    "year": row.year,
                    "status_name": row.status_name
                }
                for row in result
            ]
            
            logging.info(f"Query returned {len(data)} rows")
            
            if not data:
                logging.warning("No hazard waste disposed data found")
                return []

            return data
            
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching hazard waste disposed data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

#=================RETRIEVE ENVIRONMENTAL DATA (BRONZE)=================
#==========================FOR WATER ABSTRACTION==========================
@router.get("/water_abstraction_records", response_model=List[dict])
def get_water_abstraction_records(db: Session = Depends(get_db)):
    try:
        logging.info("Fetching water abstraction records with mapped IDs and status")

        query = text("""
            SELECT
                bewa.company_id,
                bewa.year,
                COALESCE(bewa.month, 'N/A') AS month,
                COALESCE(bewa.quarter, 'N/A') AS quarter,
                bewa.volume,
                bewa.unit_of_measurement,
                s.status_name
            FROM bronze.envi_water_abstraction bewa
            INNER JOIN silver.wa_id_mapping wim ON bewa.wa_id = wim.wa_id_bronze
            INNER JOIN silver.envi_water_abstraction sewa ON sewa.wa_id = wim.wa_id_silver
            INNER JOIN public.checker_status_log csl ON csl.record_id = sewa.wa_id 
            INNER JOIN public.status s ON s.status_id = csl.status_id
        """)

        result = db.execute(query)
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} water abstraction records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving water abstraction records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

#==========================BY ID==========================
"""
This can be used for retrieving specific environmental data records by their IDs and if
you wish to edit the raw data (that's why this uses bronze schema)
"""
@router.get("/bronze_water_abstraction_by_id/{wa_id}", response_model=EnviWaterAbstractionOut)
def get_water_abstraction_by_id(wa_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviWaterAbstraction, "wa_id", wa_id)
    if not record:
        raise HTTPException(status_code=404, detail="Water Abstraction record not found")
    return record

@router.get("/bronze_water_discharge_by_id/{wd_id}", response_model=EnviWaterDischargeOut)
def get_water_discharge_by_id(wd_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviWaterDischarge, "wd_id", wd_id)
    if not record:
        raise HTTPException(status_code=404, detail="Water Discharge record not found")
    return record

@router.get("/bronze_water_consumption_by_id/{wc_id}", response_model=EnviWaterConsumptionOut)
def get_water_consumption_by_id(wc_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviWaterConsumption, "wc_id", wc_id)
    if not record:
        raise HTTPException(status_code=404, detail="Water Consumption record not found")
    return record

@router.get("/bronze_diesel_consumption_by_id/{dc_id}", response_model=EnviDieselConsumptionOut)
def get_diesel_consumption_by_id(dc_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviDieselConsumption, "dc_id", dc_id)
    if not record:
        raise HTTPException(status_code=404, detail="Diesel Consumption record not found")
    return record

@router.get("/bronze_electric_consumption_by_id/{ec_id}", response_model=EnviElectricConsumptionOut)
def get_electric_consumption_by_id(ec_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviElectricConsumption, "ec_id", ec_id)
    if not record:
        raise HTTPException(status_code=404, detail="Electric Consumption record not found")
    return record

@router.get("/bronze_non_hazard_waste_by_id/{nhw_id}", response_model=EnviNonHazardWasteOut)
def get_non_hazard_waste_by_id(nhw_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviNonHazardWaste, "nhw_id", nhw_id)
    if not record:
        raise HTTPException(status_code=404, detail="Non Hazardous Waste record not found")
    return record

@router.get("/bronze_hazard_waste_generated_by_id/{hwg_id}", response_model=EnviHazardWasteGeneratedOut)
def get_hazard_waste_generated_by_id(hwg_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviHazardWasteGenerated, "hwg_id", hwg_id)
    if not record:
        raise HTTPException(status_code=404, detail="Hazardous Waste Generated record not found")
    return record

@router.get("/bronze_hazard_waste_disposed_by_id/{hwd_id}", response_model=EnviHazardWasteDisposedOut)
def get_hazard_waste_disposed_by_id(hwd_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviHazardWasteDisposed, "hwd_id", hwd_id)
    if not record:
        raise HTTPException(status_code=404, detail="Hazardous Waste Disposed record not found")
    return record

#======================================================CRUD-TYPE APIs======================================================
#====================================BULK ADD RECORDS (ENVI)====================================
@router.post("/bulk_upload_water_abstraction")
def bulk_upload_water_abstraction(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()  # If not using async def
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'month', 'quarter', 'volume', 'unit_of_measurement'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # data cleaning & row-level validation
        rows = []
        CURRENT_YEAR = datetime.now().year
        for i, row in df.iterrows():
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid company_id")

            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid year")

            if row["month"] not in [
                "January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"
            ]:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid month '{row['month']}'")

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid quarter '{row['quarter']}'")

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid volume")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "month": row["month"],
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
            })

        count = bulk_create_water_abstractions(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ============== EXCEL TEMPLATE ENDPOINTS ==============
@router.get("/create_data_template")
async def create_data_template(
    table_type: TableType = Query(TableType.ALL, description="Type of template to generate"),
    include_examples: bool = Query(True, description="Include example data in template"),
    format_type: str = Query("excel", description="Output format (currently only excel supported)")
):
    """
    Generate Excel template(s) for data import
    
    - **table_type**: Specific table template or 'all' for combined template
    - **include_examples**: Whether to include example rows
    - **format_type**: Output format (currently only excel)
    """
    
    try:
        if format_type.lower() != "excel":
            raise HTTPException(status_code=400, detail="Only Excel format is currently supported")
        
        if table_type == TableType.ALL:
            # Generate combined template with all tables
            excel_file = create_all_templates()
            filename = f"environmental_data_templates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            
            return StreamingResponse(
                io.BytesIO(excel_file.read()),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        else:
            # Generate specific table template
            excel_file = create_excel_template(table_type.value, include_examples)
            template = TEMPLATE_DEFINITIONS[table_type.value]
            
            return StreamingResponse(
                io.BytesIO(excel_file.read()),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={template['filename']}"}
            )
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")

@router.get("/template_info", response_model=List[Dict])
async def get_template_info():
    """
    Get information about available templates
    """
    template_info = []
    
    for table_type, template in TEMPLATE_DEFINITIONS.items():
        info = {
            "table_type": table_type,
            "table_name": f"bronze.envi_{table_type}",
            "filename": template["filename"],
            "sheet_name": template["sheet_name"],
            "column_count": len(template["headers"]),
            "columns": template["headers"],
            "data_types": template["data_types"],
            "descriptions": template["descriptions"]
        }
        template_info.append(info)
    
    return template_info

@router.get("/create_template/{table_name}")
async def create_individual_template(
    table_name: str,
    include_examples: bool = Query(True, description="Include example data")
):
    """
    Generate template for a specific table by name
    """
    # Get table mapping
    table_mapping = get_table_mapping()
    
    # Remove 'bronze.' prefix if present
    clean_table_name = table_name.replace("bronze.", "")
    
    if clean_table_name not in table_mapping:
        raise HTTPException(
            status_code=404, 
            detail=f"Table '{table_name}' not found. Available tables: {list(table_mapping.keys())}"
        )
    
    template_key = table_mapping[clean_table_name]
    
    try:
        excel_file = create_excel_template(template_key, include_examples)
        template = TEMPLATE_DEFINITIONS[template_key]
        
        return StreamingResponse(
            io.BytesIO(excel_file.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={template['filename']}"}
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")


# FOR COMBOBOX
@router.get("/distinct_cp_names", response_model=List[dict])
def get_distinct_cp_names(db: Session = Depends(get_db)):
    try:
        logging.info("Fetching distinct cp_id, cp_name values from envi_company_property")

        query = text("""
            SELECT DISTINCT cp_id, cp_name FROM silver.envi_company_property
        """)

        result = db.execute(query)
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} distinct cp_id, cp_name entries")
        return data

    except Exception as e:
        logging.error(f"Error retrieving cp_id and cp_name values: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")