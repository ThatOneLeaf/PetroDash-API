from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query, Request
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
    EnviCompanyProperty,
    EnviWaterAbstraction, 
    EnviWaterDischarge, 
    EnviWaterConsumption,
    EnviDieselConsumption,
    EnviElectricConsumption,
    EnviNonHazardWaste,
    EnviHazardWasteGenerated,
    EnviHazardWasteDisposed,
    #single insert
    insert_create_water_abstraction,
    insert_create_water_discharge,
    insert_create_water_consumption,
    insert_create_diesel_consumption,
    insert_create_electric_consumption,
    insert_create_non_hazard_waste,
    insert_create_hazard_waste_generated,
    insert_create_hazard_waste_disposed,
    #bulk insert
    bulk_create_water_abstractions,
    bulk_create_water_discharge,
    bulk_create_water_consumption,
    bulk_create_electric_consumption,
    bulk_create_non_hazard_waste,
    bulk_create_hazard_waste_generated,
    bulk_create_hazard_waste_disposed,
    bulk_create_diesel_consumption,
    #update records
    update_water_abstraction,
    update_water_discharge,
    update_water_consumption,
    update_diesel_consumption,
    update_electric_consumption,
    update_non_hazard_waste,
    update_hazard_waste_generated,
    update_hazard_waste_disposed
)
from app.bronze.schemas import (
    EnviWaterAbstractionOut,
    EnviWaterDischargeOut,
    EnviWaterConsumptionOut,
    EnviDieselConsumptionOut,
    EnviElectricConsumptionOut,
    EnviNonHazardWasteOut,
    EnviHazardWasteGeneratedOut,
    EnviHazardWasteDisposedOut,
    FilteredDataRequest
)
#from ..dependencies import get_db
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
                "company": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "unit": row.unit,
                "quarter": row.quarter,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found water abstraction data for ID: {wa_id}")
            return data
        else:
            logging.info("Executing water abstraction query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_water_abstraction"""))

            data = [
                {
                    "wa_id": row.water_abstraction_id,
                    "company": row.company_name,
                    "volume": float(row.volume) if row.volume is not None else 0,
                    "unit": row.unit,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status": row.status_name
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
                "company": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "unit": row.unit,
                "quarter": row.quarter,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found water discharge data for ID: {wd_id}")
            return data
        else:
            logging.info("Executing water discharge query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_water_discharge"""))

            data = [
                {
                    "wd_id": row.water_discharge_id,
                    "company": row.company_name,
                    "volume": float(row.volume) if row.volume is not None else 0,
                    "unit": row.unit,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status": row.status_name
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
                "company": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "unit": row.unit,
                "quarter": row.quarter,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found water consumption data for ID: {wc_id}")
            return data
        else:
            logging.info("Executing water consumption query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_water_consumption"""))

            data = [
                {
                    "wc_id": row.water_consumption_id,
                    "company": row.company_name,
                    "volume": float(row.volume) if row.volume is not None else 0,
                    "unit": row.unit,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status": row.status_name
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
                "company": row.company_name,
                "property": row.company_property_name,
                "type": row.company_property_type,
                "unit": row.unit_of_measurement,
                "consumption": float(row.consumption) if row.consumption is not None else 0,
                "month": row.month,
                "quarter": row.quarter,
                "year": row.year,
                "date": row.date,
                "status": row.status_name
            }
            
            logging.info(f"Found diesel consumption data for ID: {dc_id}")
            return data
        else:
            logging.info("Executing diesel consumption query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_diesel_consumption"""))

            data = [
                {
                    "dc_id": row.diesel_consumption_id,
                    "company": row.company_name,
                    "property": row.company_property_name,
                    "type": row.company_property_type,
                    "unit": row.unit_of_measurement,
                    "consumption": float(row.consumption) if row.consumption is not None else 0,
                    "month": row.month,
                    "quarter": row.quarter,
                    "year": row.year,
                    "date": row.date,
                    "status": row.status_name
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
                "company": row.company_name,
                "source": row.consumption_source,
                "unit": row.unit_of_measurement,
                "consumption": float(row.consumption) if row.consumption is not None else 0,
                "quarter": row.quarter,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found electric consumption data for ID: {ec_id}")
            return data
        else:
            logging.info("Executing electric consumption query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_electric_consumption"""))

            data = [
                {
                    "ec_id": row.electric_consumption_id,
                    "company": row.company_name,
                    "source": row.consumption_source,
                    "unit": row.unit_of_measurement,
                    "consumption": float(row.consumption) if row.consumption is not None else 0,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status": row.status_name
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
                "company": row.company_name,
                "metrics": row.metrics,
                "unit": row.unit_of_measurement,
                "waste": float(row.waste) if row.waste is not None else 0,
                "quarter": row.quarter,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found non hazard waste data for ID: {nhw_id}")
            return data
        else:
            logging.info("Executing non hazard waste query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_non_hazard_waste"""))

            data = [
                {
                    "nhw_id": row.non_hazardous_waste_id,
                    "company": row.company_name,
                    "metrics": row.metrics,
                    "unit": row.unit_of_measurement,
                    "waste": float(row.waste) if row.waste is not None else 0,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status": row.status_name
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
                "company": row.company_name,
                "type": row.waste_type,
                "unit": row.unit,
                "waste generated": float(row.waste_generated) if row.waste_generated is not None else 0,
                "quarter": row.quarter,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found hazard waste generated data for ID: {hwg_id}")
            return data
        else:
            logging.info("Executing hazard waste generated query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_hazard_waste_generated"""))

            data = [
                {
                    "hwg_id": row.hazard_waste_generated_id,
                    "company": row.company_name,
                    "type": row.waste_type,
                    "unit": row.unit,
                    "waste generated": float(row.waste_generated) if row.waste_generated is not None else 0,
                    "quarter": row.quarter,
                    "year": row.year,
                    "status": row.status_name
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
                "company": row.company_name,
                "type": row.waste_type,
                "unit": row.unit,
                "waste disposed": float(row.waste_disposed) if row.waste_disposed is not None else 0,
                "year": row.year,
                "status": row.status_name
            }
            
            logging.info(f"Found hazard waste disposed data for ID: {hwd_id}")
            return data
        else:
            logging.info("Executing hazard waste disposed query for all records")
            
            result = db.execute(text("""SELECT * FROM gold.vw_environment_hazard_waste_disposed"""))

            data = [
                {
                    "hwd_id": row.hazard_waste_disposed_id,
                    "company": row.company_name,
                    "type": row.waste_type,
                    "unit": row.unit,
                    "waste disposed": float(row.waste_disposed) if row.waste_disposed is not None else 0,
                    "year": row.year,
                    "status": row.status_name
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

#=================RETRIEVE ENVIRONMENTAL DATA (BRONZE)====================
#==========================FOR WATER ABSTRACTION==========================
@router.get("/water_abstraction_records", response_model=List[dict])
def get_water_abstraction_records(db: Session = Depends(get_db)):
    try:
        logging.info("Fetching water abstraction records with mapped IDs and status")

        query = text("""
            SELECT
                bewa.wa_id,
                cm.company_name AS company,
                bewa.year,
                COALESCE(bewa.month, 'N/A') AS month,
                COALESCE(bewa.quarter, 'N/A') AS quarter,
                bewa.volume,
                bewa.unit_of_measurement AS unit,
                s.status_name AS status
            FROM bronze.envi_water_abstraction bewa
            LEFT JOIN silver.wa_id_mapping wim ON bewa.wa_id = wim.wa_id_bronze
            LEFT JOIN silver.envi_water_abstraction sewa ON sewa.wa_id = wim.wa_id_silver
            INNER JOIN public.checker_status_log csl ON csl.record_id = bewa.wa_id
            INNER JOIN public.status s ON s.status_id = csl.status_id
            INNER JOIN ref.company_main cm ON cm.company_id = bewa.company_id
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
#====================================SINGLE ADD RECORDS (ENVI)====================================
#---WATER ABSTRACTION---
@router.post("/single_upload_water_abstraction")
def single_upload_water_abstraction(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single water abstraction record")
        CURRENT_YEAR = datetime.now().year

        required_fields = ['company_id', 'year', 'month', 'quarter', 'volume', 'unit_of_measurement']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["month"] not in [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        record = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "month": data["month"],
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
        }

        # Assuming you have a single insert function
        insert_create_water_abstraction(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---WATER-DISCHARGE---
@router.post("/single_upload_water_discharge")
def single_upload_water_discharge(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single water discharge record")
        CURRENT_YEAR = datetime.now().year
        required_fields = ['company_id', 'year', 'quarter', 'volume', 'unit_of_measurement']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        record = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit_of_measurement"].strip()
        }

        insert_create_water_discharge(db, record)
        return {"message": "1 water discharge record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---WATER-CONSUMPTION---
@router.post("/single_upload_water_consumption")
def single_upload_water_consumption(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single water consumption record")
        CURRENT_YEAR = datetime.now().year
        required_fields = ['company_id', 'year', 'quarter', 'volume', 'unit_of_measurement']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        record = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit_of_measurement"].strip()
        }

        insert_create_water_consumption(db, record)
        return {"message": "1 water consumption record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---ELECTRIC-CONSUMPTION---
@router.post("/single_upload_electric_consumption")
def single_upload_electric_consumption(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single electric consumption record")
        CURRENT_YEAR = datetime.now().year
        required_fields = ['company_id', 'source', 'unit_of_measurement', 'consumption', 'quarter', 'year']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["source"], str) or not data["source"].strip():
            raise HTTPException(status_code=422, detail="Invalid source")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["consumption"], (int, float)) or data["consumption"] < 0:
            raise HTTPException(status_code=422, detail="Invalid consumption")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        record = {
            "company_id": data["company_id"].strip(),
            "source": data["source"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "consumption": float(data["consumption"]),
            "quarter": data["quarter"],
            "year": int(data["year"]),
        }

        insert_create_electric_consumption(db, record)
        return {"message": "1 electric consumption record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---DIESEL-CONSUMPTION---
@router.post("/single_upload_diesel_consumption")
def single_upload_diesel_consumption(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single diesel consumption record")
        required_fields = ['company_id', 'cp_id', 'unit_of_measurement', 'consumption', 'date']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["cp_id"], str) or not data["cp_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid cp_id")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["consumption"], (int, float)) or data["consumption"] < 0:
            raise HTTPException(status_code=422, detail="Invalid consumption")

        try:
            date_parsed = datetime.strptime(data["date"], "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD.")

        record = {
            "company_id": data["company_id"].strip(),
            "cp_id": data["cp_id"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "consumption": float(data["consumption"]),
            "date": date_parsed
        }

        insert_create_diesel_consumption(db, record)
        return {"message": "1 diesel consumption record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---NON-HAZARD-WASTE---
@router.post("/single_upload_non_hazard_waste")
def single_upload_non_hazard_waste(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single non-hazard waste record")
        CURRENT_YEAR = datetime.now().year
        required_fields = ['company_id', 'metrics', 'unit_of_measurement', 'waste', 'month', 'quarter', 'year']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste"], (int, float)) or data["waste"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste")

        if data["month"] not in [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        record = {
            "company_id": data["company_id"].strip(),
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "waste": float(data["waste"]),
            "month": data["month"],
            "quarter": data["quarter"],
            "year": int(data["year"]),
        }

        insert_create_non_hazard_waste(db, record)
        return {"message": "1 non-hazard waste record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---HAZARD-WASTE-GENERATED---
@router.post("/single_upload_hazard_waste_generated")
def single_upload_hazard_waste_generated(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single hazard waste generated record")
        CURRENT_YEAR = datetime.now().year
        required_fields = ['company_id', 'metrics', 'unit_of_measurement', 'waste_generated', 'quarter', 'year']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste_generated"], (int, float)) or data["waste_generated"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste_generated")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        record = {
            "company_id": data["company_id"].strip(),
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "waste_generated": float(data["waste_generated"]),
            "quarter": data["quarter"],
            "year": int(data["year"]),
        }

        insert_create_hazard_waste_generated(db, record)
        return {"message": "1 hazard waste generated record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

#---HAZARD-WASTE-DISPOSED---
@router.post("/single_upload_hazard_waste_disposed")
def single_upload_hazard_waste_disposed(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single hazard waste disposed record")
        CURRENT_YEAR = datetime.now().year
        required_fields = ['company_id', 'metrics', 'unit_of_measurement', 'waste_disposed', 'year']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste_disposed"], (int, float)) or data["waste_disposed"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste_disposed")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        record = {
            "company_id": data["company_id"].strip(),
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "waste_disposed": float(data["waste_disposed"]),
            "year": int(data["year"]),
        }

        insert_create_hazard_waste_disposed(db, record)
        return {"message": "1 hazard waste disposed record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


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
    
@router.post("/bulk_upload_water_discharge")
def bulk_upload_water_discharge(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()  # If not using async def
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'volume', 'unit_of_measurement'}
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

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid quarter '{row['quarter']}'")

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid volume")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
            })

        count = bulk_create_water_discharge(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/bulk_upload_water_consumption")
def bulk_upload_water_consumption(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()  # If not using async def
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'volume', 'unit_of_measurement'}
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

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid quarter '{row['quarter']}'")

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid volume")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
            })

        count = bulk_create_water_consumption(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/bulk_upload_electric_consumption")
def bulk_upload_electric_consumption(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk electric consumption data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'source', 'unit_of_measurement', 'consumption'}
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

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid quarter '{row['quarter']}'")

            if not isinstance(row["source"], str) or not row["source"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid source")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            if not isinstance(row["consumption"], (int, float)) or row["consumption"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid consumption")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "source": row["source"].strip(),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
                "consumption": float(row["consumption"]),
            })

        count = bulk_create_electric_consumption(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_upload_non_hazard_waste")
def bulk_upload_non_hazard_waste(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk non-hazard waste data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'month', 'quarter', 'metrics', 'unit_of_measurement', 'waste'}
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

            if not isinstance(row["metrics"], str) or not row["metrics"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid metrics")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            if not isinstance(row["waste"], (int, float)) or row["waste"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid waste")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "month": row["month"],
                "quarter": row["quarter"],
                "metrics": row["metrics"].strip(),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
                "waste": float(row["waste"]),
            })

        count = bulk_create_non_hazard_waste(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_upload_hazard_waste_generated")
def bulk_upload_hazard_waste_generated(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk hazard waste generated data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'metrics', 'unit_of_measurement', 'waste_generated'}
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

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid quarter '{row['quarter']}'")

            if not isinstance(row["metrics"], str) or not row["metrics"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid metrics")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            if not isinstance(row["waste_generated"], (int, float)) or row["waste_generated"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid waste_generated")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "metrics": row["metrics"].strip(),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
                "waste_generated": float(row["waste_generated"]),
            })

        count = bulk_create_hazard_waste_generated(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_upload_hazard_waste_disposed")
def bulk_upload_hazard_waste_disposed(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk hazard waste disposed data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'metrics', 'unit_of_measurement', 'waste_disposed'}
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

            if not isinstance(row["metrics"], str) or not row["metrics"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid metrics")

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            if not isinstance(row["waste_disposed"], (int, float)) or row["waste_disposed"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid waste_disposed")

            rows.append({
                "company_id": row["company_id"].strip(),
                "year": int(row["year"]),
                "metrics": row["metrics"].strip(),
                "unit_of_measurement": row["unit_of_measurement"].strip(),
                "waste_disposed": float(row["waste_disposed"]),
            })

        count = bulk_create_hazard_waste_disposed(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/bulk_upload_diesel_consumption")
def bulk_upload_diesel_consumption(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk diesel consumption data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'cp_name', 'unit_of_measurement', 'consumption', 'date'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Pre-fetch all company properties for lookup (case-insensitive)
        company_properties = db.query(EnviCompanyProperty).all()
        cp_lookup = {}
        for cp in company_properties:
            key = (cp.company_id.lower(), cp.cp_name.lower())
            cp_lookup[key] = cp.cp_id

        # data cleaning & row-level validation
        rows = []
        for i, row in df.iterrows():
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid company_id")

            if not isinstance(row["cp_name"], str) or not row["cp_name"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid cp_name")

            # Look up cp_id using company_id and cp_name (case-insensitive)
            company_id = row["company_id"].strip()
            cp_name = row["cp_name"].strip()
            cp_lookup_key = (company_id.lower(), cp_name.lower())
            
            if cp_lookup_key not in cp_lookup:
                raise HTTPException(
                    status_code=422, 
                    detail=f"Row {i+2}: Company property not found for company_id '{company_id}' and cp_name '{cp_name}'"
                )
            
            cp_id = cp_lookup[cp_lookup_key]

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid unit_of_measurement")

            if not isinstance(row["consumption"], (int, float)) or row["consumption"] < 0:
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid consumption")

            # Validate date
            try:
                if isinstance(row["date"], str):
                    # Try to parse string date
                    parsed_date = pd.to_datetime(row["date"]).date()
                elif hasattr(row["date"], 'date'):
                    # Handle pandas Timestamp
                    parsed_date = row["date"].date()
                elif isinstance(row["date"], datetime.date):
                    # Already a date object
                    parsed_date = row["date"]
                else:
                    raise ValueError("Invalid date format")
            except (ValueError, TypeError):
                raise HTTPException(status_code=422, detail=f"Row {i+2}: Invalid date format")

            # Extract year from date for the crud function
            year = parsed_date.year

            rows.append({
                "company_id": company_id,
                "cp_id": cp_id,
                "unit_of_measurement": row["unit_of_measurement"].strip(),
                "consumption": float(row["consumption"]),
                "date": parsed_date,
                "year": year  # Added for the crud function grouping logic
            })

        count = bulk_create_diesel_consumption(db, rows)
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
    
@router.post("/export_excel")
async def export_excel(request: Request):
    data = await request.json()
    
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    # Write to Excel in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Filtered Data")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=exported_data.xlsx"
        }
    )

#=====================================EDIT RECORDS (BRONZE)=====================================
@router.post("/edit_water_abstraction")
def edit_water_abstraction(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit water abstraction record")
        required_fields = ['company_id', 'year', 'month', 'quarter', 'volume', 'unit_of_measurement']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")


        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["month"] not in [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        wa_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "month": data["month"],
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
        }

        update_water_abstraction(db, wa_id, record_data)
        return {"message": "Water abstraction record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/edit_water_discharge")
def edit_water_discharge(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit water discharge record")
        required_fields = ['company_id', 'year', 'quarter', 'volume', 'unit_of_measurement']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        wd_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
        }

        update_water_discharge(db, wd_id, record_data)
        return {"message": "Water discharge record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/edit_water_consumption")
def edit_water_consumption( 
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit water consumption record")
        required_fields = ['company_id', 'year', 'quarter', 'volume', 'unit_of_measurement']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        wc_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
        }

        update_water_consumption(db, wc_id, record_data)
        return {"message": "Water consumption record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/edit_electric_consumption")
def edit_electric_consumption(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit electric consumption record")
        required_fields = ['company_id', 'year', 'quarter', 'source', 'unit_of_measurement', 'consumption']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["source"], str) or not data["source"].strip():
            raise HTTPException(status_code=422, detail="Invalid source")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["consumption"], (int, float)) or data["consumption"] < 0:
            raise HTTPException(status_code=422, detail="Invalid consumption")

        ec_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "source": data["source"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "consumption": float(data["consumption"]),
        }

        update_electric_consumption(db, ec_id, record_data)
        return {"message": "Electric consumption record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/edit_non_hazard_waste")
def edit_non_hazard_waste(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit non-hazard waste record")
        required_fields = ['company_id', 'year', 'month', 'quarter', 'metrics', 'unit_of_measurement', 'waste']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["month"] not in [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste"], (int, float)) or data["waste"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste")

        nhw_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "month": data["month"],
            "quarter": data["quarter"],
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "waste": float(data["waste"]),
        }

        update_non_hazard_waste(db, nhw_id, record_data)
        return {"message": "Non-hazard waste record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/edit_hazard_waste_generated")
def edit_hazard_waste_generated(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit hazard waste generated record")
        required_fields = ['company_id', 'year', 'quarter', 'metrics', 'unit_of_measurement', 'waste_generated']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste_generated"], (int, float)) or data["waste_generated"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste_generated")

        hwg_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "waste_generated": float(data["waste_generated"]),
        }

        update_hazard_waste_generated(db, hwg_id, record_data)
        return {"message": "Hazard waste generated record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/edit_hazard_waste_disposed")
def edit_hazard_waste_disposed(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit hazard waste disposed record")
        required_fields = ['company_id', 'year', 'metrics', 'unit_of_measurement', 'waste_disposed']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste_disposed"], (int, float)) or data["waste_disposed"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste_disposed")

        hwd_id = data["id"]
        record_data = {
            "company_id": data["company_id"].strip(),
            "year": int(data["year"]),
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "waste_disposed": float(data["waste_disposed"]),
        }

        update_hazard_waste_disposed(db, hwd_id, record_data)
        return {"message": "Hazard waste disposed record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/edit_diesel_consumption")
def edit_diesel_consumption(
    data: dict, db: Session = Depends(get_db)
):
    try:
        logging.info("Edit diesel consumption record")
        required_fields = ['company_id', 'cp_name', 'unit_of_measurement', 'consumption', 'date']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["cp_name"], str) or not data["cp_name"].strip():
            raise HTTPException(status_code=422, detail="Invalid cp_name")

        # Look up cp_id using company_id and cp_name (case-insensitive)
        company_id = data["company_id"].strip()
        cp_name = data["cp_name"].strip()
        cp_lookup_key = (company_id.lower(), cp_name.lower())
        
        company_properties = db.query(EnviCompanyProperty).all()
        cp_lookup = {(cp.company_id.lower(), cp.cp_name.lower()): cp.cp_id for cp in company_properties}
        
        if cp_lookup_key not in cp_lookup:
            raise HTTPException(
                status_code=422, 
                detail=f"Company property not found for company_id '{company_id}' and cp_name '{cp_name}'"
            )
        
        cp_id = cp_lookup[cp_lookup_key]

        if not isinstance(data["unit_of_measurement"], str) or not data["unit_of_measurement"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["consumption"], (int, float)) or data["consumption"] < 0:
            raise HTTPException(status_code=422, detail="Invalid consumption")

        # Validate date
        try:
            if isinstance(data["date"], str):
                # Try to parse string date
                parsed_date = pd.to_datetime(data["date"]).date()
            elif hasattr(data["date"], 'date'):
                # Handle pandas Timestamp
                parsed_date = data["date"].date()
            elif isinstance(data["date"], datetime.date):
                # Already a date object
                parsed_date = data["date"]
            else:
                raise ValueError("Invalid date format")
        except (ValueError, TypeError):
            raise HTTPException(status_code=422, detail="Invalid date format")
        # Extract year from date for the crud function
        year = parsed_date.year
        dc_id = data["id"]
        record_data = {
            "company_id": company_id,
            "cp_id": cp_id,
            "unit_of_measurement": data["unit_of_measurement"].strip(),
            "consumption": float(data["consumption"]),
            "date": parsed_date,
            "year": year  # Added for the crud function grouping logic
        }
        update_diesel_consumption(db, dc_id, record_data)
        return {"message": "Diesel consumption record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))