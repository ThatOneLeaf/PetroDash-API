from decimal import Decimal
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
    EnviCompanyPropertyOut,
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

from app.reference.models import CompanyMain
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

def get_column_mapping(table_type):
    """Get column mapping for different table types"""
    mappings = {
        'water_abstraction': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'year': 'year',
            'month': 'month',
            'quarter': 'quarter',
            'volume': 'volume',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement'
        },
        'water_discharge': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'year': 'year',
            'quarter': 'quarter',
            'volume': 'volume',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement'
        },
        'water_consumption': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'year': 'year',
            'quarter': 'quarter',
            'volume': 'volume',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement'
        },
        'diesel_consumption': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'company property': 'cp_name',
            'cp_name': 'cp_name',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement',
            'consumption': 'consumption',
            'date': 'date'
        },
        'electric_consumption': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'source': 'source',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement',
            'consumption': 'consumption',
            'quarter': 'quarter',
            'year': 'year'
        },
        'non_hazard_waste': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'metrics': 'metrics',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement',
            'waste': 'waste',
            'month': 'month',
            'quarter': 'quarter',
            'year': 'year'
        },
        'hazard_waste_generated': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'metrics': 'metrics',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement',
            'waste generated': 'waste_generated',
            'waste_generated': 'waste_generated',
            'quarter': 'quarter',
            'year': 'year'
        },
        'hazard_waste_disposed': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'metrics': 'metrics',
            'unit of measurement': 'unit_of_measurement',
            'unit_of_measurement': 'unit_of_measurement',
            'waste disposed': 'waste_disposed',
            'waste_disposed': 'waste_disposed',
            'year': 'year'
        },
        'company_property': {
            'company id': 'company_id',
            'company_id': 'company_id',
            'company property name': 'cp_name',
            'cp_name': 'cp_name',
            'company property type': 'cp_type',
            'cp_type': 'cp_type'
        }
    }
    return mappings.get(table_type, {})

def normalize_dataframe_columns(df, table_type):
    """Normalize DataFrame columns based on table type"""
    column_mapping = get_column_mapping(table_type)
    
    # Apply mapping with fallback to standard normalization
    normalized_columns = []
    for col in df.columns:
        # First try exact mapping
        mapped_col = column_mapping.get(col.lower())
        if mapped_col:
            normalized_columns.append(mapped_col)
        else:
            # Fallback: convert to lowercase and replace spaces with underscores
            normalized_columns.append(col.lower().replace(' ', '_'))
    
    df.columns = normalized_columns
    return df

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
                "metrics": row.waste_type,
                "unit": row.unit,
                "waste": float(row.waste_generated) if row.waste_generated is not None else 0,
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
                    "metrics": row.waste_type,
                    "unit": row.unit,
                    "waste": float(row.waste_generated) if row.waste_generated is not None else 0,
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
                "metrics": row.waste_type,
                "unit": row.unit,
                "waste": float(row.waste_disposed) if row.waste_disposed is not None else 0,
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
                    "metrics": row.waste_type,
                    "unit": row.unit,
                    "waste": float(row.waste_disposed) if row.waste_disposed is not None else 0,
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
                COALESCE(s2.status_name, s1.status_name) as status  -- Changed order: s2 first, then s1
            FROM bronze.envi_water_abstraction bewa
            INNER JOIN ref.company_main cm ON bewa.company_id = cm.company_id
            INNER JOIN silver.wa_id_mapping wim ON bewa.wa_id = wim.wa_id_bronze
            LEFT JOIN record_status rs1 ON wim.wa_id_silver = rs1.record_id
            LEFT JOIN record_status rs2 ON wim.wa_id_bronze = rs2.record_id
            LEFT JOIN status s1 ON rs1.status_id = s1.status_id
            LEFT JOIN status s2 ON rs2.status_id = s2.status_id
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
@router.get("/bronze_company_property_by_id/{cp_id}", response_model=EnviCompanyPropertyOut)
def get_company_property_by_id(cp_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviCompanyProperty, "cp_id", cp_id)
    if not record:
        raise HTTPException(status_code=404, detail="Company Property record not found")
    return record

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

        valid_months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        if data["month"] not in valid_months:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        # Month-to-quarter validation
        month_to_quarter = {
            "January": "Q1", "February": "Q1", "March": "Q1",
            "April": "Q2", "May": "Q2", "June": "Q2",
            "July": "Q3", "August": "Q3", "September": "Q3",
            "October": "Q4", "November": "Q4", "December": "Q4"
        }
        expected_quarter = month_to_quarter[data["month"]]
        if data["quarter"] != expected_quarter:
            raise HTTPException(
                status_code=422,
                detail=f"Mismatch between month '{data['month']}' and quarter '{data['quarter']}'. "
                       f"Expected quarter is '{expected_quarter}'."
            )

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

        valid_months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        if data["month"] not in valid_months:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        # Month-to-quarter validation
        month_to_quarter = {
            "January": "Q1", "February": "Q1", "March": "Q1",
            "April": "Q2", "May": "Q2", "June": "Q2",
            "July": "Q3", "August": "Q3", "September": "Q3",
            "October": "Q4", "November": "Q4", "December": "Q4"
        }
        expected_quarter = month_to_quarter[data["month"]]
        if data["quarter"] != expected_quarter:
            raise HTTPException(
                status_code=422,
                detail=f"Mismatch between month '{data['month']}' and quarter '{data['quarter']}'. "
                       f"Expected quarter is '{expected_quarter}'."
            )

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
# WATER ABSTRACTION BULK UPLOAD
@router.post("/bulk_upload_water_abstraction")
def bulk_upload_water_abstraction(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'water_abstraction')

        # basic validation...
        required_columns = {'company_id', 'year', 'month', 'quarter', 'volume', 'unit_of_measurement'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Get valid units of measurement from database
        valid_units = db.query(EnviWaterAbstraction.unit_of_measurement).all()
        valid_units_set = {unit[0] for unit in valid_units}
        
        # Define month to quarter mapping
        month_to_quarter = {
            "January": "Q1", "February": "Q1", "March": "Q1",
            "April": "Q2", "May": "Q2", "June": "Q2",
            "July": "Q3", "August": "Q3", "September": "Q3",
            "October": "Q4", "November": "Q4", "December": "Q4"
        }

        # data cleaning & row-level validation
        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year
        
        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)
            
            # Company ID validation
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            if row["month"] not in month_to_quarter.keys():
                validation_errors.append(f"Row {row_number}: Invalid month '{row['month']}'")
                continue

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{row['quarter']}'")
                continue

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid volume")
                continue

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            # Unit validation
            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            # Month and quarter match validation
            expected_quarter = month_to_quarter[row["month"]]
            if row["quarter"] != expected_quarter:
                validation_errors.append(f"Row {row_number}: Month '{row['month']}' should be in quarter '{expected_quarter}', but '{row['quarter']}' was provided")
                continue

            # If all validations pass, add to rows
            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "month": row["month"],
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": unit_stripped,
            })

        # If there are validation errors, return them
        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        # If no validation errors, proceed with bulk insert
        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_water_abstractions(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# DIESEL CONSUMPTION BULK UPLOAD
@router.post("/bulk_upload_diesel_consumption")
def bulk_upload_diesel_consumption(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info("Add bulk diesel consumption data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'diesel_consumption')

        required_columns = {'company_id', 'cp_name', 'unit_of_measurement', 'consumption', 'date'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {required_columns - set(df.columns)}"
            )

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Fetch all valid unit_of_measurement values
        valid_units = db.query(EnviDieselConsumption.unit_of_measurement).distinct().all()
        valid_units_set = {unit[0].strip() for unit in valid_units}

        # Fetch all company properties and build cp_name lookup
        company_properties = db.query(EnviCompanyProperty).all()
        cp_lookup = {}
        cp_name_set = set()
        for cp in company_properties:
            key = (cp.company_id.lower(), cp.cp_name.lower())
            cp_lookup[key] = cp.cp_id
            cp_name_set.add(cp.cp_name.lower())

        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number

            # Validate company_id
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id = row["company_id"].strip()
            if company_id not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            # Validate cp_name
            if not isinstance(row["cp_name"], str) or not row["cp_name"].strip():
                validation_errors.append(f"Row {row_number}: Invalid cp_name")
                continue

            cp_name = row["cp_name"].strip()
            cp_lookup_key = (company_id.lower(), cp_name.lower())

            # Validate if cp_name exists for the given company_id
            if cp_lookup_key not in cp_lookup:
                validation_errors.append(
                    f"Row {row_number}: Company property not found for company_id '{company_id}' and cp_name '{cp_name}'"
                )
                continue

            cp_id = cp_lookup[cp_lookup_key]

            # Validate unit_of_measurement
            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(
                    f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}"
                )
                continue

            # Validate consumption
            if not isinstance(row["consumption"], (int, float)) or row["consumption"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid consumption")
                continue

            # Validate and parse date
            try:
                if isinstance(row["date"], str):
                    parsed_date = pd.to_datetime(row["date"]).date()
                elif hasattr(row["date"], 'date'):
                    parsed_date = row["date"].date()
                elif isinstance(row["date"], datetime.date):
                    parsed_date = row["date"]
                else:
                    raise ValueError("Invalid date format")
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format")
                continue

            year = parsed_date.year

            rows.append({
                "company_id": company_id,
                "cp_id": cp_id,
                "unit_of_measurement": unit_stripped,
                "consumption": float(row["consumption"]),
                "date": parsed_date,
                "year": year
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_diesel_consumption(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Get valid units of measurement from database
        valid_units = db.query(EnviWaterAbstraction.unit_of_measurement).all()  # Adjust table/column names as needed
        valid_units_set = {unit[0] for unit in valid_units}
        
        # Define month to quarter mapping
        month_to_quarter = {
            "January": "Q1", "February": "Q1", "March": "Q1",
            "April": "Q2", "May": "Q2", "June": "Q2",
            "July": "Q3", "August": "Q3", "September": "Q3",
            "October": "Q4", "November": "Q4", "December": "Q4"
        }

        # data cleaning & row-level validation
        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year
        
        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)
            
            # Company ID validation
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            if row["month"] not in month_to_quarter.keys():
                validation_errors.append(f"Row {row_number}: Invalid month '{row['month']}'")
                continue

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{row['quarter']}'")
                continue

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid volume")
                continue

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            # NEW VALIDATION 1: Check if unit_of_measurement exists in database
            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            # NEW VALIDATION 2: Check if month and quarter match
            expected_quarter = month_to_quarter[row["month"]]
            if row["quarter"] != expected_quarter:
                validation_errors.append(f"Row {row_number}: Month '{row['month']}' should be in quarter '{expected_quarter}', but '{row['quarter']}' was provided")
                continue

            # If all validations pass, add to rows
            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "month": row["month"],
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": unit_stripped,
            })

        # If there are validation errors, return them
        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        # If no validation errors, proceed with bulk insert
        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_water_abstractions(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# WATER DISCHARGE BULK UPLOAD
@router.post("/bulk_upload_water_discharge")
def bulk_upload_water_discharge(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'water_discharge')

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'volume', 'unit_of_measurement'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Get valid units of measurement from database
        valid_units = db.query(EnviWaterDischarge.unit_of_measurement).all()  # Adjust table/column names as needed
        valid_units_set = {unit[0] for unit in valid_units}

        # data cleaning & row-level validation
        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year
        
        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)
            
            # Company ID validation
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{row['quarter']}'")
                continue

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid volume")
                continue

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            # NEW VALIDATION: Check if unit_of_measurement exists in database
            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            # If all validations pass, add to rows
            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": unit_stripped,
            })

        # If there are validation errors, return them
        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        # If no validation errors, proceed with bulk insert
        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_water_discharge(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# WATER CONSUMPTION BULK UPLOAD
@router.post("/bulk_upload_water_consumption")
def bulk_upload_water_consumption(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'water_consumption')

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'volume', 'unit_of_measurement'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Get valid units of measurement from database
        valid_units = db.query(EnviWaterConsumption.unit_of_measurement).all()  # Adjust table/column names as needed
        valid_units_set = {unit[0] for unit in valid_units}

        # data cleaning & row-level validation
        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year
        
        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)
            
            # Company ID validation
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{row['quarter']}'")
                continue

            if not isinstance(row["volume"], (int, float)) or row["volume"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid volume")
                continue

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            # NEW VALIDATION: Check if unit_of_measurement exists in database
            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            # If all validations pass, add to rows
            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "volume": float(row["volume"]),
                "unit_of_measurement": unit_stripped,
            })

        # If there are validation errors, return them
        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        # If no validation errors, proceed with bulk insert
        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_water_consumption(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ELECTRIC CONSUMPTION BULK UPLOAD
@router.post("/bulk_upload_electric_consumption")
def bulk_upload_electric_consumption(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk electric consumption data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'electric_consumption')

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'source', 'unit_of_measurement', 'consumption'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Get valid units of measurement from database
        valid_units = db.query(EnviElectricConsumption.unit_of_measurement).all()  # Adjust table/column names as needed
        valid_units_set = {unit[0] for unit in valid_units}
        
        # Get valid sources from database
        valid_sources = db.query(EnviElectricConsumption.source).all()  # Adjust table/column names as needed
        valid_sources_set = {source[0] for source in valid_sources}

        # data cleaning & row-level validation
        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year
        
        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)
            
            # Company ID validation
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            # Year validation
            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            # Quarter validation
            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{row['quarter']}'")
                continue

            # Source basic validation
            if not isinstance(row["source"], str) or not row["source"].strip():
                validation_errors.append(f"Row {row_number}: Invalid source")
                continue

            # Source database validation
            source_stripped = row["source"].strip()
            if source_stripped not in valid_sources_set:
                validation_errors.append(f"Row {row_number}: Source '{source_stripped}' does not exist in database. Valid sources: {', '.join(sorted(valid_sources_set))}")
                continue

            # Unit of measurement basic validation
            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            # Unit of measurement database validation
            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            # Consumption validation
            if not isinstance(row["consumption"], (int, float)) or row["consumption"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid consumption")
                continue

            # If all validations pass, add to rows
            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "source": source_stripped,
                "unit_of_measurement": unit_stripped,
                "consumption": float(row["consumption"]),
            })

        # If there are validation errors, return them
        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        # If no validation errors, proceed with bulk insert
        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_electric_consumption(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# NON-HAZARD WASTE BULK UPLOAD
@router.post("/bulk_upload_non_hazard_waste")
def bulk_upload_non_hazard_waste(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")

    try:
        logging.info("Add bulk non-hazard waste data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'non_hazard_waste')

        required_columns = {
            "company_id", "year", "month", "quarter",
            "metrics", "unit_of_measurement", "waste"
        }
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Pre-fetch valid entries from the database
        valid_units = {row[0] for row in db.query(EnviNonHazardWaste.unit_of_measurement).all()}
        valid_metrics = {row[0] for row in db.query(EnviNonHazardWaste.metrics).all()}

        month_to_quarter = {
            "January": "Q1", "February": "Q1", "March": "Q1",
            "April": "Q2", "May": "Q2", "June": "Q2",
            "July": "Q3", "August": "Q3", "September": "Q3",
            "October": "Q4", "November": "Q4", "December": "Q4"
        }

        CURRENT_YEAR = datetime.now().year
        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)

            company_id = str(row["company_id"]).strip()
            year = int(row["year"]) if isinstance(row["year"], (int, float)) else None
            month = row["month"]
            quarter = row["quarter"]
            metrics = str(row["metrics"]).strip()
            unit = str(row["unit_of_measurement"]).strip()
            waste = float(row["waste"]) if isinstance(row["waste"], (int, float)) else None

            # Company ID validation
            if not company_id:
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            if company_id not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            if year is None or not (1900 <= year <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            if month not in month_to_quarter:
                validation_errors.append(f"Row {row_number}: Invalid month '{month}'")
                continue

            if quarter not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{quarter}'")
                continue

            expected_quarter = month_to_quarter[month]
            if quarter != expected_quarter:
                validation_errors.append(f"Row {row_number}: Month '{month}' should be in quarter '{expected_quarter}', but '{quarter}' was provided")
                continue

            if not metrics:
                validation_errors.append(f"Row {row_number}: Invalid metrics")
                continue

            if metrics not in valid_metrics:
                validation_errors.append(f"Row {row_number}: Metrics '{metrics}' not found. Valid: {', '.join(sorted(valid_metrics))}")
                continue

            if not unit:
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            if unit not in valid_units:
                validation_errors.append(f"Row {row_number}: Unit '{unit}' not found. Valid: {', '.join(sorted(valid_units))}")
                continue

            if waste is None or waste < 0:
                validation_errors.append(f"Row {row_number}: Invalid waste")
                continue

            rows.append({
                "company_id": company_id,
                "year": year,
                "month": month,
                "quarter": quarter,
                "metrics": metrics,
                "unit_of_measurement": unit,
                "waste": waste,
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_non_hazard_waste(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# HAZARD WASTE GENERATED BULK UPLOAD
@router.post("/bulk_upload_hazard_waste_generated")
def bulk_upload_hazard_waste_generated(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk hazard waste generated data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'hazard_waste_generated')

        # basic validation...
        required_columns = {'company_id', 'year', 'quarter', 'metrics', 'unit_of_measurement', 'waste_generated'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Get valid units and metrics from database
        valid_units = db.query(EnviHazardWasteGenerated.unit_of_measurement).all()
        valid_units_set = {unit[0] for unit in valid_units}

        valid_metrics = db.query(EnviHazardWasteGenerated.metrics).all()
        valid_metrics_set = {metric[0] for metric in valid_metrics}

        # data cleaning & row-level validation
        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)

            # Company ID validation
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            if row["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
                validation_errors.append(f"Row {row_number}: Invalid quarter '{row['quarter']}'")
                continue

            if not isinstance(row["metrics"], str) or not row["metrics"].strip():
                validation_errors.append(f"Row {row_number}: Invalid metrics")
                continue

            metrics_stripped = row["metrics"].strip()
            if metrics_stripped not in valid_metrics_set:
                validation_errors.append(f"Row {row_number}: Metrics '{metrics_stripped}' does not exist in database. Valid metrics: {', '.join(sorted(valid_metrics_set))}")
                continue

            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue

            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            if not isinstance(row["waste_generated"], (int, float)) or row["waste_generated"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid waste_generated")
                continue

            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "quarter": row["quarter"],
                "metrics": metrics_stripped,
                "unit_of_measurement": unit_stripped,
                "waste_generated": float(row["waste_generated"]),
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

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
        logging.info("Add bulk hazard waste disposed data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))
        df = normalize_dataframe_columns(df, 'hazard_waste_disposed')

        # basic validation...
        required_columns = {'company_id', 'year', 'metrics', 'unit_of_measurement', 'waste_disposed'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

        # Get valid company IDs from CompanyMain
        valid_company_ids = db.query(CompanyMain.company_id).all()
        valid_company_ids_set = {company_id[0] for company_id in valid_company_ids}

        # Fetch valid units and metrics from database
        valid_units = db.query(EnviHazardWasteDisposed.unit_of_measurement).distinct().all()
        valid_metrics = db.query(EnviHazardWasteDisposed.metrics).distinct().all()

        valid_units_set = {unit[0] for unit in valid_units}
        valid_metrics_set = {metric[0] for metric in valid_metrics}

        rows = []
        validation_errors = []
        CURRENT_YEAR = datetime.now().year

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number (accounting for header)

            # Validate company_id
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
                continue

            company_id_stripped = row["company_id"].strip()
            if company_id_stripped not in valid_company_ids_set:
                validation_errors.append(f"Row {row_number}: Company ID '{company_id_stripped}' does not exist in CompanyMain. Valid company IDs: {', '.join(sorted(valid_company_ids_set))}")
                continue

            # Validate year
            if not isinstance(row["year"], (int, float)) or not (1900 <= int(row["year"]) <= CURRENT_YEAR + 1):
                validation_errors.append(f"Row {row_number}: Invalid year")
                continue

            # Validate metrics
            if not isinstance(row["metrics"], str) or not row["metrics"].strip():
                validation_errors.append(f"Row {row_number}: Invalid metrics")
                continue
            metric_stripped = row["metrics"].strip()
            if metric_stripped not in valid_metrics_set:
                validation_errors.append(f"Row {row_number}: Metric '{metric_stripped}' does not exist in database. Valid metrics: {', '.join(sorted(valid_metrics_set))}")
                continue

            # Validate unit_of_measurement
            if not isinstance(row["unit_of_measurement"], str) or not row["unit_of_measurement"].strip():
                validation_errors.append(f"Row {row_number}: Invalid unit_of_measurement")
                continue
            unit_stripped = row["unit_of_measurement"].strip()
            if unit_stripped not in valid_units_set:
                validation_errors.append(f"Row {row_number}: Unit of measurement '{unit_stripped}' does not exist in database. Valid units: {', '.join(sorted(valid_units_set))}")
                continue

            # Validate waste_disposed
            if not isinstance(row["waste_disposed"], (int, float)) or row["waste_disposed"] < 0:
                validation_errors.append(f"Row {row_number}: Invalid waste_disposed")
                continue

            # If all validations pass, add to rows
            rows.append({
                "company_id": company_id_stripped,
                "year": int(row["year"]),
                "metrics": metric_stripped,
                "unit_of_measurement": unit_stripped,
                "waste_disposed": float(row["waste_disposed"]),
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = bulk_create_hazard_waste_disposed(db, rows)
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
@router.get("/distinct_cp_names/{company_name}", response_model=List[dict])
def get_distinct_cp_names(company_name: str, db: Session = Depends(get_db)):
    try:
        logging.info(f"Fetching distinct cp_id, cp_name for company_name: {company_name}")

        query = text("""
            SELECT DISTINCT ecp.cp_id, ecp.cp_name
            FROM silver.envi_company_property AS ecp
            JOIN ref.company_main AS cm ON cm.company_id = ecp.company_id
            WHERE cm.company_name = :company_name
        """)

        result = db.execute(query, {"company_name": company_name})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} entries for company_name: {company_name}")
        return data

    except Exception as e:
        logging.error(f"Error retrieving cp_id and cp_name values: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/distinct_cp_type", response_model=List[dict])
def get_distinct_cp_type(db: Session = Depends(get_db)):
    try:
        logging.info("Fetching distinct cp_id, cp_name values from envi_company_property")

        query = text("""
            SELECT DISTINCT cp_type FROM silver.envi_company_property
        """)

        result = db.execute(query)
        data = [{"cp_type": row.cp_type} for row in result]

        logging.info(f"Returned {len(data)} distinct cp_type")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct cp_type: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/distinct_diesel_consumption_unit", response_model=List[Dict[str, str]])
def get_distinct_diesel_consumption_unit(db: Session = Depends(get_db)):
    """
    Fetch distinct 'unit' from the envi_non_hazard_waste table.
    """
    try:
        logging.info("Fetching distinct unit from bronze.envi_diesel_consumption")

        query = text("""
            SELECT DISTINCT trim(unit_of_measurement) as unit FROM bronze.envi_diesel_consumption
        """)

        result = db.execute(query)
        data = [{"unit": row.unit} for row in result]

        logging.info(f"Returned {len(data)} distinct unit")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct unit: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/distinct_water_unit", response_model=List[Dict[str, str]])
def get_distinct_water_unit(db: Session = Depends(get_db)):
    """
    Fetch distinct 'unit' from the envi_non_hazard_waste table.
    """
    try:
        logging.info("Fetching distinct unit from bronze.envi_water_abstraction")

        query = text("""
            SELECT DISTINCT trim(unit_of_measurement) as unit FROM bronze.envi_water_abstraction
        """)

        result = db.execute(query)
        data = [{"unit": row.unit} for row in result]

        logging.info(f"Returned {len(data)} distinct unit")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct unit: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/distinct_haz_waste_generated", response_model=List[Dict[str, str]])
def get_distinct_haz_waste_generated(db: Session = Depends(get_db)):
    """
    Fetch distinct 'metrics' from the envi_hazard_waste_generated table.
    """
    try:
        logging.info("Fetching distinct metrics from silver.envi_hazard_waste_generated")

        query = text("""
            SELECT DISTINCT metrics FROM silver.envi_hazard_waste_generated
        """)

        result = db.execute(query)
        data = [{"metrics": row.metrics} for row in result]

        logging.info(f"Returned {len(data)} distinct metrics")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct metrics: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/distinct_hazard_waste_gen_unit", response_model=List[Dict[str, str]])
def get_distinct_hazard_waste_gen_unit(
    metrics: Optional[str] = Query(None, description="Filter by specific metrics"),
    db: Session = Depends(get_db)
):
    """
    Fetch distinct 'unit' from the envi_hazard_waste_generated table.
    Optionally filter by 'metrics'.
    """
    try:
        logging.info("Fetching distinct unit from bronze.envi_hazard_waste_generated")

        if metrics:
            query = text("""
                SELECT DISTINCT trim(unit_of_measurement) as unit 
                FROM bronze.envi_hazard_waste_generated
                WHERE trim(metrics) = :metrics
            """)
            result = db.execute(query, {"metrics": metrics.strip()})
        else:
            query = text("""
                SELECT DISTINCT trim(unit_of_measurement) as unit 
                FROM bronze.envi_hazard_waste_generated
            """)
            result = db.execute(query)

        data = [{"unit": row.unit} for row in result]

        logging.info(f"Returned {len(data)} distinct unit(s)")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct unit: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/distinct_haz_waste_disposed", response_model=List[Dict[str, str]])
def get_distinct_haz_waste_disposed(db: Session = Depends(get_db)):
    """
    Fetch distinct 'metrics' from the envi_hazard_waste_disposed table.
    """
    try:
        logging.info("Fetching distinct metrics from silver.envi_hazard_waste_disposed")

        query = text("""
            SELECT DISTINCT metrics FROM silver.envi_hazard_waste_disposed
        """)

        result = db.execute(query)
        data = [{"metrics": row.metrics} for row in result]

        logging.info(f"Returned {len(data)} distinct metrics")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct metrics: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/distinct_hazard_waste_dis_unit", response_model=List[Dict[str, str]])
def get_distinct_hazard_waste_dis_unit(
    metrics: Optional[str] = Query(None, description="Filter by specific metrics"),
    db: Session = Depends(get_db)
):
    """
    Fetch distinct 'unit' from the envi_hazard_waste_disposed table.
    Optionally filter by 'metrics'.
    """
    try:
        logging.info("Fetching distinct unit from bronze.envi_hazard_waste_disposed")

        if metrics:
            query = text("""
                SELECT DISTINCT trim(unit_of_measurement) AS unit 
                FROM bronze.envi_hazard_waste_disposed
                WHERE trim(metrics) = :metrics
            """)
            result = db.execute(query, {"metrics": metrics.strip()})
        else:
            query = text("""
                SELECT DISTINCT trim(unit_of_measurement) AS unit 
                FROM bronze.envi_hazard_waste_disposed
            """)
            result = db.execute(query)

        data = [{"unit": row.unit} for row in result]

        logging.info(f"Returned {len(data)} distinct unit(s)")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct unit: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/distinct_non_haz_waste_metrics", response_model=List[Dict[str, str]])
def get_distinct_non_haz_waste_metrics(db: Session = Depends(get_db)):
    """
    Fetch distinct 'metrics' from the envi_non_hazard_waste table.
    """
    try:
        logging.info("Fetching distinct metrics from silver.envi_non_hazard_waste")

        query = text("""
            SELECT DISTINCT metrics FROM silver.envi_non_hazard_waste
        """)

        result = db.execute(query)
        data = [{"metrics": row.metrics} for row in result]

        logging.info(f"Returned {len(data)} distinct metrics")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct metrics: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/distinct_non_haz_waste_unit", response_model=List[Dict[str, str]])
def get_distinct_non_haz_waste_unit(
    metrics: Optional[str] = Query(None, description="Filter by specific metrics"),
    db: Session = Depends(get_db)
):
    """
    Fetch distinct 'unit' from the envi_non_hazard_waste table.
    Optionally filter by 'metrics'.
    """
    try:
        logging.info("Fetching distinct unit from bronze.envi_non_hazard_waste")

        if metrics:
            query = text("""
                SELECT DISTINCT trim(unit_of_measurement) AS unit 
                FROM bronze.envi_non_hazard_waste
                WHERE trim(metrics) = :metrics
            """)
            result = db.execute(query, {"metrics": metrics.strip()})
        else:
            query = text("""
                SELECT DISTINCT trim(unit_of_measurement) AS unit 
                FROM bronze.envi_non_hazard_waste
            """)
            result = db.execute(query)

        data = [{"unit": row.unit} for row in result]

        logging.info(f"Returned {len(data)} distinct unit(s)")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct unit: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/distinct_electric_source", response_model=List[Dict[str, str]])
def get_distinct_electric_source(db: Session = Depends(get_db)):
    """
    Fetch distinct 'source' from the envi_electric_consumption table.
    """
    try:
        logging.info("Fetching distinct source from silver.envi_electric_consumption")

        query = text("""
            SELECT DISTINCT source FROM silver.envi_electric_consumption
        """)

        result = db.execute(query)
        data = [{"source": row.source} for row in result]

        logging.info(f"Returned {len(data)} distinct source")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct source: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/distinct_electric_consumption_unit", response_model=List[Dict[str, str]])
def get_distinct_electric_consumption_unit(db: Session = Depends(get_db)):
    """
    Fetch distinct 'unit' from the envi_non_hazard_waste table.
    """
    try:
        logging.info("Fetching distinct unit from bronze.envi_electric_consumption")

        query = text("""
            SELECT DISTINCT trim(unit_of_measurement) as unit FROM bronze.envi_electric_consumption
        """)

        result = db.execute(query)
        data = [{"unit": row.unit} for row in result]

        logging.info(f"Returned {len(data)} distinct unit")
        return data

    except Exception as e:
        logging.error(f"Error retrieving distinct unit: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

# for experorting data to Excel
@router.post("/export_excel")
async def export_excel(request: Request):
    data = await request.json()
    
    # Validate that data is a list of dictionaries naming convention for non-technical users
    column_mapping = {
    
    'cp_id': 'Company Property ID',
    'wa_id': 'Water Abstraction ID',
    'wd_id': 'Water Discharge ID',
    'wc_id': 'Water Consumption ID',
    'dc_id': 'Diesel Consumption ID',
    'ec_id': 'Electric Consumption ID',
    'nhw_id': 'Non-Hazardous Waste ID',
    'hwg_id': 'Hazardous Waste Generated ID',
    'hwd_id': 'Hazardous Waste Disposed ID',
    'company': 'Company',
    'source': 'Source',
    'property': 'Property',
    'type': 'Type',
    'unit': 'Unit',
    'consumption': 'Consumption',
    'month': 'Month',
    'quarter': 'Quarter',
    'year': 'Year',
    'volume': 'Volume',
    'date': 'Date',
    'status': 'Status',
    'metrics': 'Metrics',
    'waste': 'Waste',
}
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)
    df = df.rename(columns=column_mapping)

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
        required_fields = ['company', 'year', 'month', 'quarter', 'volume', 'unit']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["month"] not in [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]:
            raise HTTPException(status_code=422, detail=f"Invalid month '{data['month']}'")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        # Validate quarter-month consistency
        quarter_months = {
            "Q1": ["January", "February", "March"],
            "Q2": ["April", "May", "June"],
            "Q3": ["July", "August", "September"],
            "Q4": ["October", "November", "December"]
        }
        
        if data["month"] not in quarter_months[data["quarter"]]:
            raise HTTPException(
                status_code=422, 
                detail=f"Month '{data['month']}' does not belong to quarter '{data['quarter']}'. "
                       f"Valid months for {data['quarter']} are: {', '.join(quarter_months[data['quarter']])}"
            )

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        wa_id = data["wa_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "month": data["month"],
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit"].strip(),
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
        required_fields = ['company', 'year', 'quarter', 'volume', 'unit']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        wd_id = data["wd_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit"].strip(),
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
        required_fields = ['company', 'year', 'quarter', 'volume', 'unit']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["volume"], (int, float)) or data["volume"] < 0:
            raise HTTPException(status_code=422, detail="Invalid volume")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        wc_id = data["wc_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "volume": float(data["volume"]),
            "unit_of_measurement": data["unit"].strip(),
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
        required_fields = ['company', 'year', 'quarter', 'source', 'unit', 'consumption']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["source"], str) or not data["source"].strip():
            raise HTTPException(status_code=422, detail="Invalid source")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit")

        if not isinstance(data["consumption"], (int, float)) or data["consumption"] < 0:
            raise HTTPException(status_code=422, detail="Invalid consumption")

        ec_id = data["ec_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "source": data["source"].strip(),
            "unit_of_measurement": data["unit"].strip(),
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
        required_fields = ['company', 'year', 'quarter', 'metrics', 'unit', 'waste']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste"], (int, float)) or data["waste"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste")

        nhw_id = data["nhw_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit"].strip(),
            "waste": float(data["waste"]),  # Map 'waste' to 'waste_generated'
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
        required_fields = ['company', 'year', 'quarter', 'metrics', 'unit', 'waste']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if data["quarter"] not in {"Q1", "Q2", "Q3", "Q4"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['quarter']}'")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste"], (int, float)) or data["waste"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste_generated")

        hwg_id = data["hwg_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "quarter": data["quarter"],
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit"].strip(),
            "waste_generated": float(data["waste"]),
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
        required_fields = ['company', 'year', 'metrics', 'unit', 'waste']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        if not isinstance(data["year"], (int, float)) or not (1900 <= int(data["year"]) <= datetime.now().year + 1):
            raise HTTPException(status_code=422, detail="Invalid year")

        if not isinstance(data["metrics"], str) or not data["metrics"].strip():
            raise HTTPException(status_code=422, detail="Invalid metrics")

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit_of_measurement")

        if not isinstance(data["waste"], (int, float)) or data["waste"] < 0:
            raise HTTPException(status_code=422, detail="Invalid waste_disposed")

        hwd_id = data["hwd_id"]
        record_data = {
            "company_id": company_id,
            "year": int(data["year"]),
            "metrics": data["metrics"].strip(),
            "unit_of_measurement": data["unit"].strip(),
            "waste_disposed": float(data["waste"]),
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

        # Expect company_name instead of company_id
        required_fields = ['company', 'property', 'unit', 'consumption', 'date']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company"], str) or not data["company"].strip():
            raise HTTPException(status_code=422, detail="Invalid company name")

        if not isinstance(data["property"], str) or not data["property"].strip():
            raise HTTPException(status_code=422, detail="Invalid cp_name")

        # Look up company_id using company_name
        company_name = data["company"].strip()
        company = db.query(CompanyMain).filter(CompanyMain.company_name.ilike(company_name)).first()

        if not company:
            raise HTTPException(
                status_code=422,
                detail=f"Company not found with name '{company_name}'"
            )

        company_id = company.company_id

        # Look up cp_id using company_id and cp_name (case-insensitive)
        cp_name = data["property"].strip()
        cp_lookup_key = (company_id.lower(), cp_name.lower())
        
        company_properties = db.query(EnviCompanyProperty).all()
        cp_lookup = {(cp.company_id.lower(), cp.cp_name.lower()): cp.cp_id for cp in company_properties}
        
        if cp_lookup_key not in cp_lookup:
            raise HTTPException(
                status_code=422, 
                detail=f"Company property not found for company '{company_id}' and property '{cp_name}'"
            )
        
        cp_id = cp_lookup[cp_lookup_key]

        if not isinstance(data["unit"], str) or not data["unit"].strip():
            raise HTTPException(status_code=422, detail="Invalid unit")

        if not isinstance(data["consumption"], (int, float)) or data["consumption"] < 0:
            raise HTTPException(status_code=422, detail="Invalid consumption")

        # Validate date
        try:
            if isinstance(data["date"], str):
                parsed_date = pd.to_datetime(data["date"]).date()
            elif hasattr(data["date"], 'date'):
                parsed_date = data["date"].date()
            elif isinstance(data["date"], datetime.date):
                parsed_date = data["date"]
            else:
                raise ValueError("Invalid date format")
        except (ValueError, TypeError):
            raise HTTPException(status_code=422, detail="Invalid date format")

        year = parsed_date.year
        dc_id = data["dc_id"]

        record_data = {
            "company_id": company_id,
            "cp_id": cp_id,
            "unit_of_measurement": data["unit"].strip(),
            "consumption": float(data["consumption"]),
            "date": parsed_date,
            "year": year
        }

        update_diesel_consumption(db, dc_id, record_data)
        return {"message": "Diesel consumption record successfully updated."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))