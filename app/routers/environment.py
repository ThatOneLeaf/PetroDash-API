from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
import logging
import traceback
from fastapi.responses import StreamingResponse
import io
from datetime import datetime
from typing import Optional, List
from app.dependencies import get_db
from app.crud.base import get_all, get_many_filtered, get_one
from app.bronze.crud import (
    EnviWaterAbstraction, 
    EnviWaterDischarge, 
    EnviWaterConsumption
)
from app.bronze.schemas import (
    EnviWaterAbstractionOut,
    EnviWaterDischargeOut,
    EnviWaterConsumptionOut
)
from ..dependencies import get_db
from ..bronze.models import TableType
from ..template.envi_template_config import TEMPLATE_DEFINITIONS
from ..utils.envi_template_utils import create_excel_template, create_all_templates, get_table_mapping

router = APIRouter()

#=================RETRIEVE ALL ENVIRONMENTAL DATA=================
@router.get("/water_abstraction", response_model=List[Dict])
def get_water_abstraction(db: Session = Depends(get_db)):
    """
    Get water abstraction data from vw_environment_water_abstraction view
    Joins with status and company info, with 'HEAD APPROVED' as default status if null
    """
    try:
        logging.info("Executing water abstraction query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No water abstraction data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching water abstraction data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/water_discharge", response_model=List[Dict])
def get_water_discharge(db: Session = Depends(get_db)):
    """
    Get water discharge data from vw_environment_water_discharge view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing water discharge query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No water discharge data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching water discharge data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/water_consumption", response_model=List[Dict])
def get_water_consumption(db: Session = Depends(get_db)):
    """
    Get water consumption data from vw_environment_water_consumption view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing water consumption query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No water consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching water consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/diesel_consumption", response_model=List[Dict])
def get_diesel_consumption(db: Session = Depends(get_db)):
    """
    Get diesel consumption data from vw_environment_diesel_consumption view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing diesel consumption query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No diesel consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching diesel consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/electric_consumption", response_model=List[Dict])
def get_electric_consumption(db: Session = Depends(get_db)):
    """
    Get electric consumption data from vw_environment_electric_consumption view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing electric consumption query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No electric consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching electric consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/non_hazard_waste", response_model=List[Dict])
def get_non_hazard_waste(db: Session = Depends(get_db)):
    """
    Get non hazard waste data from vw_environment_non_hazard_waste view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing non hazard waste consumption query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No non hazard waste consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching non hazard waste consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/hazard_waste_generated", response_model=List[Dict])
def get_hazard_waste_generated(db: Session = Depends(get_db)):
    """
    Get hazard waste generated data from vw_environment_hazard_waste_generated view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing hazard waste generated waste consumption query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No hazard waste generated waste consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching hazard waste generated waste consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/hazard_waste_disposed", response_model=List[Dict])
def get_hazard_waste_disposed(db: Session = Depends(get_db)):
    """
    Get hazard waste disposed data from vw_environment_hazard_waste_disposed view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing hazard waste disposed waste consumption query")
        
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
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No hazard waste disposed waste consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching hazard waste disposed waste consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

#=================RETRIEVE ENVIRONMENTAL DATA BY ID=================
@router.get("/{wa_id}", response_model=EnviWaterAbstractionOut)
def get_water_abstraction_by_id(wa_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviWaterAbstraction, "wa_id", wa_id)
    if not record:
        raise HTTPException(status_code=404, detail="Water Abstraction record not found")
    return record

@router.get("/{wd_id}", response_model=EnviWaterDischargeOut)
def get_water_abstraction_by_id(wd_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviWaterDischarge, "wd_id", wd_id)
    if not record:
        raise HTTPException(status_code=404, detail="Water Discharge record not found")
    return record

@router.get("/{wc_id}", response_model=EnviWaterConsumptionOut)
def get_water_abstraction_by_id(wc_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnviWaterConsumption, "wc_id", wc_id)
    if not record:
        raise HTTPException(status_code=404, detail="Water Consumption record not found")
    return record
    
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