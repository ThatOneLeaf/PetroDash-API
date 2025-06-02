from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
import logging
import traceback
from fastapi.responses import StreamingResponse
import io
from datetime import datetime

from ..dependencies import get_db
from ..bronze.models import TableType
from ..template.envi_template_config import TEMPLATE_DEFINITIONS
from ..utils.envi_template_utils import create_excel_template, create_all_templates, get_table_mapping

router = APIRouter()

# ============== EXISTING ENDPOINTS ==============

@router.get("/water_abstraction", response_model=List[Dict])
def get_water_abstraction(db: Session = Depends(get_db)):
    """
    Get water abstraction data from vw_environment_water_abstraction view
    Joins with status and company info, with 'HEAD APPROVED' as default status if null
    """
    try:
        logging.info("Executing water abstraction query")
        
        result = db.execute(text("""
            SELECT
                ewa.wa_id,
                cm.company_name,
                ewa.volume,
                ewa.unit AS metrics,
                ewa.quarter,
                ewa.year,
                COALESCE(s.status_name, 'HEAD APPROVED') AS status_name
            FROM gold.vw_environment_water_abstraction ewa
            LEFT JOIN public.checker_status_log csl ON ewa.wa_id = csl.record_id
            LEFT JOIN public.status s ON csl.status_id = s.status_id
            LEFT JOIN ref.company_main cm ON ewa.company_id = cm.company_id
            ORDER BY ewa.year DESC
        """))

        data = [
            {
                "waId": row.wa_id,
                "companyName": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "metrics": row.metrics,
                "quarter": row.quarter,
                "year": row.year,
                "statusName": row.status_name
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
        
        result = db.execute(text("""
            SELECT
                ewd.wd_id,
                cm.company_name,
                ewd.volume,
                ewd.unit AS metrics,
                ewd.quarter,
                ewd.year,
                COALESCE(s.status_name, 'Head Approved') AS status_name
            FROM gold.vw_environment_water_discharge ewd
            LEFT JOIN public.checker_status_log csl ON ewd.wd_id = csl.record_id
            LEFT JOIN public.status s ON csl.status_id = s.status_id
            LEFT JOIN ref.company_main cm ON ewd.company_id = cm.company_id
            ORDER BY ewd.year DESC
        """))

        data = [
            {
                "wdId": row.wd_id,
                "companyName": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "metrics": row.metrics,
                "quarter": row.quarter,
                "year": row.year,
                "statusName": row.status_name
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
        
        result = db.execute(text("""
            SELECT
                ewc.wc_id,
                cm.company_name,
                ewc.volume,
                ewc.unit AS metrics,
                ewc.quarter,
                ewc.year,
                COALESCE(s.status_name, 'Head Approved') AS status_name
            FROM gold.vw_environment_water_consumption ewc
            LEFT JOIN public.checker_status_log csl ON ewc.wc_id = csl.record_id
            LEFT JOIN public.status s ON csl.status_id = s.status_id
            LEFT JOIN ref.company_main cm ON ewc.company_id = cm.company_id
            ORDER BY ewc.year DESC
        """))

        data = [
            {
                "wcId": row.wc_id,
                "companyName": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "metrics": row.metrics,
                "quarter": row.quarter,
                "year": row.year,
                "statusName": row.status_name
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