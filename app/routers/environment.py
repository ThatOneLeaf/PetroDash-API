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
    get_all_water_abstraction,
    get_filtered_water_abstraction,
    get_all_water_discharge,
    get_filtered_water_discharge,
    get_all_water_consumption,
    get_filtered_water_consumption
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

# ============== EXISTING ENDPOINTS ==============
@router.get("/water_abstraction", response_model=List[EnviWaterAbstractionOut])
def get_water_abstraction(
    company_name: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    filters = {}
    if company_name:
        filters["company_name"] = company_name
    if year:
        filters["year"] = year

    if filters:
        return get_filtered_water_abstraction(db, filters=filters, skip=skip, limit=limit)
    return get_all_water_abstraction(db)


@router.get("/water_discharge", response_model=List[EnviWaterAbstractionOut])
def get_water_discharge(
    company_name: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    filters = {}
    if company_name:
        filters["company_name"] = company_name
    if year:
        filters["year"] = year

    if filters:
        return get_filtered_water_discharge(db, filters=filters, skip=skip, limit=limit)
    return get_all_water_discharge(db)


@router.get("/water_consumption", response_model=List[EnviWaterConsumptionOut])
def get_water_consumption(
    company_name: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    db: Session = Depends(get_db),
    skip: int = 0,
    limit: int = 100
):
    filters = {}
    if company_name:
        filters["company_name"] = company_name
    if year:
        filters["year"] = year

    if filters:
        return get_filtered_water_consumption(db, filters=filters, skip=skip, limit=limit)
    return get_all_water_consumption(db)

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