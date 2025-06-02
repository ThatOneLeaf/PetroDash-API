from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional
from decimal import Decimal
import logging
import traceback
from fastapi import APIRouter, Response
from fastapi.responses import StreamingResponse
import pandas as pd
from io import BytesIO
import io
from datetime import datetime
from enum import Enum

from ..dependencies import get_db

router = APIRouter()

# Excel Template Classes and Definitions
class TableType(str, Enum):
    COMPANY_PROPERTY = "company_property"
    WATER_ABSTRACTION = "water_abstraction"
    WATER_DISCHARGE = "water_discharge"
    WATER_CONSUMPTION = "water_consumption"
    DIESEL_CONSUMPTION = "diesel_consumption"
    ELECTRIC_CONSUMPTION = "electric_consumption"
    NON_HAZARD_WASTE = "non_hazard_waste"
    HAZARD_WASTE_GENERATED = "hazard_waste_generated"
    HAZARD_WASTE_DISPOSED = "hazard_waste_disposed"
    ALL = "all"

# Template definitions with headers, data types, and example values
TEMPLATE_DEFINITIONS = {
    "company_property": {
        "filename": "company_property_template.xlsx",
        "sheet_name": "Company Property",
        "headers": ["cp_id", "company_id", "cp_name", "cp_type"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "VARCHAR(30)", "VARCHAR(15)"],
        "examples": ["CP-PSC-001", "PSC001", "Generator Set 1", "Equipment"],
        "descriptions": [
            "Unique Company Property ID",
            "Referenced Company ID",
            "Property Name",
            "Type: Equipment or Vehicle"
        ]
    },
    "water_abstraction": {
        "filename": "water_abstraction_template.xlsx",
        "sheet_name": "Water Abstraction",
        "headers": ["wa_id", "company_id", "year", "month", "quarter", "volume", "unit_of_measurement"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "SMALLINT", "VARCHAR(10)", "VARCHAR(2)", "DOUBLE", "VARCHAR(15)"],
        "examples": ["WA-PSC-2024-001", "PSC001", "2024", "January", "Q1", "1250.50", "Liters"],
        "descriptions": [
            "Unique Water Abstraction ID",
            "Referenced Company ID",
            "Year (YYYY)",
            "Month name",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Volume (decimal allowed)",
            "Unit of measurement"
        ]
    },
    "water_discharge": {
        "filename": "water_discharge_template.xlsx",
        "sheet_name": "Water Discharge",
        "headers": ["wd_id", "company_id", "year", "quarter", "volume", "unit_of_measurement"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "SMALLINT", "VARCHAR(2)", "DOUBLE", "VARCHAR(15)"],
        "examples": ["WD-PSC-2024-001", "PSC001", "2024", "Q1", "980.25", "Liters"],
        "descriptions": [
            "Unique Water Discharge ID",
            "Referenced Company ID",
            "Year (YYYY)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Volume (decimal allowed)",
            "Unit of measurement"
        ]
    },
    "water_consumption": {
        "filename": "water_consumption_template.xlsx",
        "sheet_name": "Water Consumption",
        "headers": ["wc_id", "company_id", "year", "quarter", "volume", "unit_of_measurement"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "SMALLINT", "VARCHAR(2)", "DOUBLE", "VARCHAR(15)"],
        "examples": ["WC-PSC-2024-001", "PSC001", "2024", "Q1", "1500.75", "Liters"],
        "descriptions": [
            "Unique Water Consumption ID",
            "Referenced Company ID",
            "Year (YYYY)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Volume (decimal allowed)",
            "Unit of measurement"
        ]
    },
    "diesel_consumption": {
        "filename": "diesel_consumption_template.xlsx",
        "sheet_name": "Diesel Consumption",
        "headers": ["dc_id", "company_id", "cp_id", "unit_of_measurement", "consumption", "date"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "VARCHAR(20)", "VARCHAR(15)", "DOUBLE", "DATE"],
        "examples": ["DC-PSC-2024-001", "PSC001", "CP-PSC-001", "Liters", "234.789", "2024-01-15"],
        "descriptions": [
            "Unique Diesel Consumption ID",
            "Referenced Company ID",
            "Referenced Company Property ID",
            "Unit of measurement",
            "Consumption amount (decimal allowed)",
            "Date (YYYY-MM-DD)"
        ]
    },
    "electric_consumption": {
        "filename": "electric_consumption_template.xlsx",
        "sheet_name": "Electric Consumption",
        "headers": ["ec_id", "company_id", "source", "unit_of_measurement", "consumption", "quarter", "year"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "VARCHAR(20)", "VARCHAR(15)", "DOUBLE", "VARCHAR(2)", "SMALLINT"],
        "examples": ["EC-PSC-2024-001", "PSC001", "Logistics Station", "kWh", "1234.56", "Q1", "2024"],
        "descriptions": [
            "Unique Electric Consumption ID",
            "Referenced Company ID",
            "Source location",
            "Unit of measurement",
            "Consumption amount (decimal allowed)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Year (YYYY)"
        ]
    },
    "non_hazard_waste": {
        "filename": "non_hazard_waste_template.xlsx",
        "sheet_name": "Non-Hazard Waste",
        "headers": ["nhw_id", "company_id", "metrics", "unit_of_measurement", "waste", "month", "quarter", "year"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "VARCHAR(50)", "VARCHAR(15)", "DOUBLE", "VARCHAR(10)", "VARCHAR(2)", "SMALLINT"],
        "examples": ["NHW-PSC-2024-001", "PSC001", "Recyclable Paper", "Kilograms", "125.50", "January", "Q1", "2024"],
        "descriptions": [
            "Unique Non-Hazard Waste ID",
            "Referenced Company ID",
            "Waste metrics/type",
            "Unit of measurement",
            "Waste amount (decimal allowed)",
            "Month name",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Year (YYYY)"
        ]
    },
    "hazard_waste_generated": {
        "filename": "hazard_waste_generated_template.xlsx",
        "sheet_name": "Hazard Waste Generated",
        "headers": ["hwg_id", "company_id", "metrics", "unit_of_measurement", "waste_generated", "quarter", "year"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "VARCHAR(50)", "VARCHAR(15)", "DOUBLE", "VARCHAR(2)", "SMALLINT"],
        "examples": ["HWG-PSC-2024-001", "PSC001", "Used Oil", "Liters", "89.25", "Q1", "2024"],
        "descriptions": [
            "Unique Hazard Waste Generated ID",
            "Referenced Company ID",
            "Waste metrics/type",
            "Unit of measurement",
            "Waste generated amount (decimal allowed)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Year (YYYY)"
        ]
    },
    "hazard_waste_disposed": {
        "filename": "hazard_waste_disposed_template.xlsx",
        "sheet_name": "Hazard Waste Disposed",
        "headers": ["hwd_id", "company_id", "metrics", "unit_of_measurement", "waste_disposed", "year"],
        "data_types": ["VARCHAR(20)", "VARCHAR(10)", "VARCHAR(50)", "VARCHAR(15)", "DOUBLE", "SMALLINT"],
        "examples": ["HWD-PSC-2024-001", "PSC001", "Chemical Waste", "Kilograms", "45.80", "2024"],
        "descriptions": [
            "Unique Hazard Waste Disposed ID",
            "Referenced Company ID",
            "Waste metrics/type",
            "Unit of measurement",
            "Waste disposed amount (decimal allowed)",
            "Year (YYYY)"
        ]
    }
}

def create_excel_template(table_type: str, include_examples: bool = True) -> io.BytesIO:
    """Create Excel template for a specific table type"""
    if table_type not in TEMPLATE_DEFINITIONS:
        raise ValueError(f"Unknown table type: {table_type}")
    
    template = TEMPLATE_DEFINITIONS[table_type]
    
    # Create Excel writer object
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Create main data sheet
        df_data = pd.DataFrame(columns=template["headers"])
        
        # Add example row if requested
        if include_examples:
            example_row = {header: example for header, example in zip(template["headers"], template["examples"])}
            df_data = pd.concat([df_data, pd.DataFrame([example_row])], ignore_index=True)
        
        df_data.to_excel(writer, sheet_name=template["sheet_name"], index=False)
        
        # Create instructions sheet
        instructions_data = {
            "Column": template["headers"],
            "Data Type": template["data_types"],
            "Description": template["descriptions"],
            "Example": template["examples"]
        }
        df_instructions = pd.DataFrame(instructions_data)
        df_instructions.to_excel(writer, sheet_name="Instructions", index=False)
        
        # Format the sheets
        workbook = writer.book
        
        # Format main sheet
        main_sheet = writer.sheets[template["sheet_name"]]
        for col in range(len(template["headers"])):
            main_sheet.column_dimensions[chr(65 + col)].width = 20
        
        # Format instructions sheet
        instructions_sheet = writer.sheets["Instructions"]
        for col in range(4):  # 4 columns in instructions
            instructions_sheet.column_dimensions[chr(65 + col)].width = 25
    
    output.seek(0)
    return output

def create_all_templates() -> io.BytesIO:
    """Create a single Excel file with all templates as separate sheets"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Create overview sheet
        overview_data = {
            "Sheet Name": [],
            "Table Name": [],
            "Description": [],
            "Record Count Columns": []
        }
        
        for table_type, template in TEMPLATE_DEFINITIONS.items():
            overview_data["Sheet Name"].append(template["sheet_name"])
            overview_data["Table Name"].append(f"bronze.envi_{table_type}")
            overview_data["Description"].append(f"Template for {table_type.replace('_', ' ').title()}")
            overview_data["Record Count Columns"].append(len(template["headers"]))
            
            # Create individual template sheet
            df_data = pd.DataFrame(columns=template["headers"])
            
            # Add example row
            example_row = {header: example for header, example in zip(template["headers"], template["examples"])}
            df_data = pd.concat([df_data, pd.DataFrame([example_row])], ignore_index=True)
            
            df_data.to_excel(writer, sheet_name=template["sheet_name"], index=False)
            
            # Format sheet
            sheet = writer.sheets[template["sheet_name"]]
            for col in range(len(template["headers"])):
                sheet.column_dimensions[chr(65 + col)].width = 20
        
        # Add overview sheet
        df_overview = pd.DataFrame(overview_data)
        df_overview.to_excel(writer, sheet_name="Overview", index=False)
        
        # Format overview sheet
        overview_sheet = writer.sheets["Overview"]
        for col in range(4):
            overview_sheet.column_dimensions[chr(65 + col)].width = 25
    
    output.seek(0)
    return output

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
    # Map table names to template keys
    table_mapping = {
        "envi_company_property": "company_property",
        "envi_water_abstraction": "water_abstraction",
        "envi_water_discharge": "water_discharge",
        "envi_water_consumption": "water_consumption",
        "envi_diesel_consumption": "diesel_consumption",
        "envi_electric_consumption": "electric_consumption",
        "envi_non_hazard_waste": "non_hazard_waste",
        "envi_hazard_waste_generated": "hazard_waste_generated",
        "envi_hazard_waste_disposed": "hazard_waste_disposed"
    }
    
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