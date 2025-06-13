from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional
from decimal import Decimal
import logging
import traceback
from datetime import datetime
import pandas as pd
from io import BytesIO
from app.bronze.crud import insert_csr_activity, update_csr_activity
from fastapi.responses import StreamingResponse

from ..dependencies import get_db

router = APIRouter()

def create_excel_template(headers: List[str], filename: str) -> BytesIO:
    """Create minimal Excel template with just headers and readable column widths"""
    df = pd.DataFrame({header: [] for header in headers})
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
        worksheet = writer.sheets['Sheet1']
        
        # Auto-adjust column widths to make headers readable
        for column in worksheet.columns:
            max_length = len(str(column[0].value)) + 2  # Header length + padding
            worksheet.column_dimensions[column[0].column_letter].width = max_length
    
    output.seek(0)
    return output

@router.get("/programs", response_model=List[Dict])
def get_csr_programs(db: Session = Depends(get_db)):
    """
    Get all CSR programs
    Returns list of programs with their details
    """
    try:
        logging.info("Executing CSR programs query")
        
        result = db.execute(text("""
            SELECT 
                program_id,
                program_name,
                date_created,
                date_updated
            FROM silver.csr_programs
            ORDER BY program_name
        """))
        
        data = [
            {
                'programId': row.program_id,
                'programName': row.program_name,
                'dateCreated': row.date_created.isoformat() if row.date_created else None,
                'dateUpdated': row.date_updated.isoformat() if row.date_updated else None
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} CSR programs")
        return data

    except Exception as e:
        logging.error(f"Error fetching CSR programs: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/projects", response_model=List[Dict])
def get_csr_projects(program_id: Optional[str] = None, db: Session = Depends(get_db)):
    """
    Get CSR projects, optionally filtered by program_id
    Returns list of projects with their program information
    """
    try:
        logging.info(f"Executing CSR projects query with program_id filter: {program_id}")
        
        where_clause = ""
        params = {}
        
        if program_id:
            where_clause = "WHERE cp.program_id = :program_id"
            params['program_id'] = program_id
        
        result = db.execute(text(f"""
            SELECT 
                cp.program_id,
                pr.program_name,
                cp.project_id,
                cp.project_name,
                cp.project_metrics,
                cp.date_created,
                cp.date_updated
            FROM silver.csr_projects cp
            JOIN silver.csr_programs pr ON cp.program_id = pr.program_id
            {where_clause}
            ORDER BY pr.program_name, cp.project_name
        """), params)

        data = [
            {
                'projectId': row.project_id,
                'programId': row.program_id,
                'programName': row.program_name,
                'projectName': row.project_name,
                'projectMetrics': row.project_metrics,
                'dateCreated': row.date_created.isoformat() if row.date_created else None,
                'dateUpdated': row.date_updated.isoformat() if row.date_updated else None
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} CSR projects")
        return data
        
    except Exception as e:
        logging.error(f"Error fetching CSR projects: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/activities", response_model=List[Dict])
def get_csr_activities(
    year: Optional[int] = None,
    company_id: Optional[str] = None,
    program_id: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Get CSR activities with optional filters
    Returns list of activities with company, project, and program information
    """
    try:
        logging.info(f"Executing CSR activities query with filters - year: {year}, company_id: {company_id}, program_id: {program_id}")
        
        where_conditions = []
        params = {}
        
        if year:
            where_conditions.append("ca.project_year = :year")
            params['year'] = year
            
        if company_id:
            where_conditions.append("ca.company_id = :company_id")
            params['company_id'] = company_id
            
        if program_id:
            where_conditions.append("cp.program_id = :program_id")
            params['program_id'] = program_id
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        result = db.execute(text(f"""
            SELECT 
                ca.csr_id,
                ca.company_id,
                cm.company_name,
                ca.project_id,
                cp.project_name,
                cp.program_id,
                pr.program_name,
                ca.project_year,
                ROUND(ca.csr_report::numeric, 2) as csr_report,
                ROUND(ca.project_expenses::numeric, 2) as project_expenses,
                csl.status_id,
                ca.date_created,
                ca.date_updated
            FROM silver.csr_activity ca
            JOIN ref.company_main cm ON ca.company_id = cm.company_id
            JOIN silver.csr_projects cp ON ca.project_id = cp.project_id
            JOIN silver.csr_programs pr ON cp.program_id = pr.program_id
            JOIN public.checker_status_log csl ON ca.csr_id = csl.record_id
            {where_clause} AND
                (
                    ca.project_id LIKE 'HE%' 
                    OR ca.project_id LIKE 'ED%' 
                    OR ca.project_id LIKE 'LI%'
                )
            ORDER BY ca.csr_report DESC NULLS LAST, cm.company_name, pr.program_name, cp.project_name
        """), params)

        data = [
            {
                'csrId': row.csr_id,
                'companyId': row.company_id,
                'companyName': row.company_name,
                'programId': row.program_id,
                'programName': row.program_name,
                'projectId': row.project_id,
                'projectName': row.project_name,
                'projectYear': row.project_year,
                'csrReport': float(row.csr_report) if row.csr_report else 0,
                'projectExpenses': float(row.project_expenses) if row.project_expenses else 0,
                'statusId': (
                    "Pending" if row.status_id == "PND"
                    else "Head Approved" if row.status_id == "HAP"
                    else row.status_id
                ),
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} CSR activities")
        return data
        
    except Exception as e:
        logging.error(f"Error fetching CSR activities: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/activities-specific", response_model=Dict)
def get_csr_activity_specific(
    csr_id: str = Query(..., alias="csrId"),
    db: Session = Depends(get_db)
):
    """
    Get a single CSR activity by project_id
    """
    try:
        result = db.execute(text("""
            SELECT 
                ca.csr_id,
                ca.company_id,
                cm.company_name,
                ca.project_id,
                cp.project_name,
                cp.program_id,
                pr.program_name,
                ca.project_year,
                ROUND(ca.csr_report::numeric, 2) as csr_report,
                ROUND(ca.project_expenses::numeric, 2) as project_expenses,
                ca.date_created,
                ca.date_updated
            FROM silver.csr_activity ca
            JOIN ref.company_main cm ON ca.company_id = cm.company_id
            JOIN silver.csr_projects cp ON ca.project_id = cp.project_id
            JOIN silver.csr_programs pr ON cp.program_id = pr.program_id
            WHERE ca.csr_id = :csr_id
            LIMIT 1
        """), {"csr_id": csr_id})

        row = result.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="CSR activity not found")

        data = {
            'csrId': row.csr_id,
            'companyId': row.company_id,
            'companyName': row.company_name,
            'programId': row.program_id,
            'programName': row.program_name,
            'projectId': row.project_id,
            'projectName': row.project_name,
            'projectYear': row.project_year,
            'csrReport': float(row.csr_report) if row.csr_report else 0,
            'projectExpenses': float(row.project_expenses) if row.project_expenses else 0,
        }
        return data

    except Exception as e:
        logging.error(f"Error fetching CSR activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/activities-update")
def update_csr_activity_single(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Update single csr activity record")
        CURRENT_YEAR = datetime.now().year

        required_fields = ['project_year', 'project_id', 'csr_report', 'project_expenses']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["project_id"], str) or not data["project_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid project ID")
        
        if not isinstance(data["project_year"], int) or not (1900 <= int(data["project_year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid project_year")
        
        if not isinstance(data["csr_report"], int) or (int(data["csr_report"]) < 0):
            raise HTTPException(status_code=422, detail="Invalid beneficiaries")

        if not isinstance(data["project_expenses"], (int, float)) or (data["project_expenses"] < 0):
            raise HTTPException(status_code=422, detail="Invalid project investment")

        record = {
            "csr_id": data["csr_id"],
            "project_id": data["project_id"],
            "project_year": data["project_year"],
            "csr_report": data["csr_report"],
            "project_expenses": data["project_expenses"]
        }

        update_csr_activity(db, record)
        return {"message": "1 record successfully updated."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error updating CSR activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/activities-single")
def insert_csr_activity_single(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single csr activity record")
        CURRENT_YEAR = datetime.now().year

        required_fields = ['company_id', 'project_id', 'project_year', 'csr_report', 'project_expenses']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["project_id"], str) or not data["project_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid project ID")
        
        if not isinstance(data["project_year"], int) or not (1900 <= int(data["project_year"]) <= CURRENT_YEAR + 1):
            raise HTTPException(status_code=422, detail="Invalid project_year")
        
        if not isinstance(data["csr_report"], int) or (int(data["csr_report"]) < 0):
            raise HTTPException(status_code=422, detail="Invalid beneficiaries")

        if not isinstance(data["project_expenses"], (int, float)) or (data["project_expenses"] < 0):
            raise HTTPException(status_code=422, detail="Invalid project investment")

        record = {
            "company_id": data["company_id"],
            "project_id": data["project_id"],
            "project_year": data["project_year"],
            "csr_report": data["csr_report"],
            "project_expenses": data["project_expenses"]
        }
        insert_csr_activity(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error inserting CSR activity: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/help-activity-template")
async def download_help_activity_template():
    """Generate Excel template for HELP activities data"""
    try:
        headers = ['company_id', 'project_name', 'project_year', 'csr_activity', 'project_expenses', 'project_remarks']
        filename = 'help_activity_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        # Do not expose internal error details to client
        logging.error(f"Error generating template: {str(e)}")
        raise HTTPException(status_code=500, detail="Error generating template.")

@router.post("help_activity-bulk")
def bulk_help_activity(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info(f"Add bulk data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        # basic validation...
        required_columns = {'company_id', 'year', 'month', 'quarter', 'volume', 'unit_of_measurement'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(status_code=400, detail=f"Missing required columns: {required_columns - set(df.columns)}")

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
            
            # Existing validations
            if not isinstance(row["company_id"], str) or not row["company_id"].strip():
                validation_errors.append(f"Row {row_number}: Invalid company_id")
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
                "company_id": row["company_id"].strip(),
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
