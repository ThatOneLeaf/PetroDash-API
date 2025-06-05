from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional
from decimal import Decimal
import logging
import traceback
from datetime import datetime
import pandas as pd
from io import BytesIO
from app.bronze.crud import insert_csr_activity

from ..dependencies import get_db

router = APIRouter()

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
                CASE 
                    WHEN csl.status_id = 'HAP' THEN 'Head Approved'
                    ELSE csl.status_id
                END AS status_id,
                ca.date_created,
                ca.date_updated
            FROM silver.csr_activity ca
            JOIN ref.company_main cm ON ca.company_id = cm.company_id
            JOIN silver.csr_projects cp ON ca.project_id = cp.project_id
            JOIN silver.csr_programs pr ON cp.program_id = pr.program_id
            JOIN public.checker_status_log as csl ON csl.record_id = ca.csr_id
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
                'projectId': row.project_id,
                'projectName': row.project_name,
                'programId': row.program_id,
                'programName': row.program_name,
                'projectYear': row.project_year,
                'csrReport': float(row.csr_report) if row.csr_report else 0,
                'projectExpenses': float(row.project_expenses) if row.project_expenses else 0,
                'statusId': row.status_id,
                'dateCreated': row.date_created.isoformat() if row.date_created else None,
                'dateUpdated': row.date_updated.isoformat() if row.date_updated else None
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} CSR activities")
        return data
        
    except Exception as e:
        logging.error(f"Error fetching CSR activities: {str(e)}")
        logging.error(traceback.format_exc())
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
            # "csr_id": data["csr_id"],
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

@router.post("/activities-bulk")
def insert_csr_activity_bulk(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single csr activity record")
        CURRENT_YEAR = datetime.now().year

        required_fields = ['csr_id', 'company_id', 'project_id', 'project_year', 'csr_report', 'project_expenses']
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
            "csr_id": data["csr_id"],
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
        raise HTTPException(status_code=500, detail=str(e))

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