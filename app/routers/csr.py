from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional
from decimal import Decimal
import logging
import traceback

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
                cp.project_id,
                cp.program_id,
                cp.project_name,
                cp.project_metrics,
                cp.date_created,
                cp.date_updated,
                pr.program_name
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