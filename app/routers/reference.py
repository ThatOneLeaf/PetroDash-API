from fastapi import APIRouter, Depends,  Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List, Dict
from ..dependencies import get_db
import logging
import traceback

router = APIRouter()

@router.get("/companies", response_model=List[Dict])
def get_companies(db: Session = Depends(get_db)):
    """
    Get list of companies from ref.company_main
    """
    try:
        result = db.execute(text("""
            SELECT 
                company_id as id,
                company_name as name
            FROM ref.company_main
            ORDER BY company_id ASC
        """))
        
        companies = [
            {
                'id': row.id,
                'name': row.name
            }
            for row in result
        ]
        
        return companies
    except Exception as e:
        logging.error(f"Error fetching companies: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/expenditure-types", response_model=List[Dict])
def get_expenditure_types(db: Session = Depends(get_db)):
    """
    Get list of expenditure types from ref.expenditure_type
    """
    try:
        result = db.execute(text("""
            SELECT 
                type_id as id,
                type_description as name
            FROM ref.expenditure_type
            ORDER BY type_id ASC
        """))
        
        types = [
            {
                'id': row.id,
                'name': row.name
            }
            for row in result
        ]
        
        return types
    except Exception as e:
        logging.error(f"Error fetching expenditure types: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))



# ====================== powerplant based on company ====================== #
@router.get("/power_plants", response_model=List[dict])
def get_fact_energy(
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            SELECT power_plant_id, site_name 
            FROM ref.ref_power_plants 
            WHERE (:company_ids IS NULL OR company_id = ANY(:company_ids));
        """)

        result = db.execute(sql, {"company_ids": company_ids})
        data = [dict(row._mapping) for row in result]
        return data

    except Exception as e:
        logging.error(f"Error calling powerplant: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")