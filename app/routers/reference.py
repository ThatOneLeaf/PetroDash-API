from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
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
