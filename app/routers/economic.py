from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
from decimal import Decimal
import logging

from ..dependencies import get_db

router = APIRouter()

@router.get("/retention", response_model=List[Dict])
def get_economic_retention(db: Session = Depends(get_db)):
    """
    Get economic value retention data
    Returns year and retention ratio data
    """
    try:
        result = db.execute(text("""
            SELECT 
                year,
                ROUND(
                    (total_economic_value_generated - total_economic_value_distributed)::numeric / 
                    NULLIF(total_economic_value_generated, 0) * 100, 
                    1
                ) as retention_ratio
            FROM gold.vw_economic_value_summary
            ORDER BY year ASC
        """))
        
        data = [
            {
                key: float(value) if isinstance(value, Decimal) else value 
                for key, value in row._mapping.items()
            } 
            for row in result
        ]
        print("Data retrieved:", data)  # Debug print
        return data
    except Exception as e:
        print("Error:", str(e))  # Debug print
        raise HTTPException(status_code=500, detail=str(e)) 