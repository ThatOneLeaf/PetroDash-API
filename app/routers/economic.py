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

@router.get("/value-generated-data", response_model=List[Dict])
def get_value_generated_data(db: Session = Depends(get_db)):
    """
    Get economic value generated data using the gold.func_economic_value_generated_details function
    Returns yearly data with detailed breakdown of revenue sources
    """
    try:
        result = db.execute(text("""
            SELECT * FROM gold.func_economic_value_generated_details(
                p_year := NULL,
                p_order_by := 'year',
                p_order_direction := 'DESC'
            )
        """))
        
        data = [
            {
                'year': row.year,
                'electricitySales': float(row.electricity_sales) if row.electricity_sales else 0,
                'oilRevenues': float(row.oil_revenues) if row.oil_revenues else 0,
                'otherRevenues': float(row.other_revenues) if row.other_revenues else 0,
                'interestIncome': float(row.interest_income) if row.interest_income else 0,
                'shareInNetIncomeOfAssociate': float(row.share_in_net_income_of_associate) if row.share_in_net_income_of_associate else 0,
                'miscellaneousIncome': float(row.miscellaneous_income) if row.miscellaneous_income else 0,
                'totalRevenue': float(row.total_economic_value_generated) if row.total_economic_value_generated else 0
            }
            for row in result
        ]
        
        return data
    except Exception as e:
        logging.error(f"Error fetching value generated data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 