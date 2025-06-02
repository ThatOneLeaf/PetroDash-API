from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
from decimal import Decimal
import logging
import traceback

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
    Get economic value generated data from gold.vw_economic_value_generated view
    Returns yearly data with detailed breakdown of revenue sources
    """
    try:
        logging.info("Executing value generated data query")
        
        result = db.execute(text("""
            SELECT 
                year,
                ROUND(electricity_sales::numeric, 2) as electricity_sales,
                ROUND(oil_revenues::numeric, 2) as oil_revenues,
                ROUND(other_revenues::numeric, 2) as other_revenues,
                ROUND(interest_income::numeric, 2) as interest_income,
                ROUND(share_in_net_income_of_associate::numeric, 2) as share_in_net_income_of_associate,
                ROUND(miscellaneous_income::numeric, 2) as miscellaneous_income,
                ROUND(total_economic_value_generated::numeric, 2) as total_economic_value_generated
            FROM gold.vw_economic_value_generated
            ORDER BY year DESC
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
        
        logging.info(f"Query returned {len(data)} rows")
        logging.info(f"Data: {data}")
        
        if not data:
            logging.warning("No data returned from query")
            return []
            
        return data
    except Exception as e:
        logging.error(f"Error fetching value generated data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/expenditures", response_model=List[Dict])
def get_economic_expenditures(db: Session = Depends(get_db)):
    """
    Get economic expenditure data from gold.vw_economic_expenditure_by_company view
    Returns yearly expenditure data with detailed breakdown by company and type
    """
    try:
        result = db.execute(text("""
            SELECT 
                year,
                company_name as comp,
                type_description as type,
                ROUND(government_payments::numeric, 2) as government,
                ROUND(local_supplier_spending::numeric, 2) as local_suppl,
                ROUND(foreign_supplier_spending::numeric, 2) as foreign_supplier_spending,
                ROUND(employee_wages_benefits::numeric, 2) as employee,
                ROUND(community_investments::numeric, 2) as community,
                ROUND(depreciation::numeric, 2) as depreciation,
                ROUND(depletion::numeric, 2) as depletion,
                ROUND(other_expenditures::numeric, 2) as others,
                ROUND(total_distributed_value_by_company::numeric, 2) as total_value
            FROM gold.vw_economic_expenditure_by_company
            ORDER BY year DESC, company_name, type_description
        """))
        
        data = [
            {
                'year': row.year,
                'comp': row.comp,
                'type': row.type,
                'government': float(row.government) if row.government else 0,
                'localSuppl': float(row.local_suppl) if row.local_suppl else 0,
                'foreignSupplierSpending': float(row.foreign_supplier_spending) if row.foreign_supplier_spending else 0,
                'employee': float(row.employee) if row.employee else 0,
                'community': float(row.community) if row.community else 0,
                'depreciation': float(row.depreciation) if row.depreciation else 0,
                'depletion': float(row.depletion) if row.depletion else 0,
                'others': float(row.others) if row.others else 0,
                'totalValue': float(row.total_value) if row.total_value else 0
            }
            for row in result
        ]
        
        logging.info(f"Retrieved {len(data)} expenditure records")
        return data
        
    except Exception as e:
        logging.error(f"Error fetching expenditure data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/capital-provider-payments", response_model=List[Dict])
def get_capital_provider_payments(db: Session = Depends(get_db)):
    """
    Get capital provider payment data from silver.econ_capital_provider_payment table
    Returns yearly data with breakdown of interest and dividend payments
    """
    try:
        logging.info("Executing capital provider payments query")
        
        result = db.execute(text("""
            SELECT 
                year,
                ROUND(interest::numeric, 2) as interest,
                ROUND(dividends_to_nci::numeric, 2) as dividends_to_nci,
                ROUND(dividends_to_parent::numeric, 2) as dividends_to_parent,
                ROUND(total_dividends_interest::numeric, 2) as total_dividends_interest
            FROM silver.econ_capital_provider_payment
            ORDER BY year DESC
        """))
        
        data = [
            {
                'year': row.year,
                'interest': float(row.interest) if row.interest else 0,
                'dividendsToNci': float(row.dividends_to_nci) if row.dividends_to_nci else 0,
                'dividendsToParent': float(row.dividends_to_parent) if row.dividends_to_parent else 0,
                'total': float(row.total_dividends_interest) if row.total_dividends_interest else 0
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} capital provider payment records")
        logging.info(f"Data: {data}")
        
        if not data:
            logging.warning("No capital provider payment data returned from query")
            return []
            
        return data
    except Exception as e:
        logging.error(f"Error fetching capital provider payment data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e)) 