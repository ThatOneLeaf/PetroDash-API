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

@router.post("/value-generated")
def create_value_generated(value_data: dict, db: Session = Depends(get_db)):
    """
    Insert economic value generated data into bronze layer and process to silver
    """
    try:
        logging.info(f"Creating value generated record: {value_data}")
        
        # Insert into bronze layer
        db.execute(text("""
            INSERT INTO bronze.econ_value (
                year,
                electricity_sales,
                oil_revenues,
                other_revenues,
                interest_income,
                share_in_net_income_of_associate,
                miscellaneous_income
            ) VALUES (
                :year,
                :electricity_sales,
                :oil_revenues,
                :other_revenues,
                :interest_income,
                :share_in_net_income_of_associate,
                :miscellaneous_income
            )
            ON CONFLICT (year) 
            DO UPDATE SET
                electricity_sales = EXCLUDED.electricity_sales,
                oil_revenues = EXCLUDED.oil_revenues,
                other_revenues = EXCLUDED.other_revenues,
                interest_income = EXCLUDED.interest_income,
                share_in_net_income_of_associate = EXCLUDED.share_in_net_income_of_associate,
                miscellaneous_income = EXCLUDED.miscellaneous_income
        """), {
            'year': value_data['year'],
            'electricity_sales': float(value_data.get('electricitySales', 0) or 0),
            'oil_revenues': float(value_data.get('oilRevenues', 0) or 0),
            'other_revenues': float(value_data.get('otherRevenues', 0) or 0),
            'interest_income': float(value_data.get('interestIncome', 0) or 0),
            'share_in_net_income_of_associate': float(value_data.get('shareInNetIncomeOfAssociate', 0) or 0),
            'miscellaneous_income': float(value_data.get('miscellaneousIncome', 0) or 0)
        })
        
        # Call silver layer load procedure
        db.execute(text("CALL silver.load_econ_silver()"))
        
        db.commit()
        logging.info("Value generated record created and processed to silver layer successfully")
        
        return {"message": "Value generated record created successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating value generated record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/expenditures")
def create_expenditure(expenditure_data: dict, db: Session = Depends(get_db)):
    """
    Insert economic expenditure data into bronze layer and process to silver
    """
    try:
        logging.info(f"Creating expenditure record: {expenditure_data}")
        
        # Insert into bronze layer
        db.execute(text("""
            INSERT INTO bronze.econ_expenditures (
                year,
                company_id,
                type_id,
                government_payments,
                supplier_spending_local,
                supplier_spending_abroad,
                employee_wages_benefits,
                community_investments,
                depreciation,
                depletion,
                others
            ) VALUES (
                :year,
                :company_id,
                :type_id,
                :government_payments,
                :supplier_spending_local,
                :supplier_spending_abroad,
                :employee_wages_benefits,
                :community_investments,
                :depreciation,
                :depletion,
                :others
            )
            ON CONFLICT (year, company_id, type_id) 
            DO UPDATE SET
                government_payments = EXCLUDED.government_payments,
                supplier_spending_local = EXCLUDED.supplier_spending_local,
                supplier_spending_abroad = EXCLUDED.supplier_spending_abroad,
                employee_wages_benefits = EXCLUDED.employee_wages_benefits,
                community_investments = EXCLUDED.community_investments,
                depreciation = EXCLUDED.depreciation,
                depletion = EXCLUDED.depletion,
                others = EXCLUDED.others
        """), {
            'year': expenditure_data['year'],
            'company_id': expenditure_data['comp'],
            'type_id': expenditure_data['type'],
            'government_payments': float(expenditure_data.get('government', 0) or 0),
            'supplier_spending_local': float(expenditure_data.get('localSuppl', 0) or 0),
            'supplier_spending_abroad': float(expenditure_data.get('foreignSupplierSpending', 0) or 0),
            'employee_wages_benefits': float(expenditure_data.get('employee', 0) or 0),
            'community_investments': float(expenditure_data.get('community', 0) or 0),
            'depreciation': float(expenditure_data.get('depreciation', 0) or 0),
            'depletion': float(expenditure_data.get('depletion', 0) or 0),
            'others': float(expenditure_data.get('others', 0) or 0)
        })
        
        # Call silver layer load procedure
        db.execute(text("CALL silver.load_econ_silver()"))
        
        db.commit()
        logging.info("Expenditure record created and processed to silver layer successfully")
        
        return {"message": "Expenditure record created successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating expenditure record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/capital-provider-payments")
def create_capital_provider_payment(payment_data: dict, db: Session = Depends(get_db)):
    """
    Insert capital provider payment data into bronze layer and process to silver
    """
    try:
        logging.info(f"Creating capital provider payment: {payment_data}")
        
        # Insert into bronze layer
        db.execute(text("""
            INSERT INTO bronze.econ_capital_provider_payment (
                year, 
                interest, 
                dividends_to_nci, 
                dividends_to_parent
            ) VALUES (
                :year,
                :interest,
                :dividends_to_nci,
                :dividends_to_parent
            )
            ON CONFLICT (year) 
            DO UPDATE SET
                interest = EXCLUDED.interest,
                dividends_to_nci = EXCLUDED.dividends_to_nci,
                dividends_to_parent = EXCLUDED.dividends_to_parent
        """), {
            'year': payment_data['year'],
            'interest': float(payment_data.get('interest', 0) or 0),
            'dividends_to_nci': float(payment_data.get('dividendsToNci', 0) or 0),
            'dividends_to_parent': float(payment_data.get('dividendsToParent', 0) or 0)
        })
        
        # Call silver layer load procedure
        db.execute(text("CALL silver.load_econ_silver()"))
        
        db.commit()
        logging.info("Capital provider payment created and processed to silver layer successfully")
        
        return {"message": "Capital provider payment created successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating capital provider payment: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process-bronze-to-silver")
def process_all_bronze_to_silver(db: Session = Depends(get_db)):
    """
    Process all bronze economic data to silver layer
    Useful for batch processing or data refresh
    """
    try:
        logging.info("Processing all bronze economic data to silver layer")
        
        # Call the single silver layer load procedure that handles all economic tables
        db.execute(text("CALL silver.load_econ_silver()"))
        
        db.commit()
        logging.info("All bronze economic data processed to silver layer successfully")
        
        return {"message": "All bronze economic data processed to silver layer successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error processing bronze to silver: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e)) 