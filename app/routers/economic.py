from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
from decimal import Decimal
import logging
import traceback
import pandas as pd
import io
from fastapi.responses import StreamingResponse
import time

from ..dependencies import get_db
from ..auth_decorators import require_role, office_checker_only

router = APIRouter()

# Helper function for creating Excel templates
def create_excel_template(headers: List[str], filename: str) -> io.BytesIO:
    """Create minimal Excel template with just headers and readable column widths"""
    df = pd.DataFrame({header: [] for header in headers})
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
        worksheet = writer.sheets['Sheet1']
        
        # Auto-adjust column widths to make headers readable
        for column in worksheet.columns:
            max_length = len(str(column[0].value)) + 2  # Header length + padding
            worksheet.column_dimensions[column[0].column_letter].width = max_length
    
    output.seek(0)
    return output

# Helper functions for validation
def validate_year(year_value):
    """Validate that year is a 4-digit number"""
    if year_value is None:
        return False, "Year is required"
    
    try:
        year = int(year_value)
        if year < 1000 or year > 9999:
            return False, f"Year must be a 4-digit number, got {year}"
        return True, None
    except (ValueError, TypeError):
        return False, f"Year must be a number, got {year_value}"

def validate_company_id(company_id, db: Session):
    """Validate that company ID exists in company_main table"""
    if company_id is None:
        return False, "Company ID is required"
    
    try:
        # First try as numeric ID
        try:
            company_id_str = str(int(company_id))
            result = db.execute(text("SELECT company_id FROM ref.company_main WHERE company_id = :company_id"), 
                              {"company_id": company_id_str})
            if result.fetchone() is not None:
                return True, None
        except (ValueError, TypeError):
            pass  # Not a number, try as text
        
        # Try as company name or code (case insensitive)
        company_str = str(company_id).strip()
        result = db.execute(text("""
            SELECT company_id FROM ref.company_main 
            WHERE UPPER(company_name) = UPPER(:company_id) 
            OR UPPER(company_id) = UPPER(:company_id)
        """), {"company_id": company_str})
        
        if result.fetchone() is not None:
            return True, None
            
        return False, f"Company ID '{company_id}' does not exist"
        
    except Exception as e:
        return False, f"Database error checking Company ID: {str(e)}"

def validate_type_id(type_id, db: Session):
    """Validate that type ID exists in expenditure_type table"""
    if type_id is None:
        return False, "Type ID is required"
    
    try:
        # First try as numeric ID
        try:
            type_id_str = str(int(type_id))
            result = db.execute(text("SELECT type_id FROM ref.expenditure_type WHERE type_id = :type_id"), 
                              {"type_id": type_id_str})
            if result.fetchone() is not None:
                return True, None
        except (ValueError, TypeError):
            pass  # Not a number, try as text
        
        # Try as type description or code (case insensitive)
        type_str = str(type_id).strip()
        result = db.execute(text("""
            SELECT type_id FROM ref.expenditure_type 
            WHERE UPPER(type_description) = UPPER(:type_id) 
            OR UPPER(type_id) = UPPER(:type_id)
        """), {"type_id": type_str})
        
        if result.fetchone() is not None:
            return True, None
            
        return False, f"Type ID '{type_id}' does not exist"
        
    except Exception as e:
        return False, f"Database error checking Type ID: {str(e)}"

# Helper function for processing Excel imports
async def process_excel_import(file: UploadFile, import_config: Dict, db: Session):
    """
    Process Excel file import with flexible column mapping and error handling
    All-or-nothing approach: if any row has validation errors, entire import is rejected
    
    Args:
        file: Uploaded Excel file
        import_config: Dict containing:
            - expected_columns: Dict mapping field names to possible column names
            - required_fields: List of required field names
            - validate_expenditures: Boolean to enable expenditure-specific validation
            - insert_query: SQL insert query template
    """
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are supported")
        
        # Read Excel file
        contents = await file.read()
        df = pd.read_excel(io.BytesIO(contents))
        
        logging.info(f"Processing Excel file with {len(df)} rows")
        logging.info(f"Columns found: {list(df.columns)}")
        
        # Map actual columns to expected columns
        column_mapping = {}
        df_columns_lower = [col.lower().strip() for col in df.columns]
        
        for expected_col, possible_names in import_config['expected_columns'].items():
            found = False
            for possible_name in possible_names:
                if possible_name.lower() in df_columns_lower:
                    actual_col_index = df_columns_lower.index(possible_name.lower())
                    actual_col_name = df.columns[actual_col_index]
                    column_mapping[expected_col] = actual_col_name
                    found = True
                    break
            if not found and expected_col in import_config['required_fields']:
                raise HTTPException(status_code=400, detail=f"Required column '{expected_col}' not found in Excel file")
        
        logging.info(f"Column mapping: {column_mapping}")
        
        # PHASE 1: Validate ALL rows first
        validation_errors = []
        valid_records = []
        
        for index, row in df.iterrows():
            # Extract and validate data
            record_data = {}
            row_errors = []
            
            for field, column in column_mapping.items():
                value = row[column] if column in row.index else None
                
                # Handle different data types and validation
                if field == 'year':
                    valid, error_msg = validate_year(value)
                    if not valid:
                        row_errors.append(error_msg)
                        continue
                    record_data[field] = int(value)
                    
                elif field == 'company_id' and import_config.get('validate_expenditures', False):
                    valid, error_msg = validate_company_id(value, db)
                    if not valid:
                        row_errors.append(error_msg)
                        continue
                    # Convert to actual company ID for database storage
                    actual_id = get_company_id(value, db)
                    if actual_id is None:
                        row_errors.append(f"Could not resolve company ID for '{value}'")
                        continue
                    record_data[field] = actual_id
                    
                elif field == 'type_id' and import_config.get('validate_expenditures', False):
                    valid, error_msg = validate_type_id(value, db)
                    if not valid:
                        row_errors.append(error_msg)
                        continue
                    # Convert to actual type ID for database storage
                    actual_id = get_type_id(value, db)
                    if actual_id is None:
                        row_errors.append(f"Could not resolve type ID for '{value}'")
                        continue
                    record_data[field] = actual_id
                    
                elif field.endswith('_id'):
                    if value is None or value == '':
                        if field in import_config['required_fields']:
                            row_errors.append(f"{field} is required")
                            continue
                        record_data[field] = None
                    else:
                        try:
                            record_data[field] = int(value)
                        except (ValueError, TypeError):
                            row_errors.append(f"{field} must be a number, got {value}")
                            continue
                else:
                    record_data[field] = float(value or 0) if value is not None else 0
            
            if row_errors:
                validation_errors.append(f"Row {index + 2}: {'; '.join(row_errors)}")
            else:
                valid_records.append(record_data)
        
        # If there are ANY validation errors, reject the entire import
        if validation_errors:
            result = {
                "message": "Import rejected due to validation errors",
                "total_processed": len(df),
                "successful_imports": 0,
                "errors": len(validation_errors),
                "error_details": validation_errors[:10]  # Limit to first 10 errors
            }
            return result
        
        # PHASE 2: If all rows are valid, proceed with import
        success_count = 0
        
        for record_data in valid_records:
            try:
                db.execute(text(import_config['insert_query']), record_data)
                success_count += 1
            except Exception as insert_error:
                # If there's an insert error after validation passed, rollback and report
                db.rollback()
                logging.error(f"Insert error after validation: {str(insert_error)}")
                raise HTTPException(status_code=500, detail=f"Database insert error: {str(insert_error)}")
        
        # Process to silver layer
        db.execute(text("CALL silver.load_econ_silver()"))
        db.commit()
        time.sleep(0.5)
        
        logging.info(f"Import completed successfully: {success_count} records imported")
        
        result = {
            "message": f"Import completed successfully",
            "total_processed": len(df),
            "successful_imports": success_count,
            "errors": 0
        }
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error in process_excel_import: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error processing file: {str(e)}")

# Helper functions to convert text identifiers to actual IDs
def get_company_id(company_identifier, db: Session):
    """Convert company name/code to actual company_id"""
    if company_identifier is None:
        return None
    
    try:
        # First try as numeric ID
        try:
            company_id_str = str(int(company_identifier))
            result = db.execute(text("SELECT company_id FROM ref.company_main WHERE company_id = :company_id"), 
                              {"company_id": company_id_str})
            row = result.fetchone()
            if row is not None:
                return row.company_id
        except (ValueError, TypeError):
            pass  # Not a number, try as text
        
        # Try as company name or code (case insensitive)
        company_str = str(company_identifier).strip()
        result = db.execute(text("""
            SELECT company_id FROM ref.company_main 
            WHERE UPPER(company_name) = UPPER(:company_id) 
            OR UPPER(company_id) = UPPER(:company_id)
        """), {"company_id": company_str})
        
        row = result.fetchone()
        if row is not None:
            return row.company_id
            
        return None
        
    except Exception as e:
        logging.error(f"Error getting company ID: {str(e)}")
        return None

def get_type_id(type_identifier, db: Session):
    """Convert type description/code to actual type_id"""
    if type_identifier is None:
        return None
    
    try:
        # First try as numeric ID
        try:
            type_id_str = str(int(type_identifier))
            result = db.execute(text("SELECT type_id FROM ref.expenditure_type WHERE type_id = :type_id"), 
                              {"type_id": type_id_str})
            row = result.fetchone()
            if row is not None:
                return row.type_id
        except (ValueError, TypeError):
            pass  # Not a number, try as text
        
        # Try as type description or code (case insensitive)
        type_str = str(type_identifier).strip()
        result = db.execute(text("""
            SELECT type_id FROM ref.expenditure_type 
            WHERE UPPER(type_description) = UPPER(:type_id) 
            OR UPPER(type_id) = UPPER(:type_id)
        """), {"type_id": type_str})
        
        row = result.fetchone()
        if row is not None:
            return row.type_id
            
        return None
        
    except Exception as e:
        logging.error(f"Error getting type ID: {str(e)}")
        return None

@router.get("/retention", response_model=List[Dict])
@office_checker_only
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
@office_checker_only
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
@office_checker_only
def get_economic_expenditures(db: Session = Depends(get_db)):
    """
    Get economic expenditure data from gold.vw_economic_expenditure_by_company view
    Returns yearly expenditure data with detailed breakdown by company and type
    """
    try:
        result = db.execute(text("""
            SELECT 
                year,
                company_id as comp,
                type_id as type,
                ROUND(government_payments::numeric, 2) as government,
                ROUND(local_supplier_spending::numeric, 2) as local_supplier_spending,
                ROUND(foreign_supplier_spending::numeric, 2) as foreign_supplier_spending,
                ROUND(employee_wages_benefits::numeric, 2) as employee,
                ROUND(community_investments::numeric, 2) as community,
                ROUND(depreciation::numeric, 2) as depreciation,
                ROUND(depletion::numeric, 2) as depletion,
                ROUND(other_expenditures::numeric, 2) as others,
                ROUND(total_distributed_value_by_company::numeric, 2) as total_distributed,
                ROUND((total_distributed_value_by_company + depreciation + 
                       depletion + other_expenditures)::numeric, 2) as total_expenditures
            FROM gold.vw_economic_expenditure_by_company
            ORDER BY year DESC, company_id, type_id
        """))
        
        data = [
            {
                'year': row.year,
                'comp': row.comp,
                'type': row.type,
                'government': float(row.government) if row.government else 0,
                'localSupplierSpending': float(row.local_supplier_spending) if row.local_supplier_spending else 0,
                'foreignSupplierSpending': float(row.foreign_supplier_spending) if row.foreign_supplier_spending else 0,
                'employee': float(row.employee) if row.employee else 0,
                'community': float(row.community) if row.community else 0,
                'depreciation': float(row.depreciation) if row.depreciation else 0,
                'depletion': float(row.depletion) if row.depletion else 0,
                'others': float(row.others) if row.others else 0,
                'totalDistributed': float(row.total_distributed) if row.total_distributed else 0,
                'totalExpenditures': float(row.total_expenditures) if row.total_expenditures else 0
            }
            for row in result
        ]
        
        logging.info(f"Retrieved {len(data)} expenditure records")
        return data
        
    except Exception as e:
        logging.error(f"Error fetching expenditure data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/expenditures/{comp}/{year}", response_model=Dict)
@office_checker_only
def get_expenditure_by_company_year(comp: str, year: int, db: Session = Depends(get_db)):
    """
    Get expenditure records for a specific company and year with company name and type descriptions
    Returns data formatted for the edit modal
    """
    try:
        logging.info(f"Fetching expenditure records for company {comp}, year {year}")
        
        result = db.execute(text("""
            SELECT 
                e.year,
                e.company_id,
                e.type_id,
                c.company_name,
                t.type_description,
                ROUND(e.government_payments::numeric, 2) as government_payments,
                ROUND(e.supplier_spending_local::numeric, 2) as supplier_spending_local,
                ROUND(e.supplier_spending_abroad::numeric, 2) as supplier_spending_abroad,
                ROUND(e.employee_wages_benefits::numeric, 2) as employee_wages_benefits,
                ROUND(e.community_investments::numeric, 2) as community_investments,
                ROUND(e.depreciation::numeric, 2) as depreciation,
                ROUND(e.depletion::numeric, 2) as depletion,
                ROUND(e.others::numeric, 2) as others
            FROM bronze.econ_expenditures e
            JOIN ref.company_main c ON e.company_id = c.company_id
            JOIN ref.expenditure_type t ON e.type_id = t.type_id
            WHERE e.company_id = :company_id AND e.year = :year
            ORDER BY e.type_id
        """), {
            'company_id': comp,
            'year': year
        })
        
        records = result.fetchall()
        
        if not records:
            raise HTTPException(status_code=404, detail=f"No expenditure records found for company {comp}, year {year}")
        
        # Format response with company info and types
        response = {
            'comp': comp,
            'year': year,
            'companyName': records[0].company_name,
            'types': {}
        }
        
        for record in records:
            type_key = record.type_description
            response['types'][type_key] = {
                'type_id': record.type_id,
                'government': float(record.government_payments) if record.government_payments else 0,
                'localSupplierSpending': float(record.supplier_spending_local) if record.supplier_spending_local else 0,
                'foreignSupplierSpending': float(record.supplier_spending_abroad) if record.supplier_spending_abroad else 0,
                'employee': float(record.employee_wages_benefits) if record.employee_wages_benefits else 0,
                'community': float(record.community_investments) if record.community_investments else 0,
                'depreciation': float(record.depreciation) if record.depreciation else 0,
                'depletion': float(record.depletion) if record.depletion else 0,
                'others': float(record.others) if record.others else 0
            }
        
        logging.info(f"Retrieved expenditure records: {response}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching expenditure record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/capital-provider-payments", response_model=List[Dict])
@office_checker_only
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
@office_checker_only
def create_value_generated(
    value_data: dict, 
    db: Session = Depends(get_db)
):
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
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("Value generated record created and processed to silver layer successfully")
        
        return {"message": "Value generated record created successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating value generated record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/value-generated/{year}", response_model=Dict)
@office_checker_only
def get_value_generated_by_year(year: int, db: Session = Depends(get_db)):
    """
    Get value generated record for a specific year
    Returns data formatted for the edit modal
    """
    try:
        logging.info(f"Fetching value generated record for year {year}")
        
        result = db.execute(text("""
            SELECT 
                year,
                ROUND(electricity_sales::numeric, 2) as electricity_sales,
                ROUND(oil_revenues::numeric, 2) as oil_revenues,
                ROUND(other_revenues::numeric, 2) as other_revenues,
                ROUND(interest_income::numeric, 2) as interest_income,
                ROUND(share_in_net_income_of_associate::numeric, 2) as share_in_net_income_of_associate,
                ROUND(miscellaneous_income::numeric, 2) as miscellaneous_income
            FROM bronze.econ_value
            WHERE year = :year
        """), {'year': year})
        
        record = result.fetchone()
        
        if not record:
            raise HTTPException(status_code=404, detail=f"No value generated record found for year {year}")
        
        # Format response
        response = {
            'year': record.year,
            'electricitySales': float(record.electricity_sales) if record.electricity_sales else 0,
            'oilRevenues': float(record.oil_revenues) if record.oil_revenues else 0,
            'otherRevenues': float(record.other_revenues) if record.other_revenues else 0,
            'interestIncome': float(record.interest_income) if record.interest_income else 0,
            'shareInNetIncomeOfAssociate': float(record.share_in_net_income_of_associate) if record.share_in_net_income_of_associate else 0,
            'miscellaneousIncome': float(record.miscellaneous_income) if record.miscellaneous_income else 0
        }
        
        logging.info(f"Retrieved value generated record: {response}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching value generated record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/value-generated/{year}")
@office_checker_only
def update_value_generated(
    year: int, 
    value_data: dict, 
    db: Session = Depends(get_db)
):
    """
    Update economic value generated data in bronze layer and process to silver
    """
    try:
        logging.info(f"Updating value generated record for year {year}: {value_data}")
        
        # Validate that the record exists
        existing_record = db.execute(text("""
            SELECT 1 FROM bronze.econ_value 
            WHERE year = :year
        """), {'year': year}).fetchone()
        
        if not existing_record:
            raise HTTPException(status_code=404, detail=f"Value generated record not found for year {year}")
        
        # Update the record in bronze layer
        db.execute(text("""
            UPDATE bronze.econ_value 
            SET 
                electricity_sales = :electricity_sales,
                oil_revenues = :oil_revenues,
                other_revenues = :other_revenues,
                interest_income = :interest_income,
                share_in_net_income_of_associate = :share_in_net_income_of_associate,
                miscellaneous_income = :miscellaneous_income
            WHERE year = :year
        """), {
            'year': year,
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
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("Value generated record updated and processed to silver layer successfully")
        
        return {"message": "Value generated record updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error updating value generated record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/expenditures")
@office_checker_only
def create_expenditure(
    expenditure_data: dict, 
    db: Session = Depends(get_db)
):
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
            'supplier_spending_local': float(expenditure_data.get('localSupplierSpending', 0) or 0),
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
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("Expenditure record created and processed to silver layer successfully")
        
        return {"message": "Expenditure record created successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating expenditure record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/expenditures/{comp}/{year}/{type}")
@office_checker_only
def update_expenditure(
    comp: str, 
    year: int, 
    type: str, 
    expenditure_data: dict, 
    db: Session = Depends(get_db)
):
    """
    Update economic expenditure data in bronze layer and process to silver
    """
    try:
        logging.info(f"Updating expenditure record: comp={comp}, year={year}, type={type}, data={expenditure_data}")
        
        # Validate that the record exists
        existing_record = db.execute(text("""
            SELECT 1 FROM bronze.econ_expenditures 
            WHERE company_id = :company_id AND year = :year AND type_id = :type_id
        """), {
            'company_id': comp,
            'year': year,
            'type_id': type
        }).fetchone()
        
        if not existing_record:
            raise HTTPException(status_code=404, detail=f"Expenditure record not found for company {comp}, year {year}, type {type}")
        
        # Update the record in bronze layer
        db.execute(text("""
            UPDATE bronze.econ_expenditures 
            SET 
                government_payments = :government_payments,
                supplier_spending_local = :supplier_spending_local,
                supplier_spending_abroad = :supplier_spending_abroad,
                employee_wages_benefits = :employee_wages_benefits,
                community_investments = :community_investments,
                depreciation = :depreciation,
                depletion = :depletion,
                others = :others
            WHERE company_id = :company_id AND year = :year AND type_id = :type_id
        """), {
            'company_id': comp,
            'year': year,
            'type_id': type,
            'government_payments': float(expenditure_data.get('government', 0) or 0),
            'supplier_spending_local': float(expenditure_data.get('localSupplierSpending', 0) or 0),
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
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("Expenditure record updated and processed to silver layer successfully")
        
        return {"message": "Expenditure record updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error updating expenditure record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/capital-provider-payments")
@office_checker_only
def create_capital_provider_payment(
    payment_data: dict, 
    db: Session = Depends(get_db)
):
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
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("Capital provider payment created and processed to silver layer successfully")
        
        return {"message": "Capital provider payment created successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error creating capital provider payment: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/capital-provider-payments/{year}", response_model=Dict)
@office_checker_only
def get_capital_provider_payment_by_year(year: int, db: Session = Depends(get_db)):
    """
    Get capital provider payment record for a specific year
    Returns data formatted for the edit modal
    """
    try:
        logging.info(f"Fetching capital provider payment record for year {year}")
        
        result = db.execute(text("""
            SELECT 
                year,
                ROUND(interest::numeric, 2) as interest,
                ROUND(dividends_to_nci::numeric, 2) as dividends_to_nci,
                ROUND(dividends_to_parent::numeric, 2) as dividends_to_parent
            FROM bronze.econ_capital_provider_payment
            WHERE year = :year
        """), {'year': year})
        
        record = result.fetchone()
        
        if not record:
            raise HTTPException(status_code=404, detail=f"No capital provider payment record found for year {year}")
        
        # Format response
        response = {
            'year': record.year,
            'interest': float(record.interest) if record.interest else 0,
            'dividendsToNci': float(record.dividends_to_nci) if record.dividends_to_nci else 0,
            'dividendsToParent': float(record.dividends_to_parent) if record.dividends_to_parent else 0
        }
        
        logging.info(f"Retrieved capital provider payment record: {response}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching capital provider payment record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/capital-provider-payments/{year}")
@office_checker_only
def update_capital_provider_payment(
    year: int, 
    payment_data: dict, 
    db: Session = Depends(get_db)
):
    """
    Update capital provider payment data in bronze layer and process to silver
    """
    try:
        logging.info(f"Updating capital provider payment record for year {year}: {payment_data}")
        
        # Validate that the record exists
        existing_record = db.execute(text("""
            SELECT 1 FROM bronze.econ_capital_provider_payment 
            WHERE year = :year
        """), {'year': year}).fetchone()
        
        if not existing_record:
            raise HTTPException(status_code=404, detail=f"Capital provider payment record not found for year {year}")
        
        # Update the record in bronze layer
        db.execute(text("""
            UPDATE bronze.econ_capital_provider_payment 
            SET 
                interest = :interest,
                dividends_to_nci = :dividends_to_nci,
                dividends_to_parent = :dividends_to_parent
            WHERE year = :year
        """), {
            'year': year,
            'interest': float(payment_data.get('interest', 0) or 0),
            'dividends_to_nci': float(payment_data.get('dividendsToNci', 0) or 0),
            'dividends_to_parent': float(payment_data.get('dividendsToParent', 0) or 0)
        })
        
        # Call silver layer load procedure
        db.execute(text("CALL silver.load_econ_silver()"))
        
        db.commit()
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("Capital provider payment record updated and processed to silver layer successfully")
        
        return {"message": "Capital provider payment record updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logging.error(f"Error updating capital provider payment record: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/process-bronze-to-silver")
@office_checker_only
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
        
        # Small delay to ensure data is fully processed
        time.sleep(0.5)
        
        logging.info("All bronze economic data processed to silver layer successfully")
        
        return {"message": "All bronze economic data processed to silver layer successfully"}
        
    except Exception as e:
        db.rollback()
        logging.error(f"Error processing bronze to silver: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

# Template generation routes using helper function
@router.get("/template-generated")
@office_checker_only
async def download_economic_generated_template():
    """Generate Excel template for economic generated data"""
    try:
        headers = ['Year', 'Electricity Sales', 'Oil Revenues', 'Other Revenues', 'Interest Income', 'Share in Net Income of Associate', 'Miscellaneous Income']
        filename = 'economic_generated_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")

@router.get("/template-expenditures")
@office_checker_only
async def download_economic_expenditures_template():
    """Generate Excel template for economic expenditures data"""
    try:
        headers = ['Company ID', 'Year', 'Type ID', 'Government Payments', 'Local Supplier Spending', 'Foreign Supplier Spending', 'Employee Wages & Benefits', 'Community Investments', 'Depreciation', 'Depletion', 'Others']
        filename = 'economic_expenditures_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")

@router.get("/template-capital-provider")
@office_checker_only
async def download_economic_capital_provider_template():
    """Generate Excel template for economic capital provider payments data"""
    try:
        headers = ['Year', 'Interest', 'Dividends to NCI', 'Dividends to Parent']
        filename = 'economic_capital_provider_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")

# Import routes using helper function
@router.post("/import-generated")
@office_checker_only
async def import_economic_generated_data(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)
):
    """Import economic generated data from Excel file"""
    config = {
        'expected_columns': {
            'year': ['Year'],
            'electricity_sales': ['Electricity Sales'],
            'oil_revenues': ['Oil Revenues'],
            'other_revenues': ['Other Revenues'],
            'interest_income': ['Interest Income'],
            'share_in_net_income_of_associate': ['Share in Net Income of Associate'],
            'miscellaneous_income': ['Miscellaneous Income']
        },
        'required_fields': ['year'],
        'insert_query': """
            INSERT INTO bronze.econ_value (
                year, electricity_sales, oil_revenues, other_revenues, interest_income,
                share_in_net_income_of_associate, miscellaneous_income
            ) VALUES (
                :year, :electricity_sales, :oil_revenues, :other_revenues, :interest_income,
                :share_in_net_income_of_associate, :miscellaneous_income
            )
            ON CONFLICT (year) 
            DO UPDATE SET
                electricity_sales = EXCLUDED.electricity_sales,
                oil_revenues = EXCLUDED.oil_revenues,
                other_revenues = EXCLUDED.other_revenues,
                interest_income = EXCLUDED.interest_income,
                share_in_net_income_of_associate = EXCLUDED.share_in_net_income_of_associate,
                miscellaneous_income = EXCLUDED.miscellaneous_income
        """
    }
    
    return await process_excel_import(file, config, db)

@router.post("/import-expenditures")
@office_checker_only
async def import_economic_expenditures_data(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)
):
    """Import economic expenditures data from Excel file"""
    config = {
        'expected_columns': {
            'company_id': ['Company ID'],
            'year': ['Year'],
            'type_id': ['Type ID'],
            'government_payments': ['Government Payments'],
            'supplier_spending_local': ['Local Supplier Spending'],
            'supplier_spending_abroad': ['Foreign Supplier Spending'],
            'employee_wages_benefits': ['Employee Wages & Benefits'],
            'community_investments': ['Community Investments'],
            'depreciation': ['Depreciation'],
            'depletion': ['Depletion'],
            'others': ['Others']
        },
        'required_fields': ['company_id', 'year', 'type_id'],
        'validate_expenditures': True,
        'insert_query': """
            INSERT INTO bronze.econ_expenditures (
                year, company_id, type_id, government_payments, supplier_spending_local,
                supplier_spending_abroad, employee_wages_benefits, community_investments,
                depreciation, depletion, others
            ) VALUES (
                :year, :company_id, :type_id, :government_payments, :supplier_spending_local,
                :supplier_spending_abroad, :employee_wages_benefits, :community_investments,
                :depreciation, :depletion, :others
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
        """
    }
    
    return await process_excel_import(file, config, db)

@router.post("/import-capital-provider")
@office_checker_only
async def import_economic_capital_provider_data(
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)
):
    """Import economic capital provider payments data from Excel file"""
    config = {
        'expected_columns': {
            'year': ['Year'],
            'interest': ['Interest'],
            'dividends_to_nci': ['Dividends to NCI'],
            'dividends_to_parent': ['Dividends to Parent']
        },
        'required_fields': ['year'],
        'insert_query': """
            INSERT INTO bronze.econ_capital_provider_payment (
                year, interest, dividends_to_nci, dividends_to_parent
            ) VALUES (
                :year, :interest, :dividends_to_nci, :dividends_to_parent
            )
            ON CONFLICT (year) 
            DO UPDATE SET
                interest = EXCLUDED.interest,
                dividends_to_nci = EXCLUDED.dividends_to_nci,
                dividends_to_parent = EXCLUDED.dividends_to_parent
        """
    }
    
    return await process_excel_import(file, config, db)

@router.get("/reference-data", response_model=Dict)
@office_checker_only
def get_reference_data(db: Session = Depends(get_db)):
    """
    Get reference data for companies and expenditure types
    Returns valid IDs and names that can be used in templates
    """
    try:
        # Get company reference data
        company_result = db.execute(text("""
            SELECT company_id, company_name 
            FROM ref.company_main 
            ORDER BY company_id
        """))
        
        companies = [
            {
                'id': row.company_id,
                'name': row.company_name
            }
            for row in company_result
        ]
        
        # Get expenditure type reference data
        type_result = db.execute(text("""
            SELECT type_id, type_description 
            FROM ref.expenditure_type 
            ORDER BY type_id
        """))
        
        types = [
            {
                'id': row.type_id,
                'description': row.type_description
            }
            for row in type_result
        ]
        
        return {
            'companies': companies,
            'expenditure_types': types
        }
        
    except Exception as e:
        logging.error(f"Error fetching reference data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard/summary", response_model=List[Dict])
@require_role("R02", "R03")
def get_economic_summary(
    years: str = None,
    order_by: str = "year", 
    order_direction: str = "ASC",
    db: Session = Depends(get_db)
):
    """
    Get economic value summary using gold.func_economic_value_by_year
    """
    try:
        # Parse years parameter
        year_array = None
        if years:
            year_list = [int(y.strip()) for y in years.split(',') if y.strip()]
            if year_list:
                year_array = f"ARRAY{year_list}::SMALLINT[]"
        
        query = f"""
            SELECT * FROM gold.func_economic_value_by_year(
                {year_array if year_array else 'NULL'},
                '{order_by}',
                '{order_direction}'
            )
        """
        
        result = db.execute(text(query))
        
        data = [
            {
                'year': row.year,
                'totalGenerated': float(row.total_economic_value_generated) if row.total_economic_value_generated else 0,
                'totalDistributed': float(row.total_economic_value_distributed) if row.total_economic_value_distributed else 0,
                'valueRetained': float(row.economic_value_retained) if row.economic_value_retained else 0
            }
            for row in result
        ]
        
        return data
    except Exception as e:
        logging.error(f"Error fetching economic summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard/generated-details", response_model=List[Dict])
@require_role("R02", "R03")
def get_generated_details(
    years: str = None,
    order_by: str = "year",
    order_direction: str = "ASC",
    db: Session = Depends(get_db)
):
    """
    Get economic value generated details using gold.func_economic_value_generated_details
    """
    try:
        year_array = None
        if years:
            year_list = [int(y.strip()) for y in years.split(',') if y.strip()]
            if year_list:
                year_array = f"ARRAY{year_list}::SMALLINT[]"
        
        query = f"""
            SELECT * FROM gold.func_economic_value_generated_details(
                {year_array if year_array else 'NULL'},
                '{order_by}',
                '{order_direction}'
            )
        """
        
        result = db.execute(text(query))
        
        data = [
            {
                'year': row.year,
                'electricitySales': float(row.electricity_sales) if row.electricity_sales else 0,
                'oilRevenues': float(row.oil_revenues) if row.oil_revenues else 0,
                'otherRevenues': float(row.other_revenues) if row.other_revenues else 0,
                'interestIncome': float(row.interest_income) if row.interest_income else 0,
                'shareInNetIncomeOfAssociate': float(row.share_in_net_income_of_associate) if row.share_in_net_income_of_associate else 0,
                'miscellaneousIncome': float(row.miscellaneous_income) if row.miscellaneous_income else 0,
                'totalGenerated': float(row.total_economic_value_generated) if row.total_economic_value_generated else 0
            }
            for row in result
        ]
        
        return data
    except Exception as e:
        logging.error(f"Error fetching generated details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard/distributed-details", response_model=List[Dict])
@require_role("R02", "R03")
def get_distributed_details(
    years: str = None,
    order_by: str = "year",
    order_direction: str = "ASC",
    db: Session = Depends(get_db)
):
    """
    Get economic value distributed details using gold.func_economic_value_distributed_details
    """
    try:
        year_array = None
        if years:
            year_list = [int(y.strip()) for y in years.split(',') if y.strip()]
            if year_list:
                year_array = f"ARRAY{year_list}::SMALLINT[]"
        
        query = f"""
            SELECT * FROM gold.func_economic_value_distributed_details(
                {year_array if year_array else 'NULL'},
                '{order_by}',
                '{order_direction}'
            )
        """
        
        result = db.execute(text(query))
        
        data = [
            {
                'year': row.year,
                'governmentPayments': float(row.total_government_payments) if row.total_government_payments else 0,
                'localSupplierSpending': float(row.total_local_supplier_spending) if row.total_local_supplier_spending else 0,
                'foreignSupplierSpending': float(row.total_foreign_supplier_spending) if row.total_foreign_supplier_spending else 0,
                'employeeWagesBenefits': float(row.total_employee_wages_benefits) if row.total_employee_wages_benefits else 0,
                'communityInvestments': float(row.total_community_investments) if row.total_community_investments else 0,
                'depreciation': float(row.total_depreciation) if row.total_depreciation else 0,
                'depletion': float(row.total_depletion) if row.total_depletion else 0,
                'otherExpenditures': float(row.total_other_expenditures) if row.total_other_expenditures else 0,
                'capitalProviderPayments': float(row.total_capital_provider_payments) if row.total_capital_provider_payments else 0,
                'totalDistributed': float(row.total_economic_value_distributed) if row.total_economic_value_distributed else 0
            }
            for row in result
        ]
        
        return data
    except Exception as e:
        logging.error(f"Error fetching distributed details: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard/company-distribution", response_model=List[Dict])
@require_role("R02", "R03")
def get_company_distribution(
    companies: str = None,
    years: str = None,
    order_by: str = "percentage_of_total_distribution",
    order_direction: str = "DESC",
    db: Session = Depends(get_db)
):
    """
    Get economic value distribution by company using gold.func_economic_value_distribution_percentage
    """
    try:
        company_array = None
        if companies:
            company_list = [c.strip() for c in companies.split(',') if c.strip()]
            if company_list:
                company_array = f"ARRAY{company_list}::VARCHAR(10)[]"
        
        year_array = None
        if years:
            year_list = [int(y.strip()) for y in years.split(',') if y.strip()]
            if year_list:
                year_array = f"ARRAY{year_list}::SMALLINT[]"
        
        query = f"""
            SELECT * FROM gold.func_economic_value_distribution_percentage(
                {company_array if company_array else 'NULL'},
                {year_array if year_array else 'NULL'},
                '{order_by}',
                '{order_direction}'
            )
        """
        
        result = db.execute(text(query))
        
        data = [
            {
                'year': row.year,
                'companyName': row.company_name,
                'totalDistributed': float(row.total_economic_value_distributed_by_company) if row.total_economic_value_distributed_by_company else 0,
                'percentage': float(row.percentage_of_total_distribution) if row.percentage_of_total_distribution else 0
            }
            for row in result
        ]
        
        return data
    except Exception as e:
        logging.error(f"Error fetching company distribution: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard/expenditure-by-company", response_model=List[Dict])
@require_role("R02", "R03")
def get_expenditure_by_company(
    companies: str = None,
    types: str = None,
    years: str = None,
    order_by: str = "year",
    order_direction: str = "ASC",
    db: Session = Depends(get_db)
):
    """
    Get expenditure details by company using gold.func_economic_expenditure_by_company
    """
    try:
        company_array = None
        if companies:
            company_list = [c.strip() for c in companies.split(',') if c.strip()]
            if company_list:
                company_array = f"ARRAY{company_list}::VARCHAR(10)[]"
        
        type_array = None
        if types:
            type_list = [t.strip() for t in types.split(',') if t.strip()]
            if type_list:
                type_array = f"ARRAY{type_list}::VARCHAR(10)[]"
        
        year_array = None
        if years:
            year_list = [int(y.strip()) for y in years.split(',') if y.strip()]
            if year_list:
                year_array = f"ARRAY{year_list}::SMALLINT[]"
        
        query = f"""
            SELECT * FROM gold.func_economic_expenditure_by_company(
                {company_array if company_array else 'NULL'},
                {type_array if type_array else 'NULL'},
                {year_array if year_array else 'NULL'},
                '{order_by}',
                '{order_direction}'
            )
        """
        
        result = db.execute(text(query))
        
        data = [
            {
                'year': row.year,
                'companyName': row.company_name,
                'typeId': row.type_id,
                'governmentPayments': float(row.government_payments) if row.government_payments else 0,
                'localSupplierSpending': float(row.local_supplier_spending) if row.local_supplier_spending else 0,
                'foreignSupplierSpending': float(row.foreign_supplier_spending) if row.foreign_supplier_spending else 0,
                'employeeWagesBenefits': float(row.employee_wages_benefits) if row.employee_wages_benefits else 0,
                'communityInvestments': float(row.community_investments) if row.community_investments else 0,
                'depreciation': float(row.depreciation) if row.depreciation else 0,
                'depletion': float(row.depletion) if row.depletion else 0,
                'otherExpenditures': float(row.other_expenditures) if row.other_expenditures else 0,
                'totalDistributed': float(row.total_distributed_value_by_company) if row.total_distributed_value_by_company else 0
            }
            for row in result
        ]
        
        return data
    except Exception as e:
        logging.error(f"Error fetching expenditure by company: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dashboard/filter-options", response_model=Dict)
@require_role("R02", "R03")
def get_dashboard_filter_options(
    db: Session = Depends(get_db)
):
    """
    Get available filter options for the dashboard
    """
    try:
        # Get available years
        years_result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.vw_economic_value_summary 
            ORDER BY year DESC
        """))
        years = [row.year for row in years_result]
        
        # Get available companies
        companies_result = db.execute(text("""
            SELECT DISTINCT company_id, company_name 
            FROM ref.company_main 
            ORDER BY company_name
        """))
        companies = [
            {'id': row.company_id, 'name': row.company_name}
            for row in companies_result
        ]
        
        # Get available expenditure types
        types_result = db.execute(text("""
            SELECT DISTINCT type_id, type_description 
            FROM ref.expenditure_type 
            ORDER BY type_description
        """))
        types = [
            {'id': row.type_id, 'description': row.type_description}
            for row in types_result
        ]
        
        return {
            'years': years,
            'companies': companies,
            'expenditureTypes': types
        }
        
    except Exception as e:
        logging.error(f"Error fetching filter options: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 