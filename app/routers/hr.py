from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.dependencies import get_db
import logging
import traceback

from app.crud.base import get_all, get_many_filtered, get_one
from app.bronze.crud import (
    HRDemographics,
    HRTenure,
    HROsh,
    HRParentalLeave,
    HRSafetyWorkdata,
    HRTraining
)
from app.bronze.schemas import (
    HRDemographicsOut,
    HRTenureOut,
    HROshOut,
    HRParentalLeaveOut,
    HRSafetyWorkdataOut,
    HRTrainingOut,
    EmployabilityCombinedOut,
    AddEmployabilityRecord,
    AddSafetyWorkdataRecord
)


router = APIRouter()

@router.get("/gender_dist_by_position", response_model=List[dict])
def get_gender_dist_by_position(
    # year: Optional[List[int]] = Query(None, alias="p_year"),
    db: Session = Depends(get_db)
):
    """
    Get the Active count of employees per Gender
    """
    try:
        logging.info("Executing Active count of employees per Gender query")
        
        result = db.execute(text("""
            SELECT
                COUNT(DISTINCT employee_id) AS total_active_employees
            FROM gold.func_employee_summary_yearly(NULL, NULL, NULL, NULL, ARRAY[EXTRACT(YEAR FROM CURRENT_DATE)::INT]) AS f;
        """))

        data = [
            {
                "total_active_employees": row.total_active_employees
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/attrition_rate", response_model=List[dict])
def get_gender_dist_by_position(
    # year: Optional[List[int]] = Query(None, alias="p_year"),
    db: Session = Depends(get_db)
):
    """
    Get the Attrition rate
    """
    try:
        logging.info("Executing Attrition rate query")
        
        result = db.execute(text("""
            WITH all_years AS (
            SELECT
                MIN(EXTRACT(YEAR FROM start_date))::INT AS min_year,
                -- MAX(EXTRACT(YEAR FROM COALESCE(end_date, CURRENT_DATE)))::INT AS max_year -- TO CURRENT DATE
                MAX(EXTRACT(YEAR FROM end_date))::INT AS max_year -- ONLY INCLUDE BASED ON DATASET
            FROM gold.dim_employee_descriptions
            ),
            year_array AS (
                SELECT ARRAY(
                    SELECT generate_series(min_year, max_year)
                    FROM all_years
                ) AS years
            ),
            function_table AS (
                SELECT * 
                FROM gold.func_hr_rate_summary_yearly(NULL, NULL, NULL, (SELECT years FROM year_array))
            )
            SELECT
                year,
                SUM(total_employees) AS total_employees,
                --ROUND(AVG(avg_tenure), 2) AS avg_tenure,
                SUM(resigned_count) AS resigned_count,
                CASE
                    WHEN SUM(total_employees) > 0 THEN ROUND((SUM(resigned_count)::NUMERIC / SUM(total_employees)) * 100, 2)
                    ELSE NULL
                END AS attrition_rate_percent
            FROM function_table
            GROUP BY year
            ORDER BY year;
        """))

        data = [
            {
                "year": row.year,
                "total_employees": row.total_employees,
                "resigned_count": row.resigned_count,
                "attrition_rate_percent": row.attrition_rate_percent,
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# =================EMPLOYABILITY RECORD BY STATUS=================
@router.get("/employability_records_by_status", response_model=List[dict])
def get_employability_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching employability records. Filter status_id: {status_id}")

        query = text("""
            SELECT demo.*, tenure.*, cm.company_name, csl.status_id
            FROM silver.hr_demographics demo
            JOIN silver.hr_tenure tenure 
                ON demo.employee_id = tenure.employee_id
            JOIN public.checker_status_log csl
                ON demo.employee_id = csl.record_id
            JOIN ref.company_main cm
                ON demo.company_id = cm.company_id
            WHERE (:status_id IS NULL OR csl.status_id = :status_id)
        """)

        result = db.execute(query, {"status_id": status_id})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving employability records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

# =================PARENTAL LEAVE RECORD BY STATUS=================
@router.get("/parental_leave_records_by_status", response_model=List[dict])
def get_parental_leave_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching parental leave records. Filter status_id: {status_id}")

        query = text("""
            SELECT pl.*, cm.company_name, csl.status_id
            FROM silver.hr_parental_leave pl
            JOIN public.checker_status_log csl
                ON pl.parental_leave_id = csl.record_id
            JOIN silver.hr_demographics demo
                ON pl.employee_id = demo.employee_id
            JOIN ref.company_main cm
                ON demo.company_id = cm.company_id
            WHERE (:status_id IS NULL OR csl.status_id = :status_id)
        """)

        result = db.execute(query, {"status_id": status_id})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving parental leave records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
# =================SAFETY WORKDATA RECORD BY STATUS================= 
@router.get("/safety_workdata_records_by_status", response_model=List[dict])
def get_safety_workdata_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching safety workdata records. Filter status_id: {status_id}")

        query = text("""
            SELECT swd.*, cm.company_name, csl.status_id
            FROM silver.hr_safety_workdata swd
            JOIN public.checker_status_log csl
                ON swd.safety_workdata_id = csl.record_id
            JOIN ref.company_main cm
                ON swd.company_id = cm.company_id
            WHERE (:status_id IS NULL OR csl.status_id = :status_id)
        """)

        result = db.execute(query, {"status_id": status_id})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving safety workdata records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
# =================OCCUPATIONAL SAFETY HEALTH RECORD BY STATUS================= 
@router.get("/occupational_safety_health_records_by_status", response_model=List[dict])
def get_occupational_safety_health_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching occupational safety health records. Filter status_id: {status_id}")

        query = text("""
            SELECT osh.*, cm.company_name, csl.status_id
            FROM silver.hr_occupational_safety_health osh
            JOIN public.checker_status_log csl
                ON osh.osh_id = csl.record_id
            JOIN ref.company_main cm
                ON osh.company_id = cm.company_id
            WHERE (:status_id IS NULL OR csl.status_id = :status_id)
        """)

        result = db.execute(query, {"status_id": status_id})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving occupational safety health records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
# =================TRAINING RECORD BY STATUS================= 
@router.get("/training_records_by_status", response_model=List[dict])
def get_training_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching training records Filter status_id: {status_id}")

        query = text("""
            SELECT tr.*, cm.company_name, csl.status_id
            FROM silver.hr_training tr
            JOIN public.checker_status_log csl
                ON tr.training_id = csl.record_id
            JOIN ref.company_main cm
                ON tr.company_id = cm.company_id
            WHERE (:status_id IS NULL OR csl.status_id = :status_id)
        """)

        result = db.execute(query, {"status_id": status_id})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving training records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

#=================RETRIEVE HR DATA BY ID(BRONZE)=================
@router.get("/get_employability_combined_by_id/{employee_id}", response_model=EmployabilityCombinedOut)
def get_employability_combined_by_id(employee_id: str, db: Session = Depends(get_db)):
    demographics = get_one(db, HRDemographics, "employee_id", employee_id)
    if not demographics:
        raise HTTPException(status_code=404, detail="Demographic record not found")
    
    tenure = get_one(db, HRTenure, "employee_id", employee_id)
    if not tenure:
        raise HTTPException(status_code=404, detail="Tenure record not found")
    
    return EmployabilityCombinedOut(demographics=demographics, tenure=tenure)
'''
@router.get("/get_hr_osh_by_id/{osh_id}", response_model=HROshOut)
def get_hr_osh_by_id(osh_id: str, db: Session = Depends(get_db)):
    record = get_one(db, HROsh, "osh_id", osh_id)
    if not record:
        raise HTTPException(status_code=404, detail="Occupational Safety and Health record not found")
    return record

@router.get("/get_hr_parental_leave_by_id/{parental_leave_id}", response_model=HRParentalLeaveOut)
def get_hr_parental_leave_by_id(parental_leave_id: str, db: Session = Depends(get_db)):
    record = get_one(db, HRParentalLeave, "parental_leave_id", parental_leave_id)
    if not record:
        raise HTTPException(status_code=404, detail="Parental Leave record not found")
    return record

@router.get("/get_hr_safety_workdata_by_id/{safety_workdata_id}", response_model=HRSafetyWorkdataOut)
def get_hr_safety_workdata_by_id(safety_workdata_id: str, db: Session = Depends(get_db)):
    record = get_one(db, HRSafetyWorkdata, "safety_workdata_id", safety_workdata_id)
    if not record:
        raise HTTPException(status_code=404, detail="Safety Workdata record not found")
    return record

@router.get("/get_hr_training_by_id/{training_id}", response_model=HRTrainingOut)
def get_hr_training_by_id(training_id: str, db: Session = Depends(get_db)):
    record = get_one(db, HRTraining, "training_id", training_id)
    if not record:
        raise HTTPException(status_code=404, detail="Training record not found")
    return record
'''

# ====================== ADD RECORD ====================== 
# --- EMPLOYABILITY ---
@router.post("/add_employability_record")
def add_employability_record(record: AddEmployabilityRecord, db: Session = Depends(get_db)):
    new_record = AddEmployabilityRecord(
        employee_id=record.demographics.employee_id,
        gender=record.demographics.gender,
        birthdate=record.demographics.birthdate,
        position_id=record.demographics.position_id,
        p_np=record.demographics.p_np,
        company_id=record.demographics.company_id,
        employment_status=record.demographics.employment_status,
        start_date=record.tenure.start_date,
        end_date=record.tenure.end_date,
    )
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    
    # update silver
    db.execute(text("CALL silver.load_hr_silver();"))
    db.commit()
    return new_record
# --- SAFETY WORKDATA ---
@router.post("/add_safety_workdata_record")
def add_safety_workdata_record(record: AddSafetyWorkdataRecord, db: Session = Depends(get_db)):
    new_record = AddSafetyWorkdataRecord(
        company_id=record.company_id,
        contractor=record.contractor,
        date=record.date,
        manpower=record.manpower,
        manhours=record.manhours

    )
    db.add(new_record)
    db.commit()
    db.refresh(new_record)
    
    # update silver
    db.execute(text("CALL silver.load_hr_silver();"))
    db.commit()
    return new_record