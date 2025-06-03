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
    HRTenure
)
from app.bronze.schemas import (
    HRDemographicsOut,
    HRTenureOut
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
            SELECT demo.*, tenure.*, csl.status_id
            FROM silver.hr_demographics demo
            JOIN silver.hr_tenure tenure 
                ON demo.employee_id = tenure.employee_id
            JOIN public.checker_status_log csl
                ON demo.employee_id = csl.record_id
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
            SELECT pl.*, csl.status_id
            FROM silver.hr_parental pl
            JOIN public.checker_status_log csl
                ON pl.employee_id = csl.record_id
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

# @router.get("/safety_workdata_records_by_status", response_model=List[dict])
# def get_safety_workdata_records_by_status(
#     status_id: Optional[str] = Query(None),
#     db: Session = Depends(get_db)
# ):
#     try:
#         logging.info(f"Fetching safety workdata records. Filter status_id: {status_id}")

#         query = text("""
#             SELECT swd.*, csl.status_id
#             FROM silver.hr_safety_workdata swd
#             JOIN public.checker_status_log csl
#                 ON pl.employee_id = csl.record_id
#             WHERE (:status_id IS NULL OR csl.status_id = :status_id)
#         """)

#         result = db.execute(query, {"status_id": status_id})
#         data = [dict(row._mapping) for row in result]

#         logging.info(f"Returned {len(data)} records")
#         return data

#     except Exception as e:
#         logging.error(f"Error retrieving employability records: {str(e)}")
#         logging.error(traceback.format_exc())
#         raise HTTPException(status_code=500, detail="Internal server error")

#=================RETRIEVE HR DATA (BRONZE)=================
@router.get("/get_employee_demographics_by_id/{employee_id}", response_model=HRDemographicsOut)
def get_employee_demographics_by_id(employee_id: str, db: Session = Depends(get_db)):
    record = get_one(db, HRDemographics, "employee_id", employee_id)
    if not record:
       raise HTTPException(status_code=404, detail="Demographic record not found")
    return record
