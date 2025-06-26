from fastapi import APIRouter, Depends, Query, HTTPException, Request, UploadFile, File
import pandas as pd
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict, Union
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.dependencies import get_db
from io import BytesIO
import logging
import traceback
import io

from app.crud.base import get_all, get_many_filtered, get_one
from app.bronze.crud import (
    HRDemographics,
    HRTenure,
    HROsh,
    HRParentalLeave,
    HRSafetyWorkdata,
    HRTraining,
    # SINGLE INSERT FUNCTIONS
    insert_employability,
    insert_safety_workdata,
    insert_parental_leave,
    insert_occupational_safety_health,
    insert_training,
    # BULK INSERT FUNCTIONS
    insert_employability_bulk,
    insert_parental_leave_bulk,
    insert_safety_workdata_bulk,
    insert_occupational_safety_health_bulk,
    insert_training_bulk,
    # UPDATE FUNCTIONS
    update_employability,
    update_safety_workdata,
    update_parental_leave,
    update_occupational_safety_health,
    update_training,
)
from app.bronze.schemas import (
    HRDemographicsOut,
    HRTenureOut,
    HROshOut,
    HRParentalLeaveOut,
    HRSafetyWorkdataOut,
    HRTrainingOut,
    EmployabilityCombinedOut
)

from datetime import datetime

router = APIRouter()

# Function to create a template
def create_excel_template(headers: List[str], filename: str) -> io.BytesIO:
    df = pd.DataFrame({header: [] for header in headers})
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
        worksheet = writer.sheets['Sheet1']
        
        for column in worksheet.columns:
            max_length = len(str(column[0].value)) + 2
            worksheet.column_dimensions[column[0].column_letter].width = max_length
    
    output.seek(0)
    return output

# ====================== DASHBOARD OVERVIEW ======================
@router.get("/overview_safety_manhours", response_model=List[dict])
def get_overview_safety_manhours(
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Overview Safety Manhours query")
            
        result = db.execute(text("""
            SELECT SUM(manhours) AS total_safety_manhours
            FROM gold.func_safety_workdata_summary();
        """))
        data = [{ "total_safety_manhours": row.total_safety_manhours } 
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
    
@router.get("/overview_safety_manpower", response_model=List[dict])
def get_overview_safety_manpower(
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Overview Safety Manhours query")
            
        result = db.execute(text("""
            SELECT SUM(manpower) AS total_safety_manpower
            FROM gold.func_safety_workdata_summary();
    
        """))
        data = [{ "total_safety_manpower": row.total_safety_manpower } 
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

@router.get("/overview_training", response_model=List[dict])
def get_overview_training(
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Overview Safety Manhours query")
            
        result = db.execute(text("""
            SELECT SUM(training_hours) AS total_training_hours
            FROM gold.func_training_summary();
            
        """))
        data = [{ "total_training_hours": row.total_training_hours } 
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

# ====================== DASHBOARD ======================
# ===== KPIs =====
@router.get("/total_safety_manhours", response_model=List[dict])
def get_total_safety_manhours(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Total Safety Manhours query")
        
        if start_date is None:
            # Set to January 1st of the current year
            start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        
        if end_date is None:
            # Set to today's date
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        result = db.execute(text("""
            SELECT SUM(manhours) AS total_safety_manhours
            FROM gold.func_safety_workdata_summary(
                :start_date,         -- p_start_date
                :end_date,         -- p_end_date
                :company_id,         -- p_company_id
                NULL                  -- p_contractor
            )
        """), {
                'start_date': start_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None,
        })

        data = [
            {
                "total_safety_manhours": row.total_safety_manhours
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

@router.get("/total_safety_manpower", response_model=List[dict])
def get_total_safety_manhours(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Total Safety Manpower query")
        
        if start_date is None:
            # Set to January 1st of the current year
            start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        
        if end_date is None:
            # Set to today's date
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        result = db.execute(text("""
            SELECT SUM(manpower) AS total_safety_manpower
            FROM gold.func_safety_workdata_summary(
                :start_date,         -- p_start_date
                :end_date,         -- p_end_date
                :company_id,         -- p_company_id
                NULL                  -- p_contractor
            )
        """), {
                'start_date': start_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None,
        })

        data = [
            {
                "total_safety_manpower": row.total_safety_manpower
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

@router.get("/total_training_hours", response_model=List[dict])
def get_total_training_hours(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Total training hours query")
        
        if start_date is None:
            # Set to January 1st of the current year
            start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        
        if end_date is None:
            # Set to today's date
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        result = db.execute(text("""
            SELECT SUM(training_hours) AS total_training_hours
                FROM gold.func_training_summary(
                :start_date, :end_date, :company_id, NULL
                )
                """), {
                'start_date': start_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None,
        })

        data = [
            {
                "total_training_hours": row.total_training_hours
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

@router.get("/no_lost_time", response_model=List[dict])
def get_no_lost_time(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing No Lost Time query")
        
        # if start_date is None:
        #     # Set to January 1st of the current year
        #     start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        
        # Get the latest LTI
        lti_result = db.execute(text("""
            SELECT MAX(TO_DATE(year || '-' || month_value || '-01', 'YYYY-MM-DD')) AS latest_lti_date
            FROM gold.func_occupational_safety_health_summary(
                NULL, CURRENT_DATE, :company_id, NULL, TRUE, NULL, NULL
            )
            WHERE incident_count > 0;
        """), {
            'company_id': company_id.split(',') if company_id else None
        })
        
        latest_lti_date = lti_result.scalar()
        
        if latest_lti_date is None:
            latest_lti_date = datetime(datetime.now().year, 1, 1).date()
            logging.info(f"No LTI found. Using default start date: {latest_lti_date}")
        else:
            logging.info(f"Latest LTI found. Using start date: {latest_lti_date}")
        
        if end_date is None:
            # Set to today's date
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        result = db.execute(text("""
            SELECT SUM(manhours) AS manhours_since_last_lti
            FROM gold.func_safety_workdata_summary(
                :start_date, :end_date, :company_id, NULL
            );
                """), {
                'start_date': latest_lti_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None,
        })

        data = [
            {
                "manhours_since_last_lti": row.manhours_since_last_lti
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
    
# ===== CHARTS =====
@router.get("/employee_count_per_company", response_model=List[dict])
def get_employee_count_per_company(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Active count of employees per Gender query")
        
        if start_date is None:
            start_date = None
        
        if end_date is None:
            end_date = None
            
        result = db.execute(text("""
            SELECT 
                comp.company_name,
                comp.company_id,
                comp.color,
                COUNT(*) AS num_employees
            FROM gold.func_employee_summary(
                NULL, NULL, NULL, :company_id, :start_date, :end_date
            ) emp
            JOIN ref.company_main comp ON comp.company_id = emp.company_id
            GROUP BY comp.company_name, comp.company_id, comp.color
            ORDER BY comp.company_name;
        """), {
                'start_date': start_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None
        })

        data = [
            {
                "company_name": row.company_name,
                "company_id": row.company_id,
                "color": row.color,
                "num_employees": row.num_employees
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

@router.get("/gender_distribution_per_position", response_model=List[dict])
def get_gender_distribution_per_position(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get the Total active employees count
    """
    try:
        logging.info("Executing Active count of employees per Gender query")
        
        if start_date is None:
            start_date = None
        
        if end_date is None:
            end_date = None
            
        result = db.execute(text("""
            SELECT
                position_id AS position,
                COUNT(*) FILTER (WHERE gender = 'M') AS male,
                COUNT(*) FILTER (WHERE gender = 'F') AS female
            FROM gold.func_employee_summary(
            NULL, NULL, NULL, :company_id, :start_date, :end_date
            )
            GROUP BY position_id
            ORDER BY position_id;
        """), {
                'start_date': start_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None,
        })

        data = [
            {
                "position": row.position,
                "male": row.male,
                "female": row.female
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

@router.get("/age_distribution", response_model=List[dict])
def get_age_distribution(
    grouping: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Age distribution query")
        
        if start_date is None:
            start_date = None
        
        if end_date is None:
            end_date = None
            
        result = db.execute(text("""
            SELECT age_category,
                COUNT(*) AS employee_count
            FROM gold.func_employee_summary(
            NULL, NULL, NULL, :company_id, :start_date, :end_date
            )
            GROUP BY age_category
            ORDER BY age_category;
        """), {
                'start_date': start_date,
                'end_date': end_date,
                'company_id': company_id.split(',') if company_id else None,
        })

        data = [
            {
                "age_category": row.age_category,
                "employee_count": row.employee_count
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


@router.get("/incident_count_per_month", response_model=List[dict])
def get_incident_count_per_month(
    grouping: Optional[str] = Query("monthly", regex="^(monthly|quarterly|yearly)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing Incident count per month query")

        if start_date is None:
            start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Determine grouping
        if grouping == "monthly":
            select_group = "year, month_value, month_name"
            order_by = "year, month_value"
        elif grouping == "quarterly":
            select_group = "year, quarter"
            order_by = "year, quarter"
        else:  # yearly
            select_group = "year"
            order_by = "year"

        sql = f"""
            SELECT {select_group}, osh.company_name, osh.company_id, comp.color, osh.incident_title, SUM(osh.incident_count) AS incidents
            FROM gold.func_occupational_safety_health_summary(
                :start_date, :end_date, :company_id, NULL, NULL, NULL, NULL
            ) osh
            JOIN ref.company_main comp ON comp.company_id = osh.company_id
            GROUP BY {select_group}, osh.company_name, osh.company_id, comp.color, osh.incident_title
            ORDER BY {order_by}, osh.company_name, osh.company_id, osh.incident_title;
        """

        result = db.execute(text(sql), {
            'start_date': start_date,
            'end_date': end_date,
            'company_id': company_id.split(',') if company_id else None
        })

        data = []
        for row in result:
            item = {
                "year": row.year,
                "company_name": row.company_name,
                "company_id": row.company_id,
                "color": row.color,
                "incident_title": row.incident_title,
                "incidents": int(row.incidents)
            }
            if grouping == "monthly":
                item["month"] = row.month_value
                item["month_name"] = row.month_name
            elif grouping == "quarterly":
                item["quarter"] = row.quarter
            data.append(item)

        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")

        if not data:
            logging.warning("No data found")
            return []

        return data

    except Exception as e:
        logging.error(f"Error fetching incident count: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/safety_manhours_per_month", response_model=List[dict])
def get_safety_manhours_per_month(
    grouping: Optional[str] = Query("monthly", regex="^(monthly|quarterly|yearly)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing safety manhours grouped query")

        if start_date is None:
            start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Define grouping
        if grouping == "monthly":
            select_group = "year, month_value, month_name"
            order_by = "year, month_value"
        elif grouping == "quarterly":
            select_group = "year, quarter"
            order_by = "year, quarter"
        else:  # yearly
            select_group = "year"
            order_by = "year"

        # Dynamically build SQL
        sql = f"""
            SELECT 
                {select_group},
                sw.company_name,
                sw.company_id,
                comp.color,
                SUM(sw.manhours) AS manhours
            FROM gold.func_safety_workdata_summary(
                :start_date, :end_date, :company_id, NULL
            ) sw
            JOIN ref.company_main comp ON comp.company_id = sw.company_id
            GROUP BY {select_group}, sw.company_name, sw.company_id, comp.color
            ORDER BY {order_by}, sw.company_name, sw.company_id;
        """

        # Execute with parameters
        result = db.execute(text(sql), {
            'start_date': start_date,
            'end_date': end_date,
            'company_id': company_id.split(',') if company_id else None
        })

        # Format response
        data = []
        for row in result:
            item = {
                "year": row.year,
                "company_name": row.company_name,
                "company_id": row.company_id,
                "color":row.color,
                "manhours": int(row.manhours)
            }
            if grouping == "monthly":
                item["month"] = row.month_value
                item["month_name"] = row.month_name
            elif grouping == "quarterly":
                item["quarter"] = row.quarter
            data.append(item)

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


@router.get("/safety_manpower_per_month", response_model=List[dict])
def get_safety_manpower_per_month(
    grouping: Optional[str] = Query("monthly", regex="^(monthly|quarterly|yearly)$"),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    company_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info("Executing safety manpower per month query")
        
        if start_date is None:
            start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%d")
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        # Handle grouping logic
        if grouping == "monthly":
            select_group = "year, month_value, month_name"
            order_by = "year, month_value"
        elif grouping == "quarterly":
            select_group = "year, quarter"
            order_by = "year, quarter"
        else:  # yearly
            select_group = "year"
            order_by = "year"

        # Build SQL dynamically using f-string
        sql = f"""
            SELECT 
                {select_group},
                sw.company_name,
                sw.company_id,
                comp.color,
                SUM(sw.manpower) AS total_monthly_safety_manpower
            FROM gold.func_safety_workdata_summary(
                :start_date, :end_date, :company_id, NULL
            ) sw
            JOIN ref.company_main comp ON comp.company_id = sw.company_id
            GROUP BY {select_group}, sw.company_name, sw.company_id, comp.color
            ORDER BY {order_by}, sw.company_name, sw.company_id;

        """

        result = db.execute(text(sql), {
            'start_date': start_date,
            'end_date': end_date,
            'company_id': company_id.split(',') if company_id else None
        })

        # Process the data based on grouping
        data = []
        for row in result:
            item = {
                "year": row.year,
                "company_name": row.company_name,
                "company_id": row.company_id,
                "color": row.color,
                "total_monthly_safety_manpower": int(row.total_monthly_safety_manpower)
            }
            if grouping == "monthly":
                item["month"] = row.month_value
                item["month_name"] = row.month_name
            elif grouping == "quarterly":
                item["quarter"] = row.quarter
            data.append(item)

        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")

        if not data:
            logging.warning("No data found")
            return []

        return data

    except Exception as e:
        logging.error(f"Error fetching safety manpower: {str(e)}")
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
                ON tenure.employee_id = demo.employee_id
            JOIN (
                SELECT DISTINCT ON (record_id) record_id, status_id
                FROM public.record_status
                ORDER BY record_id, status_timestamp DESC
            ) csl
                ON demo.employee_id = csl.record_id
            JOIN public.status st 
                ON st.status_id = csl.status_id
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
            SELECT pl.*, demo.company_id, cm.company_name, csl.status_id
            FROM silver.hr_parental_leave pl
            JOIN public.record_status csl
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
            JOIN public.record_status csl
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
            JOIN public.record_status csl
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
            JOIN public.record_status csl
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


# ====================== ADD SINGLE RECORD ====================== 
# --- EMPLOYABILITY ---
@router.post("/single_upload_employability_record")
def single_upload_employability_record(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single employability record")

        required_fields = ['employee_id', 'gender', 'birthdate', 'position_id', 'p_np', 'company_id', 'employment_status', 'start_date', 'end_date']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["p_np"], str) or not data["p_np"].strip():
            raise HTTPException(status_code=422, detail="Invalid p_np")
        
        if not isinstance(data["gender"], str) or not data["gender"].strip():
            raise HTTPException(status_code=422, detail="Invalid gender")
        
        if not isinstance(data["position_id"], str) or not data["position_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid position_id")

        if data["employment_status"] not in {"Permanent", "Temporary"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['employment_status']}'")

        record = {
            "employee_id": data["employee_id"],
            "gender": data["gender"],
            "birthdate": data["birthdate"],
            "position_id": data["position_id"],
            "p_np": data["p_np"],
            "company_id": data["company_id"],
            "employment_status": data["employment_status"],
            "start_date": data["start_date"],
            "end_date": data["end_date"],
        }

        insert_employability(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
        
# --- SAFETY WORKDATA ---
@router.post("/single_upload_safety_workdata_record")
def single_upload_safety_workdata_record(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single safety workdata record")

        required_fields = ['company_id', 'contractor', 'date', 'manpower', 'manhours']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["contractor"], str) or not data["contractor"].strip():
            raise HTTPException(status_code=422, detail="Invalid contractor")

        record = {
            "company_id": data["company_id"],
            "contractor": data["contractor"],
            "date": data["date"],
            "manpower": int(data["manpower"]),
            "manhours": int(data["manhours"]),
        }

        insert_safety_workdata(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Parental Leave ---
@router.post("/single_upload_parental_leave_record")
def single_upload_parental_leave_record(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single parental leave record")

        required_fields = ['employee_id', 'type_of_leave', 'date', 'days']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        record = {
            "employee_id": data["employee_id"],
            "type_of_leave": data["type_of_leave"],
            "date": data["date"],
            "days": int(data["days"]),
        }

        insert_parental_leave(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Training ---
@router.post("/single_upload_training_record")
def single_upload_training_record(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single training record")

        required_fields = ['company_id', 'date', 'training_title', 'training_hours', 'number_of_participants']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        record = {
            "company_id": data["company_id"],
            "date": data["date"],
            "training_title": data["training_title"],
            "training_hours": int(data["training_hours"]),
            "number_of_participants": int(data["number_of_participants"]),
        }

        insert_training(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        logging.error(traceback.format_exc())
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Occupational Safety Health ---
@router.post("/single_upload_occupational_safety_health_record")
def single_upload_occupational_safety_health_record(data: dict, db: Session = Depends(get_db)):
    try:
        logging.info("Add single occupational safety health record")

        required_fields = ['company_id', 'workforce_type', 'lost_time', 'date', 'incident_type', 'incident_title', 'incident_count']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if data["lost_time"] not in {"TRUE", "FALSE"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['lost_time']}'")

        record = {
            "company_id": data["company_id"],
            "workforce_type": data["workforce_type"],
            "lost_time": data["lost_time"] == "TRUE",
            "date": data["date"],
            "incident_type": data["incident_type"],
            "incident_title": data["incident_title"],
            "incident_count": int(data["incident_count"]),
        }

        # Assuming you have a single insert function
        insert_occupational_safety_health(db, record)

        return {"message": "1 record successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# ====================== ADD BULK RECORD ====================== 
# --- EMPLOYABILITY ---
@router.post("/bulk_upload_employability")
def bulk_upload_employability(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info("Add bulk employability data")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        required_columns = {'employee_id', 'gender', 'birthdate', 'position_id', 'p_np', 'company_id', 'employment_status', 'start_date', 'end_date'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {required_columns - set(df.columns)}"
            )
            
        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number

            # INSERT VALIDATION FOR GENDER, POSITION ID, P_NP, COMPANY ID, AND EMPLOYMENT STATUS
            birthdate_raw = row["birthdate"]
            start_date_raw = row["start_date"]
            end_date_raw = row["end_date"]
         
            # Validate and parse date
            try:
                birthdate = pd.to_datetime(row["birthdate"]).date()
                start_date = pd.to_datetime(row["start_date"]).date()
                if pd.isnull(end_date_raw) or str(end_date_raw).strip() in ["", "NaT", "None"]:
                    end_date = None
                else:
                    end_date = pd.to_datetime(end_date_raw, errors='raise').date()
            except (ValueError, TypeError) as e:
                validation_errors.append(f"Row {row_number}: Invalid date format."
                                         f"birthdate='{birthdate_raw}', start_date='{start_date_raw}', end_date='{end_date_raw}' – Error: {str(e)}")
                continue

            rows.append({
                "employee_id": str(row["employee_id"]).strip(),
                "gender": str(row["gender"]).strip(),
                "birthdate": birthdate,
                "position_id": str(row["position_id"]).strip(),
                "p_np": str(row["p_np"]).strip(),
                "company_id": str(row["company_id"]).strip(),
                "employment_status": str(row["employment_status"]).strip(),
                "start_date": start_date,
                "end_date": end_date
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = insert_employability_bulk(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_upload_safety_workdata")
def bulk_upload_safety_workdata(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info("Add bulk safety workdata")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        required_columns = {'company_id', 'contractor', 'date', 'manpower', 'manhours'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {required_columns - set(df.columns)}"
            )
            
        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number

            # INSERT VALIDATION FOR COMPANY_ID, MANPOWER, AND MANHOURS
            
            # Validate and parse date
            try:
                date = pd.to_datetime(row["date"]).date()
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format.")
                continue

            rows.append({
                "company_id": str(row["company_id"]).strip(),
                "contractor": str(row["contractor"]),
                "date": date,
                "manpower": int(row["manpower"]),
                "manhours": int(row["manhours"])
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = insert_safety_workdata_bulk(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_upload_parental_leave")
def bulk_upload_parental_leave(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info("Add bulk parental leave")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        required_columns = {'employee_id', 'type_of_leave', 'date', 'days'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {required_columns - set(df.columns)}"
            )
            
        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number

            # INSERT VALIDATION FOR COMPANY_ID, MANPOWER, AND MANHOURS
            
            # Validate and parse date
            try:
                date = pd.to_datetime(row["date"]).date()
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format.")
                continue

            rows.append({
                "employee_id": str(row["employee_id"]).strip(),
                "type_of_leave": str(row["type_of_leave"]).strip(),
                "date": date,
                "days": int(row["days"]),
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = insert_parental_leave_bulk(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_upload_training")
def bulk_upload_training(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    try:
        logging.info("Add bulk training")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        required_columns = {'company_id', 'date', 'training_title', 'training_hours', 'number_of_participants'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {required_columns - set(df.columns)}"
            )
            
        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number

            # INSERT VALIDATION FOR COMPANY_ID, TRAINING HOURS, AND NUMBER OF PARTICIPANTS
            
            # Validate and parse date
            try:
                date = pd.to_datetime(row["date"]).date()
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format.")
                continue

            rows.append({
                "company_id": str(row["company_id"]).strip(),
                "date": date,
                "training_title": str(row["training_title"]),
                "training_hours": int(row["training_hours"]),
                "number_of_participants": int(row["number_of_participants"])
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = insert_training_bulk(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/bulk_occupational_safety_health")
def bulk_upload_occupational_safety_health(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an Excel file.")
    
    TRUE_VALUES = {"TRUE", "YES", 1, True}
    FALSE_VALUES = {"FALSE", "NO", 0, False}

    def parse_lost_time(value):
        if isinstance(value, str):
            value = value.upper()
            
        if value in TRUE_VALUES:
            return True
        elif value in FALSE_VALUES:
            return False
        else:
            raise ValueError(f"Invalid lost_time value: {value}")
    
    try:
        logging.info("Add bulk occupational safety health")
        contents = file.file.read()
        df = pd.read_excel(BytesIO(contents))

        required_columns = {'company_id', 'workforce_type', 'lost_time', 'date', 'incident_type', 'incident_title', 'incident_count'}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {required_columns - set(df.columns)}"
            )
            
        rows = []
        validation_errors = []

        for i, row in df.iterrows():
            row_number = i + 2  # Excel row number

            # Validate and parse date
            try:
                date = pd.to_datetime(row["date"]).date()
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format.")
                continue

            # Validate and parse lost_time
            try:
                
                lost_time = parse_lost_time(row["lost_time"])
            except ValueError as e:
                validation_errors.append(f"Row {row_number}: {e}")
                continue

            # Validate incident_count
            try:
                incident_count = int(row["incident_count"])
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid incident_count.")
                continue

            rows.append({
                "company_id": str(row["company_id"]).strip(),
                "workforce_type": str(row["workforce_type"]),
                "lost_time": lost_time,
                "date": date,
                "incident_type": str(row["incident_type"]),
                "incident_title": str(row["incident_title"]),
                "incident_count": incident_count
            })

        if validation_errors:
            error_message = "Data validation failed:\n" + "\n".join(validation_errors)
            raise HTTPException(status_code=422, detail=error_message)

        if not rows:
            raise HTTPException(status_code=400, detail="No valid data rows found to insert")

        count = insert_occupational_safety_health_bulk(db, rows)
        return {"message": f"{count} records successfully inserted."}

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ====================== EDIT RECORD ====================== 
# --- EMPLOYABILITY ---
@router.post("/edit_employability")
def edit_employability(
    data: dict, db: Session = Depends(get_db)
):
    try:
        required_fields = ['employee_id', 'gender', 'birthdate', 'position_id', 'p_np', 'company_id', 'employment_status', 'start_date', 'end_date']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["p_np"], str) or not data["p_np"].strip():
            raise HTTPException(status_code=422, detail="Invalid p_np")
        
        if not isinstance(data["gender"], str) or not data["gender"].strip():
            raise HTTPException(status_code=422, detail="Invalid gender")
        
        if not isinstance(data["position_id"], str) or not data["position_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid position_id")

        if data["employment_status"] not in {"Permanent", "Temporary"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['employment_status']}'")
        
        employee_id = data["employee_id"]
        record_demo = {
            "employee_id": data["employee_id"],
            "gender": data["gender"],
            "birthdate": data["birthdate"],
            "position_id": data["position_id"],
            "p_np": data["p_np"],
            "company_id": data["company_id"],
            "employment_status": data["employment_status"]
        }
        

        print("📌 Received end_date:", data["end_date"])
        record_tenure = {
            "employee_id": data["employee_id"],
            "start_date": data["start_date"],
            "end_date": data["end_date"]
        }
        
        update_employability(db, employee_id, record_demo, record_tenure)
        return {"message": "employability record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# --- Safety Workdata ---
@router.post("/edit_safety_workdata")
def edit_safety_workdata(
    data: dict, db: Session = Depends(get_db)
):
    try:
        required_fields = ['company_id', 'contractor', 'date', 'manpower', 'manhours']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if not isinstance(data["contractor"], str) or not data["contractor"].strip():
            raise HTTPException(status_code=422, detail="Invalid contractor")
        
        safety_workdata_id = data["safety_workdata_id"]
        record = {
            "company_id": data["company_id"],
            "contractor": data["contractor"],
            "date": data["date"],
            "manpower": int(data["manpower"]),
            "manhours": int(data["manhours"]),
        }
        
        update_safety_workdata(db, safety_workdata_id, record)
        return {"message": "safety workdata record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Parental Leave ---
@router.post("/edit_parental_leave")
def edit_parental_leave(
    data: dict, db: Session = Depends(get_db)
):
    try:
        required_fields = ['employee_id', 'type_of_leave', 'date', 'days']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")
        
        parental_leave_id = data["parental_leave_id"]
        record = {
            "employee_id": data["employee_id"],
            "type_of_leave": data["type_of_leave"],
            "date": data["date"],
            "days": int(data["days"]),
        }
        
        update_parental_leave(db, parental_leave_id, record)
        return {"message": "parental leave record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Training ---
@router.post("/edit_training")
def edit_training(
    data: dict, db: Session = Depends(get_db)
):
    try:
        required_fields = ['company_id', 'date', 'training_title', 'training_hours', 'number_of_participants']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        training_id = data["training_id"]
        record = {
            "company_id": data["company_id"],
            "date": data["date"],
            "training_title": data["training_title"],
            "training_hours": int(data["training_hours"]),
            "number_of_participants": int(data["number_of_participants"]),
        }
        
        update_training(db, training_id, record)
        return {"message": "training record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Occupational Safety Health ---
@router.post("/edit_osh")
def edit_osh(
    data: dict, db: Session = Depends(get_db)
):
    try:
        required_fields = ['company_id', 'workforce_type', 'lost_time', 'date', 'incident_type', 'incident_title', 'incident_count']
        missing = [field for field in required_fields if field not in data]
        if missing:
            raise HTTPException(status_code=400, detail=f"Missing required fields: {missing}")

        if not isinstance(data["company_id"], str) or not data["company_id"].strip():
            raise HTTPException(status_code=422, detail="Invalid company_id")

        if data["lost_time"] not in {True, False}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['lost_time']}'")

        osh_id = data["osh_id"]
        record = {
            "company_id": data["company_id"],
            "workforce_type": data["workforce_type"],
            "lost_time": data["lost_time"],
            "date": data["date"],
            "incident_type": data["incident_type"],
            "incident_title": data["incident_title"],
            "incident_count": int(data["incident_count"]),
        }
        
        update_occupational_safety_health(db, osh_id, record)
        return {"message": "training record successfully updated."}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ====================== EXPORT DATA ======================
@router.post("/export_excel")
async def export_excel(request: Request):
    data = await request.json()
    
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    # Write to Excel in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Filtered Data")

    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=exported_data.xlsx"
        }
    )
    
# ====================== TEMPLATE GENERATION DATA ======================
@router.get("/template-employability")
async def download_economic_generated_template():
    try:
        headers = ['employee_id', 'gender', 'birthdate', 'position_id', 'p_np', 'company_id', 'employment_status', 'start_date', 'end_date']
        filename = 'hr_employability_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")
    
@router.get("/template-safety-workdata")
async def download_economic_generated_template():
    try:
        headers = ['company_id', 'contractor', 'date', 'manpower', 'manhours']
        filename = 'hr_safety_workdata_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")
    
@router.get("/template-parental-leave")
async def download_economic_generated_template():
    try:
        headers = ['employee_id', 'type_of_leave', 'date', 'days']
        filename = 'hr_parental_leave_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")
    
@router.get("/template-training")
async def download_economic_generated_template():
    try:
        headers = ['company_id', 'date', 'training_title', 'training_hours', 'number_of_participants']
        filename = 'hr_training_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")
    
@router.get("/template-osh")
async def download_economic_generated_template():
    try:
        headers = ['company_id', 'workforce_type', 'lost_time', 'date', 'incident_type', 'incident_title', 'incident_count']
        filename = 'hr_occupational_safety_health_template.xlsx'
        output = create_excel_template(headers, filename)
        
        return StreamingResponse(
            io.BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating template: {str(e)}")
