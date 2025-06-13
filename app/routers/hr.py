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

# ====================== EMPLOYABILITY DASHBOARD ======================
@router.get("/total_active_employees", response_model=List[dict])
def get_total_active_employees(
    year: Optional[int] = Query(None),
    company_id: Optional[str] = Query(None),
    position_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get the Total active employees count
    """
    try:
        logging.info("Executing Active count of employees per Gender query")
        
        if year == None:
            year = datetime.now().year
        result = db.execute(text("""
            SELECT
                COUNT(employee_id) AS total_active_employees
            FROM gold.func_employee_summary_yearly(NULL, NULL, :position_id, :company_id, :year) AS f;
        """), {
                'year': [year],
                'company_id': [company_id] if company_id else None,
                'position_id': [position_id] if position_id else None,
        })

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

@router.get("/employee_count_per_company", response_model=List[dict])
def get_employee_count_per_company(
    year: Optional[int] = Query(None),
    company_id: Optional[str] = Query(None),
    position_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get the Total active employees count
    """
    try:
        logging.info("Executing Active count of employees per Gender query")
        
        if year == None:
            year = datetime.now().year
            
        result = db.execute(text("""
            SELECT
            year,
            company_id,
            company_name,
            COUNT(DISTINCT employee_id) AS employee_count
            FROM gold.func_employee_summary_yearly(NULL, NULL, :position_id, :company_id, :year)
            GROUP BY year, company_id, company_name
            ORDER BY year DESC, employee_count DESC;
        """), {
                'year': [year],
                'company_id': [company_id] if company_id else None,
                'position_id': [position_id] if position_id else None,
        })

        data = [
            {
                "company_id": row.company_id,
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

@router.get("/gender_distribution_per_position", response_model=List[dict])
def get_gender_distribution_per_position(
    year: Optional[int] = Query(None),
    company_id: Optional[str] = Query(None),
    position_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    """
    Get the Total active employees count
    """
    try:
        logging.info("Executing Active count of employees per Gender query")
        
        if year == None:
            year = datetime.now().year
            
        result = db.execute(text("""
            SELECT
                position_id,
                position_name,
                gender,
                COUNT(*) AS employee_count
            FROM gold.func_employee_summary_yearly(NULL, NULL, :position_id, :company_id, :year)
            WHERE CURRENT_DATE BETWEEN start_date AND COALESCE(end_date, CURRENT_DATE)
            GROUP BY position_id, position_name, gender
            ORDER BY position_id, gender;
        """), {
                'year': [year],
                'company_id': [company_id] if company_id else None,
                'position_id': [position_id] if position_id else None,
        })

        data = [
            {
                "position_id": row.position_id,
                "gender": row.gender,
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
            JOIN public.record_status csl
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
            
            # Validate and parse date
            try:
                birthdate = pd.to_datetime(row["birthdate"]).date()
                start_date = pd.to_datetime(row["start_date"]).date()
                if pd.isnull(row["end_date"]) or row["end_date"] in ["", "NaT"]:
                    end_date = None
                else:
                    end_date = pd.to_datetime(row["end_date"]).date()
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format.")
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

            # INSERT VALIDATION FOR COMPANY_ID, WORKFORCE_TYPE, LOST_TIME, INCIDENT_TYPE, AND INCIDENT_COUNT
            
            # Validate and parse date
            try:
                date = pd.to_datetime(row["date"]).date()
            except (ValueError, TypeError):
                validation_errors.append(f"Row {row_number}: Invalid date format.")
                continue

            rows.append({
                "company_id": str(row["company_id"]).strip(),
                "workforce_type": str(row["workforce_type"]),
                "lost_time": str(row["lost_time"]).strip(),
                "date": date,
                "incidet_type": str(row["incident_type"]),
                "incident_title": str(row["incident_title"]),
                "incident_count": int(row["incident_count"])
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

        if data["lost_time"] not in {"TRUE", "FALSE"}:
            raise HTTPException(status_code=422, detail=f"Invalid quarter '{data['lost_time']}'")

        osh_id = data["osh_id"]
        record = {
            "company_id": data["company_id"],
            "workforce_type": data["workforce_type"],
            "lost_time": data["lost_time"] == "TRUE",
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
