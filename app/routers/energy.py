from fastapi import APIRouter, Depends, Query, HTTPException, Form, UploadFile, File
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text, update, select
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut, AddEnergyRecord
from app.public.models import RecordStatus
from app.dependencies import get_db
from app.crud.base import get_one, get_all, get_many, get_many_filtered, get_one_filtered
from datetime import datetime
import pandas as pd
import io
import logging
import traceback


router = APIRouter()

def process_status_change(
    db: Session,
    energy_id: str,
    checker_id: str,
    remarks: str,
    action: str
):
    # Step 1: Validate action
    action = action.lower()
    if action not in {"approve", "revise"}:
        raise HTTPException(status_code=400, detail="Invalid action. Must be 'approve' or 'revise'.")

    # Step 2: Verify record exists
    record = db.query(EnergyRecords).filter(EnergyRecords.energy_id == energy_id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Energy record not found.")

    # Step 3: Get latest status
    latest_status = (
        db.query(RecordStatus)
        .filter(RecordStatus.record_id == energy_id)
        .order_by(RecordStatus.status_timestamp.desc())
        .first()
    )
    if not latest_status:
        raise HTTPException(status_code=404, detail="Checker status not found.")
    
    current_status = latest_status.status_id

    # Step 4: Define transitions
    approve_transitions = {
        None: "URS",
        "FRS": "URS",
        "URS": "URH",
        "FRH": "URH",
        "URH": "APP",
    }

    reject_transitions = {
        "URS": "FRS",
        "URH": "FRH",
    }

    # Step 5: Determine next status
    if action == "approve":
        next_status = approve_transitions.get(current_status)
    else:  # revise
        next_status = reject_transitions.get(current_status)

    if not next_status:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot perform '{action}' from status '{current_status}'."
        )

    latest_status.status_id = next_status
    latest_status.status_timestamp = datetime.now()
    latest_status.remarks = remarks

    db.commit()
    db.refresh(latest_status)

    return {
        "message": f"Status updated to '{next_status}' via '{action}'.",
        "data": {
            "cs_id": latest_status.cs_id,
            "record_id": latest_status.record_id,
            "status_id": latest_status.status_id,
            "timestamp": latest_status.status_timestamp,
            "remarks": latest_status.remarks
        }
    }


@router.get("/energy_record", response_model=EnergyRecordOut)
def get_energy_record(
    energy_id: str = Query(..., description="ID of the energy record"),
    company: Optional[str] = Query(None, description="Filter by company"),
    powerplant: Optional[str] = Query(None, description="Filter by powerplant"),
    db: Session = Depends(get_db),
):
    filters = {"energy_id": energy_id}
    if company:
        filters["company"] = company
    if powerplant:
        filters["powerplant"] = powerplant
    
    record = get_one_filtered(db, EnergyRecords, filters)
    if not record:
        raise HTTPException(status_code=404, detail="Energy record not found")
    return record




# ====================== energy records by status ====================== #
@router.get("/energy_records_by_status", response_model=List[dict])
def get_energy_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching energy records. Filter status_id: {status_id}")

        query = text("""
                SELECT     
                    er.energy_id,
                    er.power_plant_id,
                    er.date_generated::date AS date_generated,
                    er.energy_generated_kwh,
                    er.co2_avoidance_kg, 
                    pp.*, rs.status_id, st.status_name, rs.remarks
                FROM silver.csv_energy_records er
                JOIN gold.dim_powerplant_profile pp 
                    ON pp.power_plant_id = er.power_plant_id
                JOIN record_status rs on rs.record_id = er.energy_id
                JOIN public.status st on st.status_id = rs.status_id
                ORDER BY er.create_at DESC, er.date_generated DESC, er.updated_at DESC;
        """)

        result = db.execute(query, {"status_id": status_id})
        data = [dict(row._mapping) for row in result]

        logging.info(f"Returned {len(data)} records")
        return data

    except Exception as e:
        logging.error(f"Error retrieving energy records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

# ====================== fact_table (func from pg) ====================== #
@router.get("/fact_energy", response_model=List[dict])
def get_fact_energy(
    power_plant_ids: Optional[List[str]] = Query(None, alias="p_power_plant_id"),
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    generation_sources: Optional[List[str]] = Query(None, alias="p_generation_source"),
    provinces: Optional[List[str]] = Query(None, alias="p_province"),
    months: Optional[List[int]] = Query(None, alias="p_month"),
    quarters: Optional[List[int]] = Query(None, alias="p_quarter"),
    years: Optional[List[int]] = Query(None, alias="p_year"),
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            SELECT * FROM gold.func_fact_energy(
                p_power_plant_id := :power_plant_ids,
                p_company_id := :company_ids,
                p_generation_source := :generation_sources,
                p_province := :provinces,
                p_month := :months,
                p_quarter := :quarters,
                p_year := :years
            )
        """)

        # Convert None to NULL for Postgres array params
        params = {
            "power_plant_ids": power_plant_ids if power_plant_ids else None,
            "company_ids": company_ids if company_ids else None,
            "generation_sources": generation_sources if generation_sources else None,
            "provinces": provinces if provinces else None,
            "months": months if months else None,
            "quarters": quarters if quarters else None,
            "years": years if years else None,
        }

        result = db.execute(sql, params)
        data = [dict(row._mapping) for row in result]

        return data

    except Exception as e:
        logging.error(f"Error calling func_fact_energy: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")




# ====================== energy records by id ====================== #
@router.get("/{energy_id}", response_model=EnergyRecordOut)
def get_energy_by_id(energy_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnergyRecords, "energy_id", energy_id)
    if not record:
        raise HTTPException(status_code=404, detail="Energy record not found")
    return record

# ====================== generate energy id ====================== #
def generate_energy_id(db: Session) -> str:
    today = datetime.now().strftime("%Y%m%d")
    like_pattern = f"EN-{today}-%"

    # Count how many records already exist for today
    count_today = (
        db.query(EnergyRecords)
        .filter(EnergyRecords.energy_id.like(like_pattern))
        .count()
    )

    seq = f"{count_today + 1:03d}"  # Format as 3-digit sequence
    return f"EN-{today}-{seq}"

# ====================== generate cs id ====================== #
def generate_cs_id(db: Session) -> str:
    today = datetime.now().strftime("%Y%m%d")
    like_pattern = f"CS{today}%"

    # Count how many records already exist for today
    count_today = (
        db.query(RecordStatus)
        .filter(RecordStatus.cs_id.like(like_pattern))
        .count()
    )

    seq = f"{count_today + 1:03d}"  # Format as 3-digit sequence
    return f"CS{today}{seq}"

# ====================== single add energy record ====================== #

@router.post("/add")
def add_energy_record(
    powerPlant: str = Form(...),
    date: str = Form(...),
    energyGenerated: float = Form(...),
    checker: str = Form(...),
    metric: str = Form(...),
    remarks: str = Form(...),
    db: Session = Depends(get_db),
):
    print(f"--- Incoming Request ---")
    print(f"Power Plant: {powerPlant}")
    print(f"Date: {date}")
    print(f"Energy Generated: {energyGenerated}")
    print(f"Metric: {metric}")
    print(f"Remarks: {remarks}")
    print(f"Checker: {checker}")
    
    try:
        # Parse date
        try:
            parsed_date = datetime.strptime(date, "%Y-%m-%d")
            print(f"Parsed Date: {parsed_date}")
        except ValueError as ve:
            print(f"Date parsing error: {ve}")
            raise HTTPException(status_code=400, detail=f"Date parsing error: {str(ve)}")

        # Check for existing record
        try:
            existing = db.query(EnergyRecords).filter(
                EnergyRecords.power_plant_id == powerPlant,
                EnergyRecords.datetime == parsed_date
            ).first()
            print(f"Existing Record: {existing}")
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "duplicate_error",
                        "message": f"Duplicate record: energy data for {powerPlant} on {parsed_date.strftime('%Y-%m-%d')} already exists."
                    }
                )
        except Exception as db_err:
            print(f"Database query failed: {db_err}")
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(db_err)}")

        # Generate ID
        try:
            new_id = generate_energy_id(db)
            print(f"Generated Energy ID: {new_id}")
        except Exception as id_err:
            print(f"ID generation failed: {id_err}")
            raise HTTPException(status_code=500, detail=f"ID generation failed: {str(id_err)}")

        # Create record and log
        try:
            new_record = EnergyRecords(
                energy_id=new_id,
                power_plant_id=powerPlant,
                datetime=parsed_date,
                energy_generated=energyGenerated,
                unit_of_measurement=metric,
            )
            print(f"New EnergyRecord object: {new_record}")

            new_log = RecordStatus(
                cs_id="CS-" + new_id,
                record_id=new_id,
                status_id="URS",
                status_timestamp=datetime.now(),
                remarks=remarks
            )
            print(f"New RecordStatus object: {new_log}")

            db.add(new_record)
            db.add(new_log)
            db.commit()
            db.refresh(new_record)
            print("Record and log inserted successfully.")
        except Exception as record_err:
            print(f"Failed to insert record or log: {record_err}")
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to insert record or log: {str(record_err)}")

        # Call stored procedure
        try:
            print("Calling stored procedure: silver.load_csv_silver()")
            db.execute(text("CALL silver.load_csv_silver();"))
            db.commit()
            print("Stored procedure executed successfully.")
        except Exception as proc_err:
            print(f"Stored procedure error: {proc_err}")
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Stored procedure error: {str(proc_err)}")

        print("Energy record successfully added.")
        return {
            "message": "Energy record successfully added.",
            "data": {
                "energy_id": new_record.energy_id,
                "power_plant_id": new_record.power_plant_id,
                "datetime": new_record.datetime,
                "energy_generated": new_record.energy_generated,
                "unit_of_measurement": new_record.unit_of_measurement
            }
        }

    except HTTPException as http_err:
        print(f"HTTPException: {http_err.detail}")
        raise http_err

    except Exception as e:
        db.rollback()
        tb = traceback.format_exc()
        print(f"Unexpected error: {e}")
        print(f"Traceback:\n{tb}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}\nTraceback:\n{tb}"
        )


# ====================== bulk add energy record ====================== #
@router.post("/bulk_add")
def bulk_add_energy_record(
    powerPlant: str = Form(...),
    checker: str = Form(...),
    metric: str = Form(...),
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    if file.content_type not in [
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.ms-excel"
    ]:
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload an Excel file.")

    try:
        contents = file.file.read()
        df = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read Excel file: {e}")

    if 'date' not in df.columns or 'energy_generated' not in df.columns:
        raise HTTPException(status_code=400, detail="Excel must contain 'date' and 'energy_generated' columns.")

    # Generate base energy_id prefix for bronze.energy records
    first_id = generate_energy_id(db)
    parts = first_id.split("-")
    prefix = "-".join(parts[:2])
    counter = int(parts[-1])
    
    # Generate base cs_id prefix for status
    first_cs_id = generate_cs_id(db)
    cs_prefix = first_cs_id[:-3]  # except the last 3 digits
    cs_counter = int(first_cs_id[-3:])  # last 3 digits

    inserted = 0
    updated = 0

    for i, (_, row) in enumerate(df.iterrows()):
        try:
            date_val = pd.to_datetime(row["date"], format="%Y-%m-%d").date()
            energy_val = float(row["energy_generated"])
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid format at row {i+2}.")

        # Check if a record exists
        existing_query = select(EnergyRecords.energy_generated).where(
            EnergyRecords.power_plant_id == powerPlant,
            EnergyRecords.datetime == date_val
        )
        result = db.execute(existing_query).first()

        if result:
            # aggregate
            update_stmt = (
                update(EnergyRecords)
                .where(
                    EnergyRecords.power_plant_id == powerPlant,
                    EnergyRecords.datetime == date_val
                )
                .values(energy_generated=EnergyRecords.energy_generated + energy_val)
            )
            db.execute(update_stmt)
            updated += 1
        else:
            # new record
            new_id = f"{prefix}-{str(counter).zfill(3)}"
            counter += 1

            new_record = EnergyRecords(
                energy_id=new_id,
                power_plant_id=powerPlant,
                datetime=date_val,
                energy_generated=energy_val,
                unit_of_measurement=metric,
            )
            db.add(new_record)

            # Add corresponding status log
            new_log_id = f"{cs_prefix}{str(cs_counter).zfill(3)}"
            cs_counter += 1
            
            new_log = RecordStatus(
                cs_id=new_log_id,
                checker_id=checker,
                record_id=new_id,
                status_id="URS",
                status_timestamp=datetime.now(),
                remarks="Newly Added"
            )
            db.add(new_log)

            inserted += 1

    try:
        db.commit()
        db.execute(text("CALL silver.load_csv_silver();"))
        db.commit()
        return {
            "message": "Processed successfully.",
            "inserted": inserted,
            "updated": updated
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

# ====================== update status ====================== #
@router.post("/update_status")
def change_status(
    energy_id: str = Form(...),
    checker_id: str = Form(...),
    remarks: str = Form(...),
    action: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        return process_status_change(
            db=db,
            energy_id=energy_id,
            checker_id=checker_id,
            remarks=remarks,
            action=action
        )
    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# ====================== edit energy record ====================== #
@router.post("/edit")
def edit_energy_record(
    energy_id: str = Form(...),
    powerPlant: str = Form(...),
    date: str = Form(...),
    energyGenerated: float = Form(...),
    checker: str = Form(...),
    metric: str = Form(...),
    remarks:str=Form(...),
    db: Session = Depends(get_db),
):
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    
    
    # Step 3: Get latest status
    latest_status = (
        db.query(RecordStatus)
        .filter(RecordStatus.record_id == energy_id)
        .order_by(RecordStatus.status_timestamp.desc())
        .first()
    )
    current_status = latest_status.status_id if latest_status else None
    new_status = "URH" if current_status in ['FRH', 'URH'] else "URS"


    # update
    update_stmt = (
        update(EnergyRecords)
        .where(EnergyRecords.energy_id == energy_id)
        .values(
            energy_generated = energyGenerated,
            unit_of_measurement = metric,
            updated_at = parsed_date,
            power_plant_id = powerPlant
        )
    )

    try:
        db.execute(update_stmt)

        # Step 4: Update the existing RecordStatus
        latest_status.status_id = new_status
        latest_status.status_timestamp = datetime.now()
        latest_status.remarks = remarks

        db.commit()
        return {"message": "Energy record updated successfully."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


