from fastapi import APIRouter, Depends, Query, HTTPException, Form, UploadFile, File
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text, update, select
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut, AddEnergyRecord
from app.dependencies import get_db
from app.crud.base import get_one, get_all, get_many, get_many_filtered
from datetime import datetime
import pandas as pd
import io
import logging
import traceback


router = APIRouter()

@router.get("/energy_records", response_model=List[EnergyRecordOut])
def get_energy_records(
    site_name: Optional[str] = Query(None),
    created_by: Optional[str] = Query(None),
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    filters = {}
    if site_name:
        filters["site_name"] = site_name
    if created_by:
        filters["created_by"] = created_by

    if filters:
        return get_many_filtered(db, EnergyRecords, filters=filters, skip=skip, limit=limit)
    return get_all(db, EnergyRecords)


# ====================== energy records by status ====================== #
@router.get("/energy_records_by_status", response_model=List[dict])
def get_energy_records_by_status(
    status_id: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        logging.info(f"Fetching energy records. Filter status_id: {status_id}")

        query = text("""
            SELECT er.*,pp.*, csl.status_id
            FROM silver.csv_energy_records er
			JOIN gold.dim_powerplant_profile pp 
                ON pp.power_plant_id=er.power_plant_id
            JOIN public.checker_status_log csl
                ON er.energy_id = csl.record_id
            WHERE (:status_id IS NULL OR csl.status_id = :status_id)
            ORDER BY er.create_at desc, er.date_generated desc, er.updated_at desc
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

# ====================== single add energy record ====================== #
@router.post("/add_energy_record")
def add_energy_record(
    powerPlant: str = Form(...),
    date: str = Form(...),
    energyGenerated: float = Form(...),
    metric: str = Form(...),
    db: Session = Depends(get_db),
):
    # Parse date string to datetime
    try:
        parsed_date = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    # Check if record already exists for this powerPlant and date
    existing = db.query(EnergyRecords).filter(
        EnergyRecords.power_plant_id == powerPlant,
        EnergyRecords.datetime == parsed_date
    ).first()

    if existing:
        raise HTTPException(status_code=400, detail="A record for this date already exists.")

    new_id = generate_energy_id(db)
    new_record = EnergyRecords(
        energy_id=new_id,
        power_plant_id=powerPlant,
        datetime=parsed_date,
        energy_generated=energyGenerated,
        unit_of_measurement=metric,
    )

    db.add(new_record)
    db.commit()
    db.refresh(new_record)

    # update silver
    db.execute(text("CALL silver.load_csv_silver();"))
    db.commit()

    return new_record


# ====================== bulk add energy record ====================== #
@router.post("/bulk_add_energy_record")
def bulk_add_energy_record(
    powerPlant: str = Form(...),
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

    # Generate base energy_id prefix
    first_id = generate_energy_id(db)
    parts = first_id.split("-")
    prefix = "-".join(parts[:2])
    counter = int(parts[-1])

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
            # Perform SQL-level update to avoid modifying model attribute directly
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
