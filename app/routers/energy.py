from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut
from app.dependencies import get_db
from app.crud.base import get_one, get_all, get_many, get_many_filtered
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
            SELECT er.*, csl.status_id
            FROM silver.csv_energy_records er
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







