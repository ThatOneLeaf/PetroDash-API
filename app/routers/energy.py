from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List
from sqlalchemy.orm import Session
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut
from app.dependencies import get_db
from app.crud.base import get_one, get_all, get_many, get_many_filtered

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


@router.get("/{energy_id}", response_model=EnergyRecordOut)
def get_energy_by_id(energy_id: str, db: Session = Depends(get_db)):
    record = get_one(db, EnergyRecords, "energy_id", energy_id)
    if not record:
        raise HTTPException(status_code=404, detail="Energy record not found")
    return record