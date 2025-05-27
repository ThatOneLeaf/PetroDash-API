from sqlalchemy.orm import Session
from models import EnergyRecords
from app.crud.base import get_one, get_many, get_many_filtered, get_all


def get_energy_record_by_id(db: Session, energy_id: str):
    return get_one(db, EnergyRecords, "energy_id", energy_id)


def get_all_energy_records(db: Session):
    return get_all(db, EnergyRecords)


def get_filtered_energy_records(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnergyRecords, filters=filters, skip=skip, limit=limit)
