from sqlalchemy.orm import Session
from .models import EnergyRecords, EnviWaterAbstraction, EnviWaterDischarge, EnviWaterConsumption, EnviElectricConsumption, EnviDieselConsumption, EnviNonHazardWaste, EnviHazardWasteGenerated, EnviHazardWasteDisposed, HRDemographics, HRTenure
from app.crud.base import get_one, get_many, get_many_filtered, get_all
from app.utils.formatting_id import generate_pkey_id, generate_bulk_pkey_ids

# =================== POWER PLANT ENERGY DATA =================
def get_energy_record_by_id(db: Session, energy_id: str):
    return get_one(db, EnergyRecords, "energy_id", energy_id)

def get_all_energy_records(db: Session):
    return get_all(db, EnergyRecords)

def get_filtered_energy_records(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnergyRecords, filters=filters, skip=skip, limit=limit)

# ====================================== ENVIRONMENTAL DATA ====================================
# ====================================== RETRIEVE DATA ====================================
# --- Water Abstraction ---
def get_water_abstraction_by_id(db: Session, wa_id: str):
    return get_one(db, EnviWaterAbstraction, "wa_id", wa_id)

def get_all_water_abstraction(db: Session):
    return get_all(db, EnviWaterAbstraction)

def get_filtered_water_abstraction(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviWaterAbstraction, filters=filters, skip=skip, limit=limit)

# --- Water Discharge ---
def get_water_discharge_by_id(db: Session, wd_id: str):
    return get_one(db, EnviWaterDischarge, "wd_id", wd_id)

def get_all_water_discharge(db: Session):
    return get_all(db, EnviWaterDischarge)

def get_filtered_water_discharge(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviWaterDischarge, filters=filters, skip=skip, limit=limit)

# --- Water Consumption ---
def get_water_consumption_by_id(db: Session, wc_id: str):
    return get_one(db, EnviWaterConsumption, "wc_id", wc_id)

def get_all_water_consumption(db: Session):
    return get_all(db, EnviWaterConsumption)

def get_filtered_water_consumption(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviWaterConsumption, filters=filters, skip=skip, limit=limit)

# --- Electric Consumption ---
def get_electric_consumption_by_id(db: Session, ec_id: str):
    return get_one(db, EnviElectricConsumption, "ec_id", ec_id)

# --- Diesel Consumption ---
def get_diesel_consumption_by_id(db: Session, dc_id: str):
    return get_one(db, EnviDieselConsumption, "dc_id", dc_id)

# --- Non-Hazardous Waste ---
def get_non_hazard_waste_by_id(db: Session, nhw_id: str):
    return get_one(db, EnviNonHazardWaste, "nhw_id", nhw_id)

# --- Hazardous Waste Generated ---
def get_hazard_waste_generated_by_id(db: Session, hwg_id: str):
    return get_one(db, EnviHazardWasteGenerated, "hwg_id", hwg_id)

# --- Hazardous Waste Disposed ---
def get_hazard_waste_disposed_by_id(db: Session, hwd_id: str):
    return get_one(db, EnviHazardWasteDisposed, "hwd_id", hwd_id)

# ====================================== INSERT DATA ====================================
# ====================================== BULK INSERT ======================================
def bulk_create_water_abstractions(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    
    # Group rows by company_id and year to handle different patterns
    from collections import defaultdict
    grouped_rows = defaultdict(list)
    
    for i, row in enumerate(rows):
        key = (row["company_id"], int(row["year"]))
        grouped_rows[key].append((i, row))
    
    # Generate IDs for each group
    id_mapping = {}
    for (company_id, year), row_list in grouped_rows.items():
        ids = generate_bulk_pkey_ids(
            db=db,
            indicator="WA",
            company_id=company_id,
            year=year,
            model_class=EnviWaterAbstraction,
            id_field="wa_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviWaterAbstraction(
            wa_id=id_mapping[i],
            company_id=row["company_id"],
            year=row["year"],
            month=row["month"],
            quarter=row["quarter"],
            volume=row["volume"],
            unit_of_measurement=row["unit_of_measurement"],
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()
    return len(records)

# =================== HR DATA =================
# --- Demographics ---
def get_employee_demographics_by_id(db: Session, employee_id: str):
    return get_one(db, HRDemographics, "employee_id", employee_id)

# --- Tenure ---
def get_employee_tenure_by_id(db: Session, tr_id: str):
    return get_one(db, HRTenure, "tr_id", tr_id)