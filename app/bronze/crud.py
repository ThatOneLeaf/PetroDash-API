from sqlalchemy.orm import Session
from .models import EnergyRecords, CSRActivity, CSRProject, CSRProgram, EnviCompanyProperty, EnviWaterAbstraction, EnviWaterDischarge, EnviWaterConsumption, EnviElectricConsumption, EnviDieselConsumption, EnviNonHazardWaste, EnviHazardWasteGenerated, EnviHazardWasteDisposed
from .models import HRDemographics, HRTenure, HRSafetyWorkdata, HRTraining, HRParentalLeave, HROsh, HRParentalLeaveSilver, HRSafetyWorkdataSilver, HRTrainingSilver
from app.crud.base import get_one, get_many, get_many_filtered, get_all
from app.utils.formatting_id import generate_single_pkey_id, generate_bulk_pkey_ids
from app.utils.gen_help_id import generate_pkey_id, generate_bulk_id
from sqlalchemy import text, desc
from sqlalchemy.sql import text
from sqlalchemy import func
from datetime import datetime, timedelta
from app.public.models import RecordStatus
from pandas import isna
import logging
import sys

logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)

# ==================== ID Generation ====================
def id_generation(db: Session, prefix: str, table_id_column):
    """
    Generates unique ID with the given prefix and today's date (YYYYMMDD),
    based on the latest existing ID in the specified table column.

    Arguments:
        prefix: The prefix for the ID (e.g., "CS", "SWD", "TR").
        table_id_column: Table Name and Column (e.g. CheckerStatus.record_id)

    Sample Call:
        cs_id = id_generation(db, "CS", CheckerStatus.record_id)

    Returns:
        str: A new unique ID (e.g., "CS202506040001").
    """
    
    # Generate CSID
    today_str = datetime.today().strftime("%Y%m%d")
    like_pattern = f"{prefix}{today_str}%"
    
    latest_id = (
        db.query(table_id_column)
        .filter(table_id_column.like(like_pattern))
        .order_by(desc(table_id_column))
        .first()
    )

    if latest_id:
        latest_sequence = int(latest_id[0][-4:])  # last 4 characters
    else:
        latest_sequence = 0

    new_sequence = str(latest_sequence + 1).zfill(4)
    generated_id = f"{prefix}{today_str}{new_sequence}"
    return generated_id

# =================== POWER PLANT ENERGY DATA =================
def get_energy_record_by_id(db: Session, energy_id: str):
    return get_one(db, EnergyRecords, "energy_id", energy_id)

def get_all_energy_records(db: Session):
    return get_all(db, EnergyRecords)

def get_filtered_energy_records(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnergyRecords, filters=filters, skip=skip, limit=limit)

# ============================ CSR/HELP DATA============================
# ========================== RETRIEVE DATA ============================
# --- CSR Activity ---
def get_csr_activity_by_id(db: Session, csr_id: str):
    return get_one(db, CSRActivity, "csr_id", csr_id)

def get_all_csr_activities(db: Session):
    return get_all(db, CSRActivity)

def get_filtered_csr_activities(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, CSRActivity, filters=filters, skip=skip, limit=limit)

# --- CSR Project ---
def get_csr_project_by_id(db: Session, project_id: str):
    return get_one(db, CSRProject, "project_id", project_id)

def get_all_csr_projects(db: Session):
    return get_all(db, CSRProject)
    
def get_filtered_csr_projects(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, CSRProject, filters=filters, skip=skip, limit=limit)

# --- CSR Program ---
def get_csr_program_by_id(db: Session, program_id: str):
    return get_one(db, CSRProgram, "program_id", program_id)

def get_all_csr_programs(db: Session):
    return get_all(db, CSRProgram)

def get_filtered_csr_programs(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, CSRProgram, filters=filters, skip=skip, limit=limit)

# ========================== INSERT SINGLE DATA ============================
def insert_csr_activity(db: Session, data: dict):
    csr_id = data.get("csr_id")
    if not csr_id:
        csr_id = generate_pkey_id(
            db=db,
            company_id=data["company_id"],
            year=data["project_year"],
            model_class=CSRActivity,
            id_field="csr_id"
        )

    try:
        db.execute(text("""
            INSERT INTO bronze.csr_activity (
                csr_id, 
                company_id,
                project_id,
                project_year,
                csr_report,
                project_expenses,
                project_remarks
            ) VALUES (
                :csr_id,
                :company_id,
                :project_id,
                :project_year,
                :csr_report,
                :project_expenses,
                :project_remarks
            )
            ON CONFLICT (csr_id) DO UPDATE SET
                company_id = EXCLUDED.company_id,
                project_id = EXCLUDED.project_id,
                project_year = EXCLUDED.project_year,
                csr_report = EXCLUDED.csr_report,
                project_expenses = EXCLUDED.project_expenses,
                project_remarks = EXCLUDED.project_remarks
        """), 
        {
            'csr_id': csr_id,
            'company_id': data["company_id"],
            'project_id': data["project_id"],
            'project_year': data["project_year"],
            'csr_report': data["csr_report"],
            'project_expenses': data["project_expenses"],
            'project_remarks': data["project_remarks"],
        })

        db.commit()

        db.execute(text("CALL silver.load_csr_silver()"))

        db.commit()

        print("CSR Activity record created and processed to silver layer successfully")

    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{csr_id}",
            record_id=csr_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()

    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()

    return  csr_id

def update_csr_activity(db: Session, data: dict):
    csr_id = data.get("csr_id")
    try:
        db.execute(text("""
            UPDATE bronze.csr_activity
            SET 
                company_id = :company_id,
                project_id = :project_id,
                project_year = :project_year,
                csr_report = :csr_report,
                project_expenses = :project_expenses,
                project_remarks = :project_remarks
            WHERE csr_id = :csr_id
        """), 
        {
            'csr_id': csr_id,
            'company_id': data["company_id"],
            'project_id': data["project_id"],
            'project_year': data["project_year"],
            'csr_report': data["csr_report"],
            'project_expenses': data["project_expenses"],
            'project_remarks': data["project_remarks"]
        })

        db.commit()

        db.execute(text("CALL silver.load_csr_silver()"))

        db.commit()

    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()

    return  {"message": "CSR Activity record updated successfully"}

def bulk_upload_csr_activity(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0

    records = []
    checker_log_objects = []

    from collections import defaultdict
    grouped_rows = defaultdict(list)

    for i, row in enumerate(rows):
        key = (row["company_id"], int(row["project_year"]))
        grouped_rows[key].append((i, row))

    id_mapping = {}
    for (company_id, project_year), row_list in grouped_rows.items():
        ids = generate_bulk_id(
            db=db,
            company_id=company_id,
            year=project_year,
            model_class=CSRActivity,
            id_field="csr_id",
            count=len(row_list)
        )

        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id

    # Build CSR records and CheckerStatus logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        csr_id = id_mapping[i]

        # Create CSR record
        record = CSRActivity(
            csr_id=csr_id,
            company_id=row["company_id"],
            project_id=row["project_id"],
            project_year=row["project_year"],
            csr_report=row["csr_report"],
            project_expenses=row["project_expenses"],
            project_remarks=row["project_remarks"],
        )
        records.append(record)

        # Create checker_status_log model instance
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{csr_id}",
            record_id=csr_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_log_objects.append(checker_log)

    # Insert records into EnviWaterAbstraction
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call stored procedure
    try:
        db.execute(text("CALL silver.load_csr_silver()"))
        db.commit()
        print("Stored procedure executed successfully")
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()

    # Insert checker_status_log using ORM model
    try:
        db.bulk_save_objects(checker_log_objects)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()

    return records

# ====================================== ENVIRONMENTAL DATA ====================================
# ====================================== RETRIEVE DATA ====================================

# =================== ENVIRONMENTAL DATA =================
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

def get_all_electric_consumption(db: Session):
    return get_all(db, EnviElectricConsumption)

def get_filtered_electric_consumption(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviElectricConsumption, filters=filters, skip=skip, limit=limit)

# --- Diesel Consumption ---
def get_diesel_consumption_by_id(db: Session, dc_id: str):
    return get_one(db, EnviDieselConsumption, "dc_id", dc_id)

def get_all_diesel_consumption(db: Session):
    return get_all(db, EnviDieselConsumption)

def get_filtered_diesel_consumption(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviDieselConsumption, filters=filters, skip=skip, limit=limit)

# --- Non-Hazardous Waste ---
def get_non_hazard_waste_by_id(db: Session, nhw_id: str):
    return get_one(db, EnviNonHazardWaste, "nhw_id", nhw_id)

def get_all_non_hazard_waste(db: Session):
    return get_all(db, EnviNonHazardWaste)

def get_filtered_non_hazard_waste(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviNonHazardWaste, filters=filters, skip=skip, limit=limit)

# --- Hazardous Waste Generated ---
def get_hazard_waste_generated_by_id(db: Session, hwg_id: str):
    return get_one(db, EnviHazardWasteGenerated, "hwg_id", hwg_id)

def get_all_hazard_waste_generated(db: Session):
    return get_all(db, EnviHazardWasteGenerated)

def get_filtered_hazard_waste_generated(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviHazardWasteGenerated, filters=filters, skip=skip, limit=limit)

# --- Hazardous Waste Disposed ---
def get_hazard_waste_disposed_by_id(db: Session, hwd_id: str):
    return get_one(db, EnviHazardWasteDisposed, "hwd_id", hwd_id)

def get_all_hazard_waste_disposed(db: Session):
    return get_all(db, EnviHazardWasteDisposed)

def get_filtered_hazard_waste_disposed(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnviHazardWasteDisposed, filters=filters, skip=skip, limit=limit)

# ====================================== INSERT DATA ======================================
#water abstraction
def insert_create_water_abstraction(db: Session, data: dict):
    # Generate a primary key if needed
    wa_id = data.get("wa_id") or generate_single_pkey_id(
            db=db,
            indicator="WA",
            company_id=data["company_id"],
            year=data["year"],
            model_class=EnviWaterAbstraction,
            id_field="wa_id"
        )

    # Create the water abstraction record
    record = EnviWaterAbstraction(
        wa_id=wa_id,
        company_id=data["company_id"],
        year=data["year"],
        month=data["month"],
        quarter=data["quarter"],
        volume=data["volume"],
        unit_of_measurement=data["unit_of_measurement"],
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    # Add corresponding checker status log (URS)
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{wa_id}",
            record_id=wa_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()

    # Optionally call the stored procedure
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := TRUE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record


#water discharge
def insert_create_water_discharge(db: Session, data: dict):
    wd_id = data.get("wd_id") or generate_single_pkey_id(
        db=db,
        indicator="WD",
        company_id=data["company_id"],
        year=data["year"],
        model_class=EnviWaterDischarge,
        id_field="wd_id"
    )

    record = EnviWaterDischarge(
        wd_id=wd_id,
        company_id=data["company_id"],
        year=data["year"],
        quarter=data["quarter"],
        volume=data["volume"],
        unit_of_measurement=data["unit_of_measurement"],
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{wd_id}",
            record_id=wd_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()

    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := TRUE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# water consumption
def insert_create_water_consumption(db: Session, data: dict):
    wc_id = data.get("wc_id") or generate_single_pkey_id(
        db=db,
        indicator="WC",
        company_id=data["company_id"],
        year=data["year"],
        model_class=EnviWaterConsumption,
        id_field="wc_id"
    )
    record = EnviWaterConsumption(
        wc_id=wc_id,
        company_id=data["company_id"],
        year=data["year"],
        quarter=data["quarter"],
        volume=data["volume"],
        unit_of_measurement=data["unit_of_measurement"],
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    try:
        db.add(RecordStatus(
            cs_id=f"CS-{wc_id}",
            record_id=wc_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        ))
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()

    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := TRUE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# electric consumption
def insert_create_electric_consumption(db: Session, data: dict):
    ec_id = data.get("ec_id") or generate_single_pkey_id(
        db=db,
        indicator="EC",
        company_id=data["company_id"],
        year=data["year"],
        model_class=EnviElectricConsumption,
        id_field="ec_id"
    )
    record = EnviElectricConsumption(
        ec_id=ec_id,
        company_id=data["company_id"],
        source=data["source"],
        unit_of_measurement=data["unit_of_measurement"],
        consumption=data["consumption"],
        quarter=data["quarter"],
        year=data["year"]
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    try:
        db.add(RecordStatus(
            cs_id=f"CS-{ec_id}",
            record_id=ec_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        ))
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()

    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := TRUE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# diesel consumption
def insert_create_diesel_consumption(db: Session, data: dict):
    
    if "date" not in data:
        raise ValueError("Missing 'date' field.")
    
    if isinstance(data["date"], str):
        try:
            parsed_date = datetime.strptime(data["date"], "%Y-%m-%d")
        except ValueError:
            parsed_date = datetime.strptime(data["date"], "%m/%d/%Y")
    else:
        parsed_date = data["date"]
    
    year = parsed_date.year
    
    dc_id = data.get("dc_id") or generate_single_pkey_id(
        db=db,
        indicator="DC",
        company_id=data["company_id"],
        year=year,
        model_class=EnviDieselConsumption,
        id_field="dc_id"
    )

    property_exists = db.query(EnviCompanyProperty).filter_by(cp_id=data["cp_id"]).first()
    if not property_exists:
        raise ValueError(f"cp_id '{data['cp_id']}' does not exist.")
    
    record = EnviDieselConsumption(
        dc_id=dc_id,
        company_id=data["company_id"].strip(),
        cp_id=data["cp_id"].strip(),
        unit_of_measurement=data["unit_of_measurement"].strip(),
        consumption=float(data["consumption"]),
        date=parsed_date       
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    try:
        db.add(RecordStatus(
            cs_id=f"CS-{dc_id}",
            record_id=dc_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        ))
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()

    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := TRUE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# non-hazard waste
def insert_create_non_hazard_waste(db: Session, data: dict):
    nhw_id = data.get("nhw_id") or generate_single_pkey_id(
            db=db,
            indicator="NHW",
            company_id=data["company_id"],
            year=data["year"],
            model_class=EnviNonHazardWaste,
            id_field="nhw_id"
        )
    record = EnviNonHazardWaste(
        nhw_id=nhw_id,
        company_id=data["company_id"],
        metrics=data["metrics"],
        unit_of_measurement=data["unit_of_measurement"],
        waste=data["waste"],
        month=data["month"],
        quarter=data["quarter"],
        year=data["year"]
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    
    try:
        db.add(RecordStatus(
            cs_id=f"CS-{nhw_id}",
            record_id=nhw_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        ))
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := TRUE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# hazard waste generated
def insert_create_hazard_waste_generated(db: Session, data: dict):
    hwg_id = data.get("hwg_id") or generate_single_pkey_id(
            db=db,
            indicator="HW",
            company_id=data["company_id"],
            year=data["year"],
            model_class=EnviHazardWasteGenerated,
            id_field="hwg_id"
        )
    record = EnviHazardWasteGenerated(
        hwg_id=hwg_id,
        company_id=data["company_id"],
        metrics=data["metrics"],
        unit_of_measurement=data["unit_of_measurement"],
        waste_generated=data["waste_generated"],
        quarter=data["quarter"],
        year=data["year"]
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    
    try:
        db.add(RecordStatus(
            cs_id=f"CS-{hwg_id}",
            record_id=hwg_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        ))
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := TRUE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# hazard waste disposed
def insert_create_hazard_waste_disposed(db: Session, data: dict):
    hwd_id = data.get("hwd_id") or generate_single_pkey_id(
            db=db,
            indicator="HWD",
            company_id=data["company_id"],
            year=data["year"],
            model_class=EnviHazardWasteDisposed,
            id_field="hwd_id"
        )
    record = EnviHazardWasteDisposed(
        hwd_id=hwd_id,
        company_id=data["company_id"],
        metrics=data["metrics"],
        unit_of_measurement=data["unit_of_measurement"],
        waste_disposed=data["waste_disposed"],
        year=data["year"]
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    
    try:
        db.add(RecordStatus(
            cs_id=f"CS-{hwd_id}",
            record_id=hwd_id,
            status_id="URS",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        ))
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record
# ====================================== BULK INSERT ======================================
def bulk_create_water_abstractions(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0

    records = []
    checker_log_objects = []

    from collections import defaultdict
    grouped_rows = defaultdict(list)

    for i, row in enumerate(rows):
        key = (row["company_id"], int(row["year"]))
        grouped_rows[key].append((i, row))

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

    # Build abstraction records and CheckerStatus logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        wa_id = id_mapping[i]

        # Create abstraction record
        record = EnviWaterAbstraction(
            wa_id=wa_id,
            company_id=row["company_id"],
            year=row["year"],
            month=row["month"],
            quarter=row["quarter"],
            volume=row["volume"],
            unit_of_measurement=row["unit_of_measurement"],
        )
        records.append(record)

        # Create checker_status_log model instance
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{wa_id}",
            record_id=wa_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_log_objects.append(checker_log)

    # Insert records into EnviWaterAbstraction
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call stored procedure
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := TRUE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
        print("Stored procedure executed successfully")
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()

    # Insert checker_status_log using ORM model
    try:
        db.bulk_save_objects(checker_log_objects)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()

    return len(records)

def bulk_create_water_discharge(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="WD",
            company_id=company_id,
            year=year,
            model_class=EnviWaterDischarge,
            id_field="wd_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build discharge records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        wd_id = id_mapping[i]
        
        # Create discharge record
        record = EnviWaterDischarge(
            wd_id=wd_id,
            company_id=row["company_id"],
            year=row["year"],
            quarter=row["quarter"],
            volume=row["volume"],
            unit_of_measurement=row["unit_of_measurement"],
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{wd_id}",
            record_id=wd_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := TRUE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

def bulk_create_water_consumption(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="WC",
            company_id=company_id,
            year=year,
            model_class=EnviWaterConsumption,
            id_field="wc_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build consumption records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        wc_id = id_mapping[i]
        
        # Create consumption record
        record = EnviWaterConsumption(
            wc_id=wc_id,
            company_id=row["company_id"],
            year=row["year"],
            quarter=row["quarter"],
            volume=row["volume"],
            unit_of_measurement=row["unit_of_measurement"],
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{wc_id}",
            record_id=wc_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := TRUE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

def bulk_create_electric_consumption(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="EC",
            company_id=company_id,
            year=year,
            model_class=EnviElectricConsumption,
            id_field="ec_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build electric consumption records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        ec_id = id_mapping[i]
        
        # Create electric consumption record
        record = EnviElectricConsumption(
            ec_id=ec_id,
            company_id=row["company_id"],
            source=row["source"],
            unit_of_measurement=row["unit_of_measurement"],
            consumption=row["consumption"],
            quarter=row["quarter"],
            year=row["year"]            
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{ec_id}",
            record_id=ec_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := TRUE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

def bulk_create_non_hazard_waste(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="NHW",
            company_id=company_id,
            year=year,
            model_class=EnviNonHazardWaste,
            id_field="nhw_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build non-hazard waste records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        nhw_id = id_mapping[i]
        
        # Create non-hazard waste record
        record = EnviNonHazardWaste(
            nhw_id=nhw_id,
            company_id=row["company_id"],
            metrics=row["metrics"],
            unit_of_measurement=row["unit_of_measurement"],
            waste=row["waste"],
            month=row["month"],
            quarter=row["quarter"],
            year=row["year"]            
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{nhw_id}",
            record_id=nhw_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := TRUE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

def bulk_create_hazard_waste_generated(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="HWG",
            company_id=company_id,
            year=year,
            model_class=EnviHazardWasteGenerated,
            id_field="hwg_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build hazard waste generated records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        hwg_id = id_mapping[i]
        
        # Create hazard waste generated record
        record = EnviHazardWasteGenerated(
            hwg_id=hwg_id,
            company_id=row["company_id"],
            metrics=row["metrics"],
            unit_of_measurement=row["unit_of_measurement"],
            waste_generated=row["waste_generated"],
            quarter=row["quarter"],
            year=row["year"]            
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{hwg_id}",
            record_id=hwg_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := TRUE,
                load_hazard_waste_disposed := FALSE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

def bulk_create_hazard_waste_disposed(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="HWD",
            company_id=company_id,
            year=year,
            model_class=EnviHazardWasteDisposed,
            id_field="hwd_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build hazard waste disposed records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        hwd_id = id_mapping[i]
        
        # Create hazard waste disposed record
        record = EnviHazardWasteDisposed(
            hwd_id=hwd_id,
            company_id=row["company_id"],
            metrics=row["metrics"],
            unit_of_measurement=row["unit_of_measurement"],
            waste_disposed=row["waste_disposed"],
            year=row["year"]            
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{hwd_id}",
            record_id=hwd_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := TRUE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

def bulk_create_diesel_consumption(db: Session, rows: list[dict]) -> int:
    if not rows:
        return 0
        
    records = []
    checker_logs = []
    
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
            indicator="DC",
            company_id=company_id,
            year=year,
            model_class=EnviDieselConsumption,
            id_field="dc_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Build diesel consumption records and collect logs
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        dc_id = id_mapping[i]
        
        # Create diesel consumption record
        record = EnviDieselConsumption(
            dc_id=dc_id,
            company_id=row["company_id"],
            cp_id=row["cp_id"],
            unit_of_measurement=row["unit_of_measurement"],
            consumption=row["consumption"],
            date=row["date"]        
        )
        records.append(record)
        
        # Create CheckerStatus record
        status_time = base_timestamp + timedelta(hours=i + 1)
        checker_log = RecordStatus(
            cs_id=f"CS-{dc_id}",
            record_id=dc_id,
            status_id="URS",
            status_timestamp=status_time,
            remarks="real-data inserted"
        )
        checker_logs.append(checker_log)
    
    # Insert records
    db.bulk_save_objects(records)
    db.commit()

    """
    INSERT AUDIT LOGIC HERE 
    """

    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := TRUE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
             )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    # Insert checker_status_log entries using CheckerStatus model
    try:
        db.bulk_save_objects(checker_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
        
    return len(records)

# ====================================== EDIT ENVI RECORD ======================================
def update_water_abstraction(db: Session, wa_id: str, data: dict):
    record = get_one(db, EnviWaterAbstraction, "wa_id", wa_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := TRUE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_water_discharge(db: Session, wd_id: str, data: dict):    
    record = get_one(db, EnviWaterDischarge, "wd_id", wd_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := TRUE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_water_consumption(db: Session, wc_id: str, data: dict):
    record = get_one(db, EnviWaterConsumption, "wc_id", wc_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := TRUE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_electric_consumption(db: Session, ec_id: str, data: dict):
    record = get_one(db, EnviElectricConsumption, "ec_id", ec_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := TRUE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_non_hazard_waste(db: Session, nhw_id: str, data: dict):
    record = get_one(db, EnviNonHazardWaste, "nhw_id", nhw_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := TRUE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_hazard_waste_generated(db: Session, hwg_id: str, data: dict):
    record = get_one(db, EnviHazardWasteGenerated, "hwg_id", hwg_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := TRUE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_hazard_waste_disposed(db: Session, hwd_id: str, data: dict):
    record = get_one(db, EnviHazardWasteDisposed, "hwd_id", hwd_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := FALSE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

def update_diesel_consumption(db: Session, dc_id: str, data: dict):
    record = get_one(db, EnviDieselConsumption, "dc_id", dc_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_envi_silver(
                load_company_property := FALSE,
                load_water_abstraction := FALSE,
                load_water_discharge := FALSE,
                load_water_consumption := FALSE,
                load_diesel_consumption := TRUE,
                load_electric_consumption := FALSE,
                load_non_hazard_waste := FALSE,
                load_hazard_waste_generated := FALSE,
                load_hazard_waste_disposed := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record

# =============================== HR DATA ======================================
# =========== RETRIEVE DATA BY ID ===========
# --- Demographics ---
def get_employee_demographics_by_id(db: Session, employee_id: str):
    return get_one(db, HRDemographics, "employee_id", employee_id)

# --- Tenure ---
def get_employee_tenure_by_id(db: Session, tr_id: str):
    return get_one(db, HRTenure, "tr_id", tr_id)

# --- Safety Workdata ---
def get_safety_workdata_by_id(db: Session, sw_id: str):
    return get_one(db, HRSafetyWorkdata, "sw_id", sw_id)

# --- Training ---
def get_training_by_id(db: Session, training_id: str):
    return get_one(db, HRTraining, "training_id", training_id)

# --- Parental Leave ---
def get_parental_leave_by_id(db: Session, pl_id: str):
    return get_one(db, HRParentalLeave, "pl_id", pl_id)

# --- Occupational Safety and Health ---
def get_osh_by_id(db: Session, osh_id: str):
    return get_one(db, HROsh, "osh_id", osh_id)

# =========== INSERT SINGLE DATA ===========
# --- Employability ---
def insert_employability(db: Session, data: dict):
    
    record_demo = HRDemographics(
        employee_id=data["employee_id"],
        gender=data["gender"],
        birthdate=data["birthdate"],
        position_id=data["position_id"],
        p_np=data["p_np"],
        company_id=data["company_id"],
        employment_status=data["employment_status"]
    )
    db.add(record_demo)
    db.commit()
    db.refresh(record_demo)
    
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{data['employee_id']}",
            record_id=data["employee_id"],
            status_id="URH",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := TRUE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Demographics: Error executing stored procedure: {e}")
        db.rollback()
    
    print("Creating HRTenure object with:", data["employee_id"], data["start_date"], data.get("end_date"))
    record_tenure = HRTenure(
        employee_id=data["employee_id"],
        start_date=data["start_date"],
        end_date=data.get("end_date", None)
    )
    db.add(record_tenure)
    db.commit()
    db.refresh(record_tenure)
    try:
        print("Calling load_hr_silver for tenure...")
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := TRUE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Tenure: Error executing stored procedure: {e}")
        db.rollback()
        
    return record_demo.employee_id

# --- Safety Workdata ---
def insert_safety_workdata(db: Session, data: dict):
    safety_workdata_id = id_generation(db, "SWD", HRSafetyWorkdata.safety_workdata_id)
    
    record = HRSafetyWorkdata(
        safety_workdata_id=safety_workdata_id,
        company_id=data["company_id"],
        contractor=data["contractor"],
        date=data["date"],
        manpower=data["manpower"],
        manhours=data["manhours"]
    )
    
    db.add(record)
    db.commit()
    db.refresh(record)
    
    record_Silver = HRSafetyWorkdataSilver(
        safety_workdata_id=safety_workdata_id,
        company_id=data["company_id"],
        contractor=data["contractor"],
        date=data["date"],
        manpower=data["manpower"],
        manhours=data["manhours"]
    )
    db.add(record_Silver)
    db.commit()
    db.refresh(record_Silver)
    
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{safety_workdata_id}",
            record_id=safety_workdata_id,
            status_id="URH",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := TRUE,
                load_occupational_safety_health := FALSE,
                load_from_sql := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    return safety_workdata_id

# --- Parental Leave ---
def insert_parental_leave(db: Session, data: dict):
    parental_leave_id = id_generation(db, "PL", HRParentalLeave.parental_leave_id)
    
    start_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
    num_days = int(data["days"])

    end_date = start_date + timedelta(days=num_days)
    months_availed = num_days // 30
    
    record = HRParentalLeave(
        parental_leave_id=parental_leave_id,
        employee_id=data["employee_id"],
        type_of_leave=data["type_of_leave"],
        date=data["date"],
        days=data["days"],
        # end_date=end_date,
        # months_availed=months_availed
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    
    record_Silver = HRParentalLeaveSilver(
        parental_leave_id=parental_leave_id,
        employee_id=data["employee_id"],
        type_of_leave=data["type_of_leave"],
        date=data["date"],
        days=data["days"],
        end_date=end_date,
        months_availed=months_availed
    )
    db.add(record_Silver)
    db.commit()
    db.refresh(record_Silver)
    
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{parental_leave_id}",
            record_id=parental_leave_id,
            status_id="URH",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()


    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := TRUE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    return parental_leave_id

# --- Training ---
def insert_training(db: Session, data: dict):
    training_id = id_generation(db, "TR", HRTraining.training_id)
    
    total_training_hours = int(data["training_hours"]) * int(data["number_of_participants"])
    
    record = HRTraining(
        training_id=training_id,
        company_id=data["company_id"],
        date=data["date"],
        training_title=data["training_title"],
        training_hours=data["training_hours"],
        number_of_participants=data["number_of_participants"],
        #total_training_hours=total_training_hours
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    
    record_Silver = HRTrainingSilver(
        training_id=training_id,
        company_id=data["company_id"],
        date=data["date"],
        training_title=data["training_title"],
        training_hours=data["training_hours"],
        number_of_participants=data["number_of_participants"],
        total_training_hours=total_training_hours
    )
    db.add(record_Silver)
    db.commit()
    db.refresh(record_Silver)
    
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{training_id}",
            record_id=training_id,
            status_id="URH",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := TRUE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    return training_id

# --- Occupational Safety Health ---
def insert_occupational_safety_health(db: Session, data: dict):
    osh_id = id_generation(db, "OSH", HROsh.osh_id)
    
    record = HROsh(
        osh_id=osh_id,
        company_id=data["company_id"],
        workforce_type=data["workforce_type"],
        lost_time=data["lost_time"].upper() == "TRUE",
        date=data["date"],
        incident_type=data["incident_type"],
        incident_title=data["incident_title"],
        incident_count=data["incident_count"]
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    
    
    try:
        checker_log = RecordStatus(
            cs_id=f"CS-{osh_id}",
            record_id=osh_id,
            status_id="URH",
            status_timestamp=datetime.now(),
            remarks="real-data inserted"
        )
        db.add(checker_log)
        db.commit()
    except Exception as e:
        print(f"Error inserting checker status log: {e}")
        db.rollback()
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := TRUE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    return osh_id

from datetime import datetime

today_str = datetime.today().strftime('%Y%m%d')

# ====================================== UPDATE HR DATA ======================================
# --- Employability ---
def update_employability(db: Session, employee_id: str, data_demo: dict, data_tenure: dict):
    # Demographics
    record_demo = get_one(db, HRDemographics, "employee_id", employee_id)
    if not record_demo:
        return None
    
    for key, value in data_demo.items():
        setattr(record_demo, key, value)
    
    db.commit()
    db.refresh(record_demo)
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := TRUE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    # Tenure
    record_tenure = get_one(db, HRTenure, "employee_id", employee_id)
    if not record_tenure:
        return None
    
    for key, value in data_tenure.items():
        setattr(record_tenure, key, value)
    
    db.commit()
    db.refresh(record_tenure)
    
    try:
       
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := TRUE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record_demo, record_tenure

# --- Safety Workdata ---
def update_safety_workdata(db: Session, safety_workdata_id: str, data):
    record = get_one(db, HRSafetyWorkdata, "safety_workdata_id", safety_workdata_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := TRUE,
                load_occupational_safety_health := FALSE,
                load_from_sql := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# --- Parental Leave ---
def update_parental_leave(db: Session, parental_leave_id: str, data):
    record = get_one(db, HRParentalLeave, "parental_leave_id", parental_leave_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := TRUE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# --- Occupational Safety Health ---
def update_occupational_safety_health(db: Session, osh_id: str, data):
    record = get_one(db, HROsh, "osh_id", osh_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := TRUE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# --- Training ---
def update_training(db: Session, training_id: str, data):
    record = get_one(db, HRTraining, "training_id", training_id)
    if not record:
        return None
    
    for key, value in data.items():
        setattr(record, key, value)
    
    db.commit()
    db.refresh(record)
    
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := TRUE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := FALSE
            )
        """))
        db.commit()
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    return record

# ====================================== BULK INSERT HR DATA ======================================
# --- Employability ---
def insert_employability_bulk(db: Session, rows) -> int:
    if not rows:
        return 0
        
    record_demo = []
    record_tenure = []
    record_logs = []
    
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        # Create demographics record
        record = HRDemographics(
            employee_id=row["employee_id"],
            gender=row["gender"],
            birthdate=row["birthdate"],
            position_id=row["position_id"],
            p_np=row["p_np"],
            company_id=row["company_id"],
            employment_status=row["employment_status"]
        )
        record_demo.append(record)
        
        # Create RecordLog record
        status_time = base_timestamp + timedelta(hours=i + 1)
        record_log = RecordStatus(
            cs_id=f"CS-{row['employee_id']}",
            record_id=row["employee_id"],
            status_id="URH",
            status_timestamp=status_time.date(),
            remarks="real-data inserted"
        )
        record_logs.append(record_log)
    
    # Insert demographics records
    db.bulk_save_objects(record_demo)
    db.commit()
    
    db.bulk_save_objects(record_logs)
    db.commit()
        
    # Insert tenure records
    for i, row in enumerate(rows):
        record = HRTenure(
            employee_id=row["employee_id"],
            start_date=row["start_date"],
            end_date=row["end_date"]
        )
        record_tenure.append(record)
    db.bulk_save_objects(record_tenure)
    db.commit()

    # Call the stored procedure to load demographics
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := TRUE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
        print("Stored procedure executed successfully")
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()

    # Call the stored procedure to load tenure
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := TRUE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        db.commit()
        print("Stored procedure executed successfully")
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
    
    return record_demo, record_tenure

# --- Safety Workdata ---
def insert_safety_workdata_bulk (db:Session, rows) -> int:
    if not rows:
        return 0
        
    records = []
    records_Silver = []
    record_logs = []
    
    last_id_row = db.query(HRSafetyWorkdata).order_by(HRSafetyWorkdata.safety_workdata_id.desc()).first()
    if last_id_row and last_id_row.safety_workdata_id.startswith("SWD"):
        last_num = int(last_id_row.safety_workdata_id[-4:])
    else:
        last_num = 0
        
    today_str = datetime.today().strftime("%Y%m%d")
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        new_num = last_num + i + 1
        safety_workdata_id = f"SWD{today_str}{str(new_num).zfill(4)}"
        # safety_workdata_id = id_generation(db, "SWD", HRSafetyWorkdata.safety_workdata_id)
        # Create safety workdata record
        record = HRSafetyWorkdata(
            safety_workdata_id=safety_workdata_id,
            company_id=row["company_id"],
            contractor=row["contractor"],
            date = row["date"],
            manpower = row["manpower"],
            manhours = row["manhours"],
        )
        records.append(record)
        
        record_Silver = HRSafetyWorkdataSilver(
            safety_workdata_id=safety_workdata_id,
            company_id=row["company_id"],
            contractor=row["contractor"],
            date = row["date"],
            manpower = row["manpower"],
            manhours = row["manhours"],
        )
        records_Silver.append(record_Silver)
        
        status_time = base_timestamp + timedelta(hours=i + 1)
        record_log = RecordStatus(
            cs_id=f"CS-{safety_workdata_id}",
            record_id=safety_workdata_id,
            status_id="URH",
            status_timestamp=status_time.date(),
            remarks="real-data inserted"
        )
        record_logs.append(record_log)
        
    # Insert safety workdata records
    db.bulk_save_objects(records)
    db.commit()
    
    db.bulk_save_objects(records_Silver)
    db.commit()
    
    # Call the stored procedure after inserting data
    # try:
    #     db.execute(text("""
    #         CALL silver.load_hr_silver(
    #             load_demographics := FALSE,
    #             load_tenure := FALSE,
    #             load_parental_leave := FALSE,
    #             load_training := FALSE,
    #             load_safety_workdata := TRUE,
    #             load_occupational_safety_health := FALSE,
    #             load_from_sql := TRUE
    #         )
    #     """))
        
    #     db.commit()
    #     print("Stored procedure executed successfully")
        
    # except Exception as e:
    #     print(f"Error executing stored procedure: {e}")
    #     db.rollback()
        
    
    # Insert record_status_log entries using RecordStatus model
    try:
        db.bulk_save_objects(record_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
    
    return records

# --- Parental Leave ---
def insert_parental_leave_bulk (db:Session, rows) -> int:
    if not rows:
        return 0
        
    records = []
    records_Silver = []
    record_logs = []
    
    last_id_row = db.query(HRParentalLeave).order_by(HRParentalLeave.parental_leave_id.desc()).first()
    if last_id_row and last_id_row.parental_leave_id.startswith("PL"):
        last_num = int(last_id_row.parental_leave_id[-4:])
    else:
        last_num = 0
        
    today_str = datetime.today().strftime("%Y%m%d")
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        new_num = last_num + i + 1
        parental_leave_id = f"PL{today_str}{str(new_num).zfill(4)}"
        #parental_leave_id = id_generation(db, "PL", HRParentalLeave.parental_leave_id)
        
        if isinstance(row["date"], str):
            start_date = datetime.strptime(row["date"], "%Y-%m-%d").date()
        else:
            start_date = row["date"]
        num_days = int(row["days"])

        end_date = start_date + timedelta(days=num_days)
        months_availed = num_days // 30
        
        # Create parental leave record bronze
        record = HRParentalLeave(
            parental_leave_id=parental_leave_id,
            employee_id=row["employee_id"],
            type_of_leave=row["type_of_leave"],
            date=row["date"],
            days=row["days"],
            # end_date=end_date,
            # months_availed=months_availed
        )
        records.append(record)
        
        # Create parental leave silver
        record_Silver = HRParentalLeaveSilver(
            parental_leave_id=parental_leave_id,
            employee_id=row["employee_id"],
            type_of_leave=row["type_of_leave"],
            date=row["date"],
            days=row["days"],
            end_date=end_date,
            months_availed=months_availed
        )
        records_Silver.append(record_Silver)
        
        status_time = base_timestamp + timedelta(hours=i + 1)
        record_log = RecordStatus(
            cs_id=f"CS-{parental_leave_id}",
            record_id=parental_leave_id,
            status_id="URH",
            status_timestamp=status_time.date(),
            remarks="real-data inserted"
        )
        record_logs.append(record_log)
        
    # Insert parental leave records
    db.bulk_save_objects(records)
    db.commit()
    
    db.bulk_save_objects(records_Silver)
    db.commit()
    
    # Call the stored procedure after inserting data
    # try:
    #     logger.debug('Load Procedure')
    #     db.execute(text("""
    #         CALL silver.load_hr_silver(
    #             load_demographics := FALSE,
    #             load_tenure := FALSE,
    #             load_parental_leave := TRUE,
    #             load_training := FALSE,
    #             load_safety_workdata := FALSE,
    #             load_occupational_safety_health := FALSE,
    #             load_from_sql := FALSE
    #         )
    #     """))
        
    #     db.commit()
    #     logger.debug('This mean it passes')
    #     print("Stored procedure executed successfully")
        
    # except Exception as e:
    #     print(f"Error executing stored procedure: {e}")
    #     logger.debug('Nope')
    #     db.rollback()
        
    
    # Insert record_status_log entries using RecordStatus model
    try:
        db.bulk_save_objects(record_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
    
    return records

# --- Occupational Safety Health ---
def insert_occupational_safety_health_bulk (db:Session, rows) -> int:
    if not rows:
        return 0
        
    records = []
    record_logs = []
    
    last_id_row = db.query(HROsh).order_by(HROsh.osh_id.desc()).first()
    if last_id_row and last_id_row.osh_id.startswith("OSH"):
        last_num = int(last_id_row.osh_id[-4:])
    else:
        last_num = 0
        
    today_str = datetime.today().strftime("%Y%m%d")
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        new_num = last_num + i + 1
        osh_id = f"OSH{today_str}{str(new_num).zfill(4)}"
        #osh_id = id_generation(db, "OSH", HROsh.osh_id)
        
        # Create occupational safety health record
        record = HROsh(
            osh_id=osh_id,
            company_id=row["company_id"],
            workforce_type=row["workforce_type"],
            lost_time=row["lost_time"],
            date=row["date"],
            incident_type=row["incident_type"],
            incident_title=row["incident_title"],
            incident_count=row["incident_count"]
        )
        records.append(record)
        
        status_time = base_timestamp + timedelta(hours=i + 1)
        record_log = RecordStatus(
            cs_id=f"CS-{osh_id}",
            record_id=osh_id,
            status_id="URH",
            status_timestamp=status_time.date(),
            remarks="real-data inserted"
        )
        record_logs.append(record_log)
        
    # Insert occupational safety health records
    db.bulk_save_objects(records)
    db.commit()
    
    
    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := FALSE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := TRUE,
                load_from_sql := TRUE
            )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    
    # Insert record_status_log entries using RecordStatus model
    try:
        db.bulk_save_objects(record_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
    
    return records

# --- Training ---
def insert_training_bulk (db:Session, rows) -> int:
    if not rows:
        return 0
        
    records = []
    record_logs = []
    
    last_id_row = db.query(HRTraining).order_by(HRTraining.training_id.desc()).first()
    if last_id_row and last_id_row.training_id.startswith("TR"):
        last_num = int(last_id_row.training_id[-4:])
    else:
        last_num = 0
        
    today_str = datetime.today().strftime("%Y%m%d")
    base_timestamp = datetime.now()
    for i, row in enumerate(rows):
        new_num = last_num + i + 1
        training_id = f"TR{today_str}{str(new_num).zfill(4)}"
        #training_id = id_generation(db, "TR", HRTraining.training_id)
        #total_training_hours = int(row["training_hours"]) * int(row["number_of_participants"])
        
        # Create training record
        record = HRTraining(
            training_id=training_id,
            company_id=row["company_id"],
            date=row["date"],
            training_title=row["training_title"],
            training_hours=row["training_hours"],
            number_of_participants=row["number_of_participants"],
            #total_training_hours=total_training_hours
        )
        records.append(record)
        
        status_time = base_timestamp + timedelta(hours=i + 1)
        record_log = RecordStatus(
            cs_id=f"CS-{training_id}",
            record_id=training_id,
            status_id="URH",
            status_timestamp=status_time.date(),
            remarks="real-data inserted"
        )
        record_logs.append(record_log)
        
    # Insert occupational safety health records
    db.bulk_save_objects(records)
    db.commit()
    
    
    # Call the stored procedure after inserting data
    try:
        db.execute(text("""
            CALL silver.load_hr_silver(
                load_demographics := FALSE,
                load_tenure := FALSE,
                load_parental_leave := FALSE,
                load_training := TRUE,
                load_safety_workdata := FALSE,
                load_occupational_safety_health := FALSE,
                load_from_sql := TRUE
            )
        """))
        
        db.commit()
        print("Stored procedure executed successfully")
        
    except Exception as e:
        print(f"Error executing stored procedure: {e}")
        db.rollback()
        
    
    # Insert record_status_log entries using RecordStatus model
    try:
        db.bulk_save_objects(record_logs)
        db.commit()
        print("Checker status logs inserted.")
    except Exception as e:
        print(f"Error inserting checker status logs: {e}")
        db.rollback()
    
    return records
