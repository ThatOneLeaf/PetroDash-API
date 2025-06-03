from sqlalchemy.orm import Session
from .models import EnergyRecords, CSRActivity, CSRProject, CSRProgram, EnviCompanyProperty, EnviWaterAbstraction, EnviWaterDischarge, EnviWaterConsumption, EnviElectricConsumption, EnviDieselConsumption, EnviNonHazardWaste, EnviHazardWasteGenerated, EnviHazardWasteDisposed
from .models import HRDemographics, HRTenure, HRSafetyWorkdata, HRTraining, HRParentalLeave, HROsh
from app.crud.base import get_one, get_many, get_many_filtered, get_all
from app.utils.formatting_id import generate_pkey_id, generate_bulk_pkey_ids
from sqlalchemy import text

# =================== POWER PLANT ENERGY DATA =================
def get_energy_record_by_id(db: Session, energy_id: str):
    return get_one(db, EnergyRecords, "energy_id", energy_id)

def get_all_energy_records(db: Session):
    return get_all(db, EnergyRecords)

def get_filtered_energy_records(db: Session, filters: dict, skip: int = 0, limit: int = 100):
    return get_many_filtered(db, EnergyRecords, filters=filters, skip=skip, limit=limit)

# ============================ CSR/HELP DATA============================
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

    # Call the stored procedure after inserting data
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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_water_discharge(db: Session, rows: list[dict]) -> int:
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
            indicator="WD",
            company_id=company_id,
            year=year,
            model_class=EnviWaterDischarge,
            id_field="wd_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviWaterDischarge(
            wd_id=id_mapping[i],
            company_id=row["company_id"],
            year=row["year"],
            quarter=row["quarter"],
            volume=row["volume"],
            unit_of_measurement=row["unit_of_measurement"],
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_water_consumption(db: Session, rows: list[dict]) -> int:
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
            indicator="WC",
            company_id=company_id,
            year=year,
            model_class=EnviWaterConsumption,
            id_field="wc_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviWaterConsumption(
            wc_id=id_mapping[i],
            company_id=row["company_id"],
            year=row["year"],
            quarter=row["quarter"],
            volume=row["volume"],
            unit_of_measurement=row["unit_of_measurement"],
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_electric_consumption(db: Session, rows: list[dict]) -> int:
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
            indicator="EC",
            company_id=company_id,
            year=year,
            model_class=EnviElectricConsumption,
            id_field="ec_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviElectricConsumption(
            ec_id=id_mapping[i],
            company_id=row["company_id"],
            source=row["source"],
            unit_of_measurement=row["unit_of_measurement"],
            consumption=row["consumption"],
            quarter=row["quarter"],
            year=row["year"]            
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_non_hazard_waste(db: Session, rows: list[dict]) -> int:
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
            indicator="NHW",
            company_id=company_id,
            year=year,
            model_class=EnviNonHazardWaste,
            id_field="nhw_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviNonHazardWaste(
            nhw_id=id_mapping[i],
            company_id=row["company_id"],
            metrics=row["metrics"],
            unit_of_measurement=row["unit_of_measurement"],
            waste=row["waste"],
            month=row["month"],
            quarter=row["quarter"],
            year=row["year"]            
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_hazard_waste_generated(db: Session, rows: list[dict]) -> int:
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
            indicator="HWG",
            company_id=company_id,
            year=year,
            model_class=EnviHazardWasteGenerated,
            id_field="hwg_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviHazardWasteGenerated(
            hwg_id=id_mapping[i],
            company_id=row["company_id"],
            metrics=row["metrics"],
            unit_of_measurement=row["unit_of_measurement"],
            waste_generated=row["waste_generated"],
            quarter=row["quarter"],
            year=row["year"]            
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_hazard_waste_disposed(db: Session, rows: list[dict]) -> int:
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
            indicator="HWD",
            company_id=company_id,
            year=year,
            model_class=EnviHazardWasteDisposed,
            id_field="hwd_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviHazardWasteDisposed(
            hwd_id=id_mapping[i],
            company_id=row["company_id"],
            metrics=row["metrics"],
            unit_of_measurement=row["unit_of_measurement"],
            waste_disposed=row["waste_disposed"],
            year=row["year"]            
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

def bulk_create_diesel_consumption(db: Session, rows: list[dict]) -> int:
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
            indicator="DC",
            company_id=company_id,
            year=year,
            model_class=EnviDieselConsumption,
            id_field="dc_id",
            count=len(row_list)
        )
        
        for (original_index, _), generated_id in zip(row_list, ids):
            id_mapping[original_index] = generated_id
    
    # Create records with proper IDs
    for i, row in enumerate(rows):
        record = EnviDieselConsumption(
            dc_id=id_mapping[i],
            company_id=row["company_id"],
            cp_id=row["cp_id"],
            unit_of_measurement=row["unit_of_measurement"],
            consumption=row["consumption"],
            date=row["date"]        
        )
        records.append(record)
    
    db.bulk_save_objects(records)
    db.commit()

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
        # You can choose to raise the exception or handle it gracefully
        # raise e
    return len(records)

# =================== HR DATA =================
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