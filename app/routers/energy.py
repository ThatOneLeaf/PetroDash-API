from fastapi import APIRouter, Depends, Query, HTTPException, Form, UploadFile, File
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text, update, select, bindparam, ARRAY, String, Integer
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut, AddEnergyRecord
from app.public.models import RecordStatus
from app.dependencies import get_db
from app.crud.base import get_one, get_all, get_many, get_many_filtered, get_one_filtered
from datetime import datetime
import pandas as pd
import io
import math
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


# ====================== dashboard ====================== #

@router.get("/energy_dashboard_raw", response_model=List[dict])
def get_energy_dashboard_raw(
    power_plant_ids: Optional[List[str]] = Query(None, alias="p_power_plant_id"),
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    generation_sources: Optional[List[str]] = Query(None, alias="p_generation_source"),
    provinces: Optional[List[str]] = Query(None, alias="p_province"),
    months: Optional[List[int]] = Query(None, alias="p_month"),
    quarters: Optional[List[int]] = Query(None, alias="p_quarter"),
    years: Optional[List[int]] = Query(None, alias="p_year"),
    x: Optional[str] = Query(None, alias="p_x"),  # grouping by e.g., power_plant_id
    y: Optional[str] = Query(None, alias="p_y"),  # time granularity: monthly, quarterly, yearly
    v: Optional[str] = Query("energy_generated_mwh", alias="p_v"),  # aggregated metric
    db: Session = Depends(get_db)
):
    try:
        logging.info("Fetching raw energy records.")

        query = text("""
            SELECT * 
            FROM gold.func_fact_energy(                
                :power_plant_ids,
                :company_ids,
                :generation_sources,
                :provinces,
                :months,
                :quarters,
                :years
            );
        """)

        result = db.execute(query, {
            "power_plant_ids": power_plant_ids,
            "company_ids": company_ids,
            "generation_sources": generation_sources,
            "provinces": provinces,
            "months": months,
            "quarters": quarters,
            "years": years
        })

        rows = result.fetchall()
        columns = result.keys()
        data = [dict(zip(columns, row)) for row in rows]

        if not x or not y or not v:
            # Return raw data if no pivot requested
            return data

        df = pd.DataFrame(data)

        # Define time group column based on y
        if y == "monthly":
            df["time_group"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
        elif y == "quarterly":
            df["time_group"] = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)
        elif y == "yearly":
            df["time_group"] = df["year"].astype(str)
        else:
            raise HTTPException(status_code=400, detail="Invalid y parameter. Use monthly, quarterly, or yearly.")

        if x not in df.columns or v not in df.columns:
            raise HTTPException(status_code=400, detail="Invalid x or v parameter.")

        # Perform pivot: rows = x, columns = time_group, values = v
        pivot = df.pivot_table(
            index=x,
            columns="time_group",
            values=v,
            aggfunc="sum",
            fill_value=0
        ).reset_index()

        # Convert pivot result to records
        pivot_result = pivot.to_dict(orient="records")

        logging.info(f"Pivot result: {len(pivot_result)} rows.")
        return pivot_result

    except Exception as e:
        logging.error(f"Error retrieving or processing records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

    
# ====================== dashboard ====================== #
def process_query_data(
    db: Session,
    query_str: str,  # ✅ Query is now passed as a string
    x: str,
    y: str,
    v: List[str],
    power_plant_ids: Optional[List[str]] = None,
    company_ids: Optional[List[str]] = None,
    generation_sources: Optional[List[str]] = None,
    provinces: Optional[List[str]] = None,
    months: Optional[List[int]] = None,
    quarters: Optional[List[int]] = None,
    years: Optional[List[int]] = None
) -> Dict[str, Any]:
    # Execute query
    query = text(query_str)
    result = db.execute(query, {
        "power_plant_ids": power_plant_ids,
        "company_ids": company_ids,
        "generation_sources": generation_sources,
        "provinces": provinces,
        "months": months,
        "quarters": quarters,
        "years": years
    })

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        return {}

    if "month_name" in df.columns and pd.api.types.is_string_dtype(df["month_name"]):
        df["month_name"] = df["month_name"].str.strip()

    # Build period column
    if y == "monthly":
        df["period"] = df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    elif y == "quarterly":
        df["period"] = df["year"].astype(str) + "-Q" + df["quarter"].astype(str)
    elif y == "yearly":
        df["period"] = df["year"].astype(str)
    else:
        raise HTTPException(status_code=400, detail="Invalid y value. Use 'monthly', 'quarterly', or 'yearly'.")

    if x not in df.columns:
        raise HTTPException(status_code=400, detail=f"'{x}' not found in data columns.")
    for metric in v:
        if metric not in df.columns:
            raise HTTPException(status_code=400, detail=f"'{metric}' not found in data columns.")

    fixed_cols = {x, "period", *v}
    other_cols = [col for col in df.columns if col not in fixed_cols]

    agg_dict = {metric: 'sum' for metric in v}
    for col in other_cols:
        agg_dict[col] = 'first'

    grouped_df = df.groupby([x, "period"], dropna=False).agg(agg_dict).reset_index()

    # Line chart
    line_graph = {
        metric: [
            {
                "name": key,
                "data": [{"x": row["period"], "y": float(row[metric])} for _, row in group.iterrows()]
            }
            for key, group in grouped_df.groupby(x)
        ]
        for metric in v
    }

    # Pie chart
    pie_chart = {
        metric: [
            {
                "name": row[x],
                "value": float(row[metric]),
                "percent": float(row[metric]) / float(total) * 100 if total else 0
            }
            for _, row in grouped_df.groupby(x, dropna=False)[metric]
                .sum()
                .reset_index()
                .pipe(lambda df: (
                    df.assign(_total=df[metric].sum())  # add total column
                ))
                .iterrows()
        ]
        for metric in v
        for total in [grouped_df.groupby(x, dropna=False)[metric].sum().sum()]  # compute total once per metric
    }

    # Bar chart
    bar_chart = {
        metric: [
            {"name": row[x], "value": float(row[metric])}
            for _, row in grouped_df.groupby(x, dropna=False)[metric].sum()
            .reset_index().sort_values(metric, ascending=False).iterrows()
        ]
        for metric in v
    }

    # Totals
    totals = {metric: float(df[metric].sum()) for metric in v}

    return {
        "line_graph": line_graph,
        "bar_chart": bar_chart,
        "pie_chart": pie_chart,
        "totals": totals
    }

def to_nullable_list(param):
    return param if param else None

def process_raw_data(
    db: Session,
    query_str: str,
    power_plant_ids: Optional[List[str]] = None,
    company_ids: Optional[List[str]] = None,
    generation_sources: Optional[List[str]] = None,
    provinces: Optional[List[str]] = None,
    months: Optional[List[int]] = None,
    quarters: Optional[List[int]] = None,
    years: Optional[List[int]] = None
) -> List[Dict[str, Any]]:

    from sqlalchemy.dialects.postgresql import ARRAY

    def nullable(val):
        return val if val else None

    query = text(query_str).bindparams(
        bindparam("power_plant_ids", type_=ARRAY(String)),
        bindparam("company_ids", type_=ARRAY(String)),
        bindparam("generation_sources", type_=ARRAY(String)),
        bindparam("provinces", type_=ARRAY(String)),
        bindparam("months", type_=ARRAY(Integer)),
        bindparam("quarters", type_=ARRAY(Integer)),
        bindparam("years", type_=ARRAY(Integer)),
    )

    result = db.execute(query, {
        "power_plant_ids": nullable(power_plant_ids),
        "company_ids": nullable(company_ids),
        "generation_sources": nullable(generation_sources),
        "provinces": nullable(provinces),
        "months": nullable(months),
        "quarters": nullable(quarters),
        "years": nullable(years),
    })

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if df.empty:
        return []

    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    return df.to_dict(orient="records")



def parse_comma_separated(value: Optional[str]) -> Optional[List[str]]:
    if value:
        return [v.strip() for v in value.split(",") if v.strip()]
    return None

def normalize_list(lst):
    if lst is None or len(lst) == 0:
        return None
    return lst

@router.get("/energy_dashboard", response_model=Dict[str, Any])
def get_energy_dashboard(
    p_company_id: Optional[str] = Query(None),
    p_power_plant_id: Optional[str] = Query(None),
    p_generation_source: Optional[str] = Query(None),
    p_province: Optional[str] = Query(None),
    p_month: Optional[str] = Query(None),
    p_quarter: Optional[str] = Query(None),
    p_year: Optional[str] = Query(None),
    x: str = Query("company_id"),
    y: str = Query("monthly"),
    db: Session = Depends(get_db)
):
    # Parse and normalize all filter parameters
    company_ids = normalize_list(parse_comma_separated(p_company_id))
    power_plant_ids = normalize_list(parse_comma_separated(p_power_plant_id))
    generation_sources = normalize_list(parse_comma_separated(p_generation_source))
    provinces = normalize_list(parse_comma_separated(p_province))
    months = normalize_list([int(m) for m in parse_comma_separated(p_month) or []])
    quarters = normalize_list([int(q) for q in parse_comma_separated(p_quarter) or []])
    years = normalize_list([int(y) for y in parse_comma_separated(p_year) or []])

    logging.info(f"Filters - company_ids: {company_ids}, power_plant_ids: {power_plant_ids}, "
                 f"generation_sources: {generation_sources}, provinces: {provinces}, "
                 f"months: {months}, quarters: {quarters}, years: {years}")

    try:
        energy = """
            SELECT 
                *
            FROM gold.func_fact_energy(
                :power_plant_ids,
                :company_ids,
                :generation_sources,
                :provinces,
                :months,
                :quarters,
                :years
            );
        """
        energy_result = process_query_data(
            db=db,
            query_str=energy,
            x=x,
            y=y,
            v=["total_energy_generated", "total_co2_avoidance"],
            power_plant_ids=power_plant_ids,
            company_ids=company_ids,
            generation_sources=generation_sources,
            provinces=provinces,
            months=months,
            quarters=quarters,
            years=years
        )
        
        # format ------------------
        def format_large_number(value):
            if value >= 1_000_000_000:
                return f"{value / 1_000_000_000:.1f}B"
            elif value >= 1_000_000:
                return f"{value / 1_000_000:.1f}M"
            elif value <1:
                return f"{value:,.4f}"
            else:
                return math.ceil(value)

        def format_equivalence(record):
            record["co2_equivalent"] = format_large_number(round(float(record["co2_equivalent"]), 4))
            return record

        equivalence = """
            SELECT 
                energy_generated,
                co2_avoided,
                conversion_value,
                co2_equivalent,
                metric,
                equivalence_category,
                equivalence_label
            FROM gold.func_co2_equivalence_per_metric(
                :power_plant_ids,
                :company_ids,
                :generation_sources,
                :provinces,
                :months,
                :quarters,
                :years
            );
        """

        eq_result = process_raw_data(
            db=db,
            query_str=equivalence,
            power_plant_ids=power_plant_ids,
            company_ids=company_ids,
            generation_sources=generation_sources,
            provinces=provinces,
            months=months,
            quarters=quarters,
            years=years
        )

        # Apply formatting
        equivalence_dict = {
            f"EQ_{i+1}": format_equivalence(record)
            for i, record in enumerate(eq_result)
        }
        
        
        hp = """
            SELECT *
            FROM gold.func_household_powered(
                (:power_plant_ids)::VARCHAR(10)[],
                (:company_ids)::VARCHAR(10)[],
                (:generation_sources)::TEXT[],
                (:provinces)::VARCHAR(30)[],
                (:months)::INT[],
                (:quarters)::INT[],
                (:years)::INT[]
            );
        """

        hp_result = process_query_data(
            db=db,
            query_str=hp,
            x=x,
            y=y,
            v=["est_house_powered"],
            power_plant_ids=power_plant_ids,
            company_ids=company_ids,
            generation_sources=generation_sources,
            provinces=provinces,
            months=months,
            quarters=quarters,
            years=years
        )


        label_query = text("""
            SELECT
                generation_source AS label,
                TRIM(
                    CONCAT(
                        'kg CO₂ avoided = EG(kWh) × ', kg_co2_per_kwh, ' kg CO₂/kWh',
                        CASE 
                            WHEN co2_emitted_kg IS NOT NULL THEN 
                            '; kg CO₂ emitted = EG(kWh) × ' || ROUND(co2_emitted_kg / 1000.0, 6) || ' kg CO₂/kWh; Emission Reduction = kg CO₂ avoided - kg CO₂ emitted'
                            ELSE
                            ''
                        END
                    )
                ) AS formula
            FROM
                ref.ref_emission_factors
        """)

        # This returns each row as a dictionary-like object
        label_result = db.execute(label_query).mappings().all()
        label_dicts = [dict(row) for row in label_result]


        return {
            "energy_data": energy_result,
            "equivalence_data": equivalence_dict,
            "house_powered": hp_result,
            "formula":label_dicts
        }

    except Exception as e:
        logging.error(f"Error retrieving energy records: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

    
# ========= fund alloc -===============
@router.get("/fund_allocation", response_model=List[dict])
def get_fund_allocation(
    power_plant_ids: Optional[List[str]] = Query(None, alias="p_power_plant_id"),
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    ff_ids: Optional[List[str]] = Query(None, alias="p_ff_id"),
    months: Optional[List[int]] = Query(None, alias="p_month"), # numeric
    years: Optional[List[int]] = Query(None, alias="p_year"), 
    ff_category: Optional[str] = Query(None, alias="p_ff_category"), # allocation, beneficiaries
    db: Session = Depends(get_db)
):
    try:
        logging.info("Fetching fund allocation data.")

        query = text("""
            SELECT * FROM gold.func_fund_alloc(
                :power_plant_ids,
                :company_ids,
                :ff_ids,
                :months,
                :years,
                :ff_category
            );
        """)

        result = db.execute(query, {
            "power_plant_ids": power_plant_ids,
            "company_ids": company_ids,
            "ff_ids": ff_ids,
            "months": months,
            "years": years,
            "ff_category": ff_category
        })

        df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        if df.empty:
            return []

        df["month_name"] = df["month_name"].str.strip()
        return df.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error retrieving fund allocation data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
# ====================== year fund alloc ====================== #
@router.get("/fund_allocation_yearly", response_model=List[dict])
def get_fund_allocation_yearly(
    power_plant_ids: Optional[List[str]] = Query(None, alias="p_power_plant_id"),
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    ff_ids: Optional[List[str]] = Query(None, alias="p_ff_id"),
    years: Optional[List[int]] = Query(None, alias="p_year"), 
    db: Session = Depends(get_db)
):
    try:
        logging.info("Fetching yearly fund allocation data.")

        query = text("""
            SELECT * FROM gold.func_fund_alloc_year(
                :power_plant_ids,
                :company_ids,
                :ff_ids,
                :years
            );
        """)

        result = db.execute(query, {
            "power_plant_ids": power_plant_ids,
            "company_ids": company_ids,
            "ff_ids": ff_ids,
            "years": years
        })

        df = pd.DataFrame(result.fetchall(), columns=list(result.keys()))
        if df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error retrieving yearly fund allocation data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


# ====================== template ====================== #
@router.get("/download_template")
def download_template():
    columns = ["date", "energy_generated", "powerPlant", "metric"]
    df = pd.DataFrame(columns=columns)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Template")

    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=energy_template.xlsx"}
    )

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
    
    try:
        # Parse date
        try:
            parsed_date = datetime.strptime(date, "%Y-%m-%d")
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=f"Date parsing error: {str(ve)}")

        # Check for existing record
        try:
            existing = db.query(EnergyRecords).filter(
                EnergyRecords.power_plant_id == powerPlant,
                EnergyRecords.datetime == parsed_date
            ).first()
            if existing:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "duplicate_error",
                        "message": f"Duplicate record: energy data for {powerPlant} on {parsed_date.strftime('%Y-%m-%d')} already exists."
                    }
                )
        except Exception as db_err:
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(db_err)}")

        # Generate ID
        try:
            new_id = generate_energy_id(db)
        except Exception as id_err:
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
            new_log = RecordStatus(
                cs_id="CS-" + new_id,
                record_id=new_id,
                status_id="URS",
                status_timestamp=datetime.now(),
                remarks=remarks
            )

            db.add(new_record)
            db.add(new_log)
            db.commit()
            db.refresh(new_record)
        except Exception as record_err:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to insert record or log: {str(record_err)}")

        # Call stored procedure
        try:
            db.execute(text("CALL silver.load_csv_silver();"))
            db.commit()
        except Exception as proc_err:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Stored procedure error: {str(proc_err)}")

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
        raise http_err

    except Exception as e:
        db.rollback()
        tb = traceback.format_exc()
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}\nTraceback:\n{tb}"
        )



# ====================== bulk add energy record ====================== #
@router.post("/bulk_add")
def bulk_add_energy_record(
    # powerPlant: str = Form(...),
    # checker: str = Form(...),
    # metric: str = Form(...),
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

    if 'date' not in df.columns or 'energy_generated' not in df.columns or 'powerPlant' not in df.columns or 'metric' not in df.columns:
        raise HTTPException(status_code=400, detail="Excel must contain 'date', 'energy_generated', 'powerPlant', and 'metric' columns.")

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
            powerPlant = str(row["powerPlant"])
            metric = str(row["metric"])
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
                #checker_id=checker,
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


