
from fastapi import APIRouter, Depends,  Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List, Dict
from ..dependencies import get_db
import logging
import traceback

router = APIRouter()

# ====================== KPI DATA API ====================== #
@router.get("/kpi-data", response_model=dict)
def get_kpi_data(db: Session = Depends(get_db)):
    """
    Get KPI data for admin dashboard
    """
    try:
        # Total users
        total_users = db.execute(text("""
            SELECT COUNT(*) FROM account
        """)).scalar()

        # Admins
        admins = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_role = 'R01'
        """)).scalar()

        # Executives
        executives = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_role = 'R02'
        """)).scalar()

        # HO Checkers
        ho_checkers = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_role = 'R03'
        """)).scalar()

        # Site Checkers
        site_checkers = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_role = 'R04'
        """)).scalar()

        # Encoders
        encoders = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_role = 'R05'
        """)).scalar()

        # Active users
        active_users = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_status = 'active'
        """)).scalar()

        # Deactivated users
        deactivated_users = db.execute(text("""
            SELECT COUNT(*) FROM account WHERE account_status != 'active'
        """)).scalar()

        

        

        return {
            "activeUsers": active_users,
            "admins": admins,
            "executives": executives,
            "hoCheckers": ho_checkers,
            "siteCheckers": site_checkers,
            "encoders": encoders
        }
    except Exception as e:
        logging.error(f"Error fetching KPI data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/companies", response_model=List[Dict])
def get_companies(db: Session = Depends(get_db)):
    """
    Get list of companies from ref.company_main
    """
    try:
        result = db.execute(text("""
            SELECT 
                company_id as id,
                company_name as name
            FROM ref.company_main
            ORDER BY company_id ASC
        """))
        
        companies = [
            {
                'id': row.id,
                'name': row.name
            }
            for row in result
        ]
        
        return companies
    except Exception as e:
        logging.error(f"Error fetching companies: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/expenditure-types", response_model=List[Dict])
def get_expenditure_types(db: Session = Depends(get_db)):
    """
    Get list of expenditure types from ref.expenditure_type
    """
    try:
        result = db.execute(text("""
            SELECT 
                type_id as id,
                type_description as name
            FROM ref.expenditure_type
            ORDER BY type_id ASC
        """))
        
        types = [
            {
                'id': row.id,
                'name': row.name
            }
            for row in result
        ]
        
        return types
    except Exception as e:
        logging.error(f"Error fetching expenditure types: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))



# ====================== powerplant based on company ====================== #
@router.get("/power_plants", response_model=List[dict])
def get_fact_energy(
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            SELECT power_plant_id as id, site_name as name
            FROM ref.ref_power_plants 
            WHERE (:company_ids IS NULL OR company_id = ANY(:company_ids));
        """)

        result = db.execute(sql, {"company_ids": company_ids})
        types = [
            {
                'id': row.id,
                'name': row.name
            }
            for row in result
        ]
        return types

    except Exception as e:
        logging.error(f"Error calling powerplant: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    

@router.get("/co2_equivalence", response_model=List[dict])
def get_equivalence(
    db: Session = Depends(get_db)
    ):
    try:
        sql = text("""
            select * from ref.ref_co2_equivalence;
        """)

        result = db.execute(sql)
        data = [dict(row._mapping) for row in result]
        return data

    except Exception as e:
        logging.error(f"Error calling co2_equivalence: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/prov", response_model=List[dict])
def get_equivalence(
    db: Session = Depends(get_db)
    ):
    try:
        sql = text("""
            SELECT DISTINCT 
            province AS id, 
            province AS name 
            FROM ref.ref_power_plants;

        """)

        result = db.execute(sql)
        data = [dict(row._mapping) for row in result]
        return data

    except Exception as e:
        logging.error(f"Error calling co2_equivalence: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/source", response_model=List[dict])
def get_sources(
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            select 
                generation_source as id, 
                INITCAP(generation_source) AS name 
            from ref.ref_emission_factors;
        """)

        result = db.execute(sql, {"company_ids": company_ids})
        data = [dict(row._mapping) for row in result]
        return data

    except Exception as e:
        logging.error(f"Error calling powerplant: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/pp_company", response_model=List[dict])
def get_sources(
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            select distinct
                pp.company_id as id, 
                cm.company_name as name 
            from ref.ref_power_plants pp 
            left join ref.company_main cm on cm.company_id=pp.company_id;
        """)

        result = db.execute(sql, {"company_ids": company_ids})
        data = [dict(row._mapping) for row in result]
        return data

    except Exception as e:
        logging.error(f"Error calling powerplant: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    

@router.get("/pp_info", response_model=List[dict])
def get_sources(
    company_ids: Optional[List[str]] = Query(None, alias="p_company_id"),
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            select 
                pp.power_plant_id,
                pp.site_name, 
                cm.company_id,
                cm.company_name, 
                cm.color,
                pp.province,
                ef.generation_source
            from ref.ref_power_plants pp 
            left join ref.company_main cm on cm.company_id = pp.company_id 
            left join ref.ref_emission_factors ef on pp.ef_id = ef.ef_id;
        """)

        result = db.execute(sql, {"company_ids": company_ids})
        data = [dict(row._mapping) for row in result]
        return data

    except Exception as e:
        logging.error(f"Error calling powerplant: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/audit-trail", response_model=List[dict])
def get_audit_trail(db: Session = Depends(get_db)):
    """
    Get all records from audit_trail table
    """
    try:
        sql = text("""select
                audit_id,
                account.email,
                target_table,
                record_id,
                action_type,
                old_value,
                new_value,
                audit_timestamp,
                description
                from audit_trail
                left join account on account.account_id = audit_trail.account_id""")
        result = db.execute(sql)
        data = [dict(row._mapping) for row in result]
        return data
    except Exception as e:
        logging.error(f"Error fetching audit trail: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")