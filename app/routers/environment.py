from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict
from decimal import Decimal
import logging
import traceback

from ..dependencies import get_db

router = APIRouter()

@router.get("/water_abstraction", response_model=List[Dict])
def get_water_abstraction(db: Session = Depends(get_db)):
    """
    Get water abstraction data from vw_environment_water_abstraction view
    Joins with status and company info, with 'HEAD APPROVED' as default status if null
    """
    try:
        logging.info("Executing water abstraction query")
        
        result = db.execute(text("""
            SELECT
                ewa.wa_id,
                cm.company_name,
                ewa.volume,
                ewa.unit AS metrics,
                ewa.quarter,
                ewa.year,
                COALESCE(s.status_name, 'HEAD APPROVED') AS status_name
            FROM gold.vw_environment_water_abstraction ewa
            LEFT JOIN public.checker_status_log csl ON ewa.wa_id = csl.record_id
            LEFT JOIN public.status s ON csl.status_id = s.status_id
            LEFT JOIN ref.company_main cm ON ewa.company_id = cm.company_id
            ORDER BY ewa.year DESC
        """))

        data = [
            {
                "waId": row.wa_id,
                "companyName": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "metrics": row.metrics,
                "quarter": row.quarter,
                "year": row.year,
                "statusName": row.status_name
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")
        
        if not data:
            logging.warning("No water abstraction data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching water abstraction data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/water-discharge", response_model=List[Dict])
def get_water_discharge(db: Session = Depends(get_db)):
    """
    Get water discharge data from vw_environment_water_discharge view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing water discharge query")
        
        result = db.execute(text("""
            SELECT
                ewd.wd_id,
                cm.company_name,
                ewd.volume,
                ewd.unit AS metrics,
                ewd.quarter,
                ewd.year,
                COALESCE(s.status_name, 'Head Approved') AS status_name
            FROM gold.vw_environment_water_discharge ewd
            LEFT JOIN public.checker_status_log csl ON ewd.wd_id = csl.record_id
            LEFT JOIN public.status s ON csl.status_id = s.status_id
            LEFT JOIN ref.company_main cm ON ewd.company_id = cm.company_id
            ORDER BY ewd.year DESC
        """))

        data = [
            {
                "wdId": row.wd_id,
                "companyName": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "metrics": row.metrics,
                "quarter": row.quarter,
                "year": row.year,
                "statusName": row.status_name
            }
            for row in result
        ]
        
        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")

        if not data:
            logging.warning("No water discharge data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching water discharge data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/water-consumption", response_model=List[Dict])
def get_water_consumption(db: Session = Depends(get_db)):
    """
    Get water consumption data from vw_environment_water_consumption view
    Joins with status and company info, with 'Head Approved' as default status if null
    """
    try:
        logging.info("Executing water consumption query")
        
        result = db.execute(text("""
            SELECT
                ewc.wc_id,
                cm.company_name,
                ewc.volume,
                ewc.unit AS metrics,
                ewc.quarter,
                ewc.year,
                COALESCE(s.status_name, 'Head Approved') AS status_name
            FROM gold.vw_environment_water_consumption ewc
            LEFT JOIN public.checker_status_log csl ON ewc.wc_id = csl.record_id
            LEFT JOIN public.status s ON csl.status_id = s.status_id
            LEFT JOIN ref.company_main cm ON ewc.company_id = cm.company_id
            ORDER BY ewc.year DESC
        """))

        data = [
            {
                "wcId": row.wc_id,
                "companyName": row.company_name,
                "volume": float(row.volume) if row.volume is not None else 0,
                "metrics": row.metrics,
                "quarter": row.quarter,
                "year": row.year,
                "statusName": row.status_name
            }
            for row in result
        ]

        logging.info(f"Query returned {len(data)} rows")
        logging.debug(f"Data: {data}")

        if not data:
            logging.warning("No water consumption data found")
            return []

        return data
    except Exception as e:
        logging.error(f"Error fetching water consumption data: {str(e)}")
        logging.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
