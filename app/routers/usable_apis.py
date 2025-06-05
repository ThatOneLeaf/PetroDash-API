from fastapi import APIRouter, Depends, Query, HTTPException, Form, UploadFile, File, Request
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text, update, select
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut, AddEnergyRecord
from app.public.models import CheckerStatus
from app.dependencies import get_db
from app.crud.base import get_one, get_all, get_many, get_many_filtered
from datetime import datetime
import pandas as pd
import io
import logging
import traceback


router = APIRouter()

# ====================== update status ====================== #
@router.post("/update_status")
async def update_status(
    request: Request,
    db: Session = Depends(get_db),
):
    data = await request.json()  # kunin ang JSON body bilang dict

    cs_id = data.get("cs_id")
    new_status = data.get("new_status")

    if not cs_id or not new_status:
        raise HTTPException(status_code=400, detail="cs_id and new_status are required.")

    update_stmt = (
        update(CheckerStatus)
        .where(CheckerStatus.cs_id == cs_id)
        .values(
            cs_id=cs_id,
            status_id=new_status,
            status_timestamp=datetime.now()
        )
    )

    try:
        db.execute(update_stmt)
        db.commit()
        return {"message": "Status updated successfully."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")