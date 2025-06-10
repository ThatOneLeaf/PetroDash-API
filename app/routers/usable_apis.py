from fastapi import APIRouter, Depends, Query, HTTPException, Form, UploadFile, File, Request
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy import text, update, select
from app.bronze.crud import EnergyRecords
from app.bronze.schemas import EnergyRecordOut, AddEnergyRecord
from app.public.models import RecordStatus
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
    data = await request.json()

    record_id = data.get("record_id")
    new_status = data.get("new_status")
    remarks = data.get("remarks")

    if not record_id or not new_status:
        raise HTTPException(status_code=400, detail="record_id and new_status are required.")

    # Prepare the fields to update
    update_data = {
        "record_id": record_id,
        "status_id": new_status,
        "status_timestamp": datetime.now(),
        "remarks": remarks if remarks else None  # Set to None if empty or not provided
    }

    update_stmt = (
        update(RecordStatus)
        .where(RecordStatus.record_id == record_id)
        .values(**update_data)
    )

    try:
        db.execute(update_stmt)
        db.commit()
        return {"message": "Status updated successfully."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")