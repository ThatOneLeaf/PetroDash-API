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
from ..services.audit_trail import append_audit_trail
from ..services.auth import User
from ..auth_decorators import get_user_info



router = APIRouter()

# ====================== update status ====================== #
# singe update status
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

# ====================== bulk update status ====================== #

@router.post("/bulk_update_status")
async def bulk_update_status(
    request: Request,
    db: Session = Depends(get_db),
    user_info: User = Depends(get_user_info)
):
    data = await request.json()

    record_ids = data.get("record_ids")  # expecting a list
    new_status = data.get("new_status")
    remarks = data.get("remarks")  # optional

    if not record_ids or not isinstance(record_ids, list) or not new_status:
        raise HTTPException(status_code=400, detail="record_ids (list) and new_status are required.")

    try:
        # Step 1: Fetch old statuses for audit trail
        existing_records = db.execute(
            select(RecordStatus).where(RecordStatus.record_id.in_(record_ids))
        ).scalars().all()

        # Prepare old values by record_id
        old_values_map = {r.record_id: r.status_id for r in existing_records}

        # Step 2: Perform bulk update
        update_stmt = (
            update(RecordStatus)
            .where(RecordStatus.record_id.in_(record_ids))
            .values(
                status_id=new_status,
                status_timestamp=datetime.now(),
                remarks=remarks if remarks else None
            )
        )
        db.execute(update_stmt)

        # Step 3: Append audit trail for each record
        for record in existing_records:
            append_audit_trail(
                db=db,
                account_id=str(user_info.account_id),
                target_table="record_status",
                record_id=record.record_id,
                action_type="update",
                old_value=str(old_values_map.get(record.record_id)),
                new_value=str(new_status),
                description=f"Status updated to '{new_status}' via bulk update."
            )

        db.commit()
        return {"message": f"Updated {len(record_ids)} record(s) successfully."}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
