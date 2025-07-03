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
from ..auth_decorators import get_user_info
from ..services.audit_trail import append_audit_trail, append_bulk_audit_trail
from ..services.auth import User

router = APIRouter()

# ====================== update status ====================== #
# single update status
@router.post("/update_status")
async def update_status(
    request: Request,
    db: Session = Depends(get_db),
    user_info: User = Depends(get_user_info)
):
    data = await request.json()

    record_id = data.get("record_id")
    new_status = data.get("new_status")
    remarks = data.get("remarks")

    if not record_id or not new_status:
        raise HTTPException(status_code=400, detail="record_id and new_status are required.")

    try:
        # Get the old status before updating
        old_record = db.query(RecordStatus).filter(RecordStatus.record_id == record_id).first()
        old_status = old_record.status_id if old_record else ""
        old_remarks = old_record.remarks if old_record else ""

        # Prepare the fields to update
        update_data = {
            "status_id": new_status,
            "status_timestamp": datetime.now(),
            "remarks": remarks if remarks else None
        }

        update_stmt = (
            update(RecordStatus)
            .where(RecordStatus.record_id == record_id)
            .values(**update_data)
        )

        result = db.execute(update_stmt)
        db.commit()

        # Create audit trail with old and new values
        old_value = f"status: {old_status}, remarks: {old_remarks}"
        new_value = f"status: {new_status}, remarks: {remarks or 'None'}"
        
        append_audit_trail(
            db=db,
            account_id=str(user_info.account_id),
            target_table="record_status",
            record_id=record_id,
            action_type="update",
            old_value=old_value,
            new_value=new_value,
            description="Updated record status"
        )
        
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
        # Get old values from the first record_id
        first_record_id = record_ids[0]
        old_record = db.query(RecordStatus).filter(RecordStatus.record_id == first_record_id).first()
        old_status = old_record.status_id if old_record else ""
        old_remarks = old_record.remarks if old_record else ""

        # Prepare the fields to update
        update_data = {
            "status_id": new_status,
            "status_timestamp": datetime.now(),
            "remarks": remarks if remarks else None
        }

        # Perform bulk update
        update_stmt = (
            update(RecordStatus)
            .where(RecordStatus.record_id.in_(record_ids))
            .values(**update_data)
        )

        result = db.execute(update_stmt)
        db.commit()

        # Get the actual number of rows affected
        rows_affected = result.rowcount

        # Prepare bulk audit entries
        audit_entries = []
        for record_id in record_ids:
            # Use the same old values for all records
            old_value = f"status: {old_status}, remarks: {old_remarks}"
            new_value = f"status: {new_status}, remarks: {remarks or 'None'}"

            audit_entries.append({
                "account_id": str(user_info.account_id),
                "target_table": "record_status",
                "record_id": record_id,
                "action_type": "update",
                "old_value": old_value,
                "new_value": new_value,
                "description": f"Bulk updated record status to {new_status}"
            })

        # Bulk insert audit trail
        append_bulk_audit_trail(db, audit_entries)

        return {"message": f"Updated {rows_affected} record(s) successfully."}

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
