from datetime import datetime
from sqlalchemy.orm import Session
from app.public.models import AuditTrail
from datetime import datetime

def format_audit_id(sequence: int = 1) -> str:
    """
    Format audit_id into AUYYYYMMDDHHMMSSmm## format:
    - AU: prefix
    - YYYYMMDDHHMMSS: UTC timestamp
    - mm: 2-digit milliseconds (truncated)
    - ##: sequence number (2 digits)
    
    Total: 20 characters
    """
    now = datetime.utcnow()
    timestamp = now.strftime('%Y%m%d%H%M%S')
    milliseconds = int(now.microsecond / 10000)  # from microseconds to 2-digit ms
    formatted_id = f"AU{timestamp}{milliseconds:02d}{sequence:02d}"
    return formatted_id


def append_audit_trail(
    db:  Session,
    account_id: str,
    target_table: str,
    record_id: str,
    action_type: str,
    old_value: str,
    new_value: str,
    description: str
):
    audit_id = format_audit_id()  # Generate a formatted audit ID
    audit = AuditTrail(
        audit_id=audit_id,
        account_id=account_id,
        target_table=target_table,
        record_id=record_id,
        action_type=action_type,
        old_value=old_value,
        new_value=new_value,
        audit_timestamp=datetime.utcnow(),
        description=description
    )
    db.add(audit)
    db.commit()
    db.refresh(audit)
    return audit