from datetime import datetime
from sqlalchemy.orm import Session
from app.public.models import AuditTrail
from datetime import datetime
from typing import List, Dict, Any

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
    db: Session,
    account_id: str,
    target_table: str,
    record_id: str,
    action_type: str,
    old_value: str,
    new_value: str,
    description: str
):
    """Single audit trail entry - commits immediately"""
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


def append_bulk_audit_trail(
    db: Session,
    audit_entries: List[Dict[str, Any]]
):
    """
    Bulk insert audit trail entries - commits once at the end
    
    Args:
        db: Database session
        audit_entries: List of dictionaries containing audit trail data
                      Each dict should have: account_id, target_table, record_id, 
                      action_type, old_value, new_value, description
    """
    if not audit_entries:
        return []
    
    audit_objects = []
    base_timestamp = datetime.utcnow()
    
    for i, entry in enumerate(audit_entries):
        # Generate unique audit_id for each entry
        audit_id = format_audit_id(sequence=i + 1)
        
        # Create audit object
        audit = AuditTrail(
            audit_id=audit_id,
            account_id=entry["account_id"],
            target_table=entry["target_table"],
            record_id=entry["record_id"],
            action_type=entry["action_type"],
            old_value=entry["old_value"],
            new_value=entry["new_value"],
            audit_timestamp=base_timestamp,
            description=entry["description"]
        )
        audit_objects.append(audit)
    
    # Bulk insert all audit records
    db.bulk_save_objects(audit_objects)
    db.commit()
    
    return audit_objects


def prepare_bulk_audit_entries(
    records: List[Any],
    account_id: str,
    get_record_data_func: callable
) -> List[Dict[str, Any]]:
    """
    Helper function to prepare bulk audit entries from records
    
    Args:
        records: List of database record objects
        account_id: User account ID
        get_record_data_func: Function that takes a record and returns audit data dict
    
    Returns:
        List of audit entry dictionaries ready for bulk insertion
    """
    audit_entries = []
    
    for record in records:
        # Get record-specific audit data
        record_data = get_record_data_func(record)
        
        # Add common audit data
        record_data["account_id"] = account_id
        audit_entries.append(record_data)
    
    return audit_entries