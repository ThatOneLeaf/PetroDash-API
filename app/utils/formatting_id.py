import datetime
from sqlalchemy.orm import Session
from sqlalchemy import desc

def generate_pkey_id(db: Session, indicator: str, model_class, id_field: str):
    """
    Generate a new ID based on the current date and last entry.
    """
    current_date_str = datetime.datetime.now().strftime('%Y%m%d')
    sequence_length = 3

    id_column = getattr(model_class, id_field)

    last_entry = (
        db.query(model_class)
        .filter(id_column.like(f'{indicator}-{current_date_str}-%'))
        .order_by(desc(id_column))
        .first()
    )

    if last_entry:
        last_sequence = int(getattr(last_entry, id_field).split('-')[-1])
        next_sequence = f"{last_sequence + 1:0{sequence_length}d}"
    else:
        next_sequence = f"{1:0{sequence_length}d}"

    generated_id = f"{indicator}-{current_date_str}-{next_sequence}"
    return generated_id