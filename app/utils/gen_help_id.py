from sqlalchemy.orm import Session
from sqlalchemy import desc

# for single insert use only
def generate_pkey_id(
    db: Session,
    company_id: str,
    year: int,
    model_class,
    id_field: str
):
    """
    Generate a new primary key ID in the format:
    <company_id>_<year>_XXX where XXX is a zero-padded sequence.
    """
    sequence_length = 3
    id_column = getattr(model_class, id_field)

    pattern = f"{company_id}-{year}-%"
    last_entry = (
        db.query(model_class)
        .filter(id_column.like(pattern))
        .order_by(desc(id_column))
        .first()
    )

    if last_entry:
        last_id = getattr(last_entry, id_field)
        last_sequence_str = last_id.split('-')[-1]  # changed from `_` to `-`
        last_sequence = int(last_sequence_str)
        next_sequence = f"{last_sequence + 1:0{sequence_length}d}"
    else:
        next_sequence = f"{1:0{sequence_length}d}"

    generated_id = f"{company_id}-{year}-{next_sequence}"

    return generated_id

# for bulk insert use
def generate_bulk_id(
    db: Session,
    company_id: str,
    year: int,
    model_class,
    id_field: str,
    count: int
):
    """
    Generate multiple primary key IDs for bulk operations.
    Returns a list of IDs with continuous sequence numbers.
    """
    sequence_length = 3
    id_column = getattr(model_class, id_field)
    pattern = f"{company_id}-{year}-%"
    
    last_entry = (
        db.query(model_class)
        .filter(id_column.like(pattern))
        .order_by(desc(id_column))
        .first()
    )
    
    if last_entry:
        last_id = getattr(last_entry, id_field)
        last_sequence_str = last_id.split('-')[-1]
        start_sequence = int(last_sequence_str) + 1
    else:
        start_sequence = 1
    
    # Generate list of IDs
    ids = []
    for i in range(count):
        sequence_num = start_sequence + i
        generated_id = f"{company_id}-{year}-{sequence_num:0{sequence_length}d}"
        ids.append(generated_id)
    
    return ids