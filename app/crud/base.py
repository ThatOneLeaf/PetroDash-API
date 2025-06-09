from typing import Type, List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import DeclarativeMeta


def get_one(db: Session, model: Type[DeclarativeMeta], id_field: str, id_value: Any):
    return db.query(model).filter(getattr(model, id_field) == id_value).first()

def get_one_filtered(db: Session, model: Type[DeclarativeMeta], filters: dict):
    query = db.query(model)
    for field, value in filters.items():
        query = query.filter(getattr(model, field) == value)
    return query.first()


def get_many(db: Session, model: Type[DeclarativeMeta], skip: int = 0, limit: int = 100):
    return db.query(model).offset(skip).limit(limit).all()


def get_many_filtered(
    db: Session,
    model: Type[DeclarativeMeta],
    filters: Optional[Dict[str, Any]] = None,
    skip: int = 0,
    limit: int = 100
):
    query = db.query(model)
    if filters:
        for field, value in filters.items():
            query = query.filter(getattr(model, field) == value)
    return query.offset(skip).limit(limit).all()


def get_all(db: Session, model: Type[DeclarativeMeta]):
    return db.query(model).all()
