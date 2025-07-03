from fastapi.responses import StreamingResponse
# Bulk add accounts from file (CSV)
import csv
from io import StringIO, BytesIO

from fastapi import APIRouter, HTTPException, Depends, status, UploadFile, File, Form, Response
from sqlalchemy.orm import Session
from typing import List, Optional
from ..public import models
from app.dependencies import get_db
from ..public.schemas import AccountCreate, AccountProfileOut
import uuid
# Bulk add accounts from file (CSV)
import csv
from io import StringIO
from datetime import datetime  
import ulid 
from passlib.hash import bcrypt


router = APIRouter()



# Helper to generate ULID (or UUID)
def generate_ulid():
    return str(ulid.new())

# Create single account with profile
@router.post("/add", response_model=AccountProfileOut, status_code=status.HTTP_201_CREATED)
def create_account(account: AccountCreate, db: Session = Depends(get_db)):
    try:
        account_id = generate_ulid()
        db_account = models.Account(
            account_id=account_id,
            email=account.email,
            password=bcrypt.hash("changeme"),  # Default password, should be changed later
            account_role=account.account_role,
            power_plant_id=account.power_plant_id,
            company_id=account.company_id,
            account_status=account.account_status,
            date_created=datetime.now(),
            date_updated=datetime.now()

        )
        db_profile = models.UserProfile(
            account_id=account_id,
            emp_id=account.profile.emp_id,
            first_name=account.profile.first_name,
            last_name=account.profile.last_name,
            middle_name=account.profile.middle_name,
            suffix=account.profile.suffix,
            contact_number=account.profile.contact_number,
            address=account.profile.address,
            birthdate=account.profile.birthdate,
            gender=account.profile.gender,
            profile_created=datetime.now(),
            profile_updated=datetime.now()
        )
        db.add(db_account)
        db.add(db_profile)
        db.commit()
        db.refresh(db_account)
        db.refresh(db_profile)
        return {
            "account_id": db_account.account_id,
            "email": db_account.email,
            "account_role": db_account.account_role,
            "power_plant_id": db_account.power_plant_id,
            "company_id": db_account.company_id,
            "account_status": db_account.account_status,
            "first_name": db_profile.first_name,
            "last_name": db_profile.last_name,
            "middle_name": db_profile.middle_name,
            "suffix": db_profile.suffix,
            "contact_number": db_profile.contact_number,
            "address": db_profile.address,
            "birthdate": db_profile.birthdate,
            "gender": db_profile.gender
        }
    except Exception as e:
        print(f"Error creating account: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to create account: {e}")

# Get all accounts with profiles
@router.get("/", response_model=List[AccountProfileOut])
def get_all_accounts(db: Session = Depends(get_db)):
    accounts = db.query(models.Account).order_by(
        models.Account.date_updated.desc(),
        models.Account.date_created.desc()
    ).all()
    profiles = db.query(models.UserProfile).all()
    profile_map = {p.account_id: p for p in profiles}
    result = []
    for acc in accounts:
        prof = profile_map.get(acc.account_id)
        if prof:
            result.append({
                "account_id": acc.account_id,
                "email": acc.email,
                "account_role": acc.account_role,
                "power_plant_id": acc.power_plant_id,
                "company_id": acc.company_id,
                "account_status": acc.account_status,
                "first_name": prof.first_name,
                "last_name": prof.last_name,
                "middle_name": prof.middle_name,
                "suffix": prof.suffix,
                "contact_number": prof.contact_number,
                "address": prof.address,
                "birthdate": prof.birthdate.strftime("%Y-%m-%d") if prof.birthdate else "",
                "gender": prof.gender
            })
    return result

# Activate account
@router.patch("/{account_id}/activate", response_model=AccountProfileOut)
def activate_account(account_id: str, db: Session = Depends(get_db)):
    acc = db.query(models.Account).filter(models.Account.account_id == account_id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found")
    acc.account_status = "active"
    db.commit()
    prof = db.query(models.UserProfile).filter(models.UserProfile.account_id == account_id).first()
    return {
        "account_id": acc.account_id,
        "email": acc.email,
        "account_role": acc.account_role,
        "power_plant_id": acc.power_plant_id,
        "company_id": acc.company_id,
        "account_status": acc.account_status,
        "first_name": prof.first_name,
        "last_name": prof.last_name,
        "middle_name": prof.middle_name,
        "suffix": prof.suffix,
        "contact_number": prof.contact_number,
        "address": prof.address,
        "birthdate": prof.birthdate,
        "gender": prof.gender
    }

# Deactivate account
@router.patch("/{account_id}/deactivate", response_model=AccountProfileOut)
def deactivate_account(account_id: str, db: Session = Depends(get_db)):
    acc = db.query(models.Account).filter(models.Account.account_id == account_id).first()
    if not acc:
        raise HTTPException(status_code=404, detail="Account not found")
    acc.account_status = "deactivated"
    db.commit()
    prof = db.query(models.UserProfile).filter(models.UserProfile.account_id == account_id).first()
    return {
        "account_id": acc.account_id,
        "email": acc.email,
        "account_role": acc.account_role,
        "power_plant_id": acc.power_plant_id,
        "company_id": acc.company_id,
        "account_status": acc.account_status,
        "first_name": prof.first_name,
        "last_name": prof.last_name,
        "middle_name": prof.middle_name,
        "suffix": prof.suffix,
        "contact_number": prof.contact_number,
        "address": prof.address,
        "birthdate": prof.birthdate,
        "gender": prof.gender
    }

# API to preview CSV data and validate all rows (no DB insert)
@router.post("/bulk/preview", status_code=200)
async def preview_bulk_accounts(
    file: UploadFile = File(...),
    power_plant_id: Optional[str] = Form(None),
    company_id: Optional[str] = Form(None),
    account_role: str = Form(...),
    db: Session = Depends(get_db)
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded CSV file is empty.")
    s = StringIO(content.decode('utf-8'))
    reader = csv.DictReader(s)
    required_headers = {"email", "first_name", "last_name"}
    if not required_headers.issubset(set(reader.fieldnames or [])):
        raise HTTPException(status_code=400, detail=f"Missing required headers: {required_headers - set(reader.fieldnames or [])}")

    all_rows = list(reader)
    template_row_indices = []
    errors = []
    seen_emails = set()
    valid_rows = []

    # Identify template/sample rows
    for i, row in enumerate(all_rows, start=2):
        email = row.get('email', '').strip().lower()
        birthdate_raw = row.get('birthdate', '').strip()
        if (
            email == "user@example.com" or
            row.get('emp_id', '').strip() == "EMP001" or
            row.get('first_name', '').strip() == "John" or
            row.get('last_name', '').strip() == "Doe" or
            row.get('middle_name', '').strip() == "A." or
            row.get('suffix', '').strip() == "Jr." or
            row.get('contact_number', '').strip() == "09123456789" or
            row.get('address', '').strip() == "123 Main St" or
            birthdate_raw == "MM/DD/YYYY" or
            row.get('gender', '').strip() == "Male, Female, Other"
        ):
            template_row_indices.append(i)

    # If all rows are template/sample rows, treat as empty
    if len(template_row_indices) == len(all_rows):
        raise HTTPException(status_code=400, detail="Uploaded CSV file is empty.")

    # Validate all non-template rows
    for i, row in enumerate(all_rows, start=2):
        if i in template_row_indices:
            continue
        email = row.get('email', '').strip().lower()
        birthdate_raw = row.get('birthdate', '').strip()

        # Validate required fields
        if not email or not row.get('first_name') or not row.get('last_name'):
            errors.append(f"Row {i}: Missing required fields (email, first_name, last_name).")
            continue

        # Check for duplicate emails within the file
        if email in seen_emails:
            errors.append(f"Row {i}: Duplicate email '{email}' found in the uploaded file.")
            continue
        seen_emails.add(email)

        # Validate birthdate format if present
        birthdate = None
        if birthdate_raw:
            try:
                birthdate = datetime.strptime(birthdate_raw, "%m/%d/%Y").date()
            except ValueError:
                errors.append(f"Row {i}: Invalid birthdate format '{birthdate_raw}' for email '{email}'. Expected MM/DD/YYYY.")
                continue

        # Normalize and validate contact number
        contact_number = row.get('contact_number', '').strip()
        if contact_number and not contact_number.startswith('0'):
            contact_number = f'0{contact_number}'

        # Add row to valid_rows (include all fields)
        valid_rows.append({
            **row,
            "email": email,
            "birthdate": birthdate.strftime("%Y-%m-%d") if birthdate else row.get('birthdate', ''),
        })

    if errors:
        return {"valid": False, "errors": errors}
    return {"valid": True, "rows": valid_rows}

# Bulk create accounts from CSV file
@router.post("/bulk", response_model=List[AccountProfileOut], status_code=status.HTTP_201_CREATED)
async def bulk_create_accounts_from_file(
    file: UploadFile = File(...),
    power_plant_id: Optional[str] = Form(None),
    company_id: Optional[str] = Form(None),
    account_role: str = Form(...),
    db: Session = Depends(get_db)
):
    # 1. Check file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded CSV file is empty.")

    s = StringIO(content.decode('utf-8'))
    reader = csv.DictReader(s)

    required_headers = {"email", "first_name", "last_name"}
    if not required_headers.issubset(set(reader.fieldnames or [])):
        raise HTTPException(status_code=400, detail=f"Missing required headers: {required_headers - set(reader.fieldnames or [])}")

    created = []
    errors = []
    seen_emails = set()
    template_row_indices = []
    all_rows = list(reader)

    # Identify template/sample rows
    for i, row in enumerate(all_rows, start=2):
        email = row.get('email', '').strip().lower()
        birthdate_raw = row.get('birthdate', '').strip()
        if (
            email == "user@example.com" or
            row.get('emp_id', '').strip() == "EMP001" or
            row.get('first_name', '').strip() == "John" or
            row.get('last_name', '').strip() == "Doe" or
            row.get('middle_name', '').strip() == "A." or
            row.get('suffix', '').strip() == "Jr." or
            row.get('contact_number', '').strip() == "09123456789" or
            row.get('address', '').strip() == "123 Main St" or
            birthdate_raw == "MM/DD/YYYY" or
            row.get('gender', '').strip() == "Male, Female, Other"
        ):
            template_row_indices.append(i)

    # If all rows are template/sample rows, treat as empty
    if len(template_row_indices) == len(all_rows):
        raise HTTPException(status_code=400, detail="Uploaded CSV file is empty.")

    # Now process only non-template rows
    for i, row in enumerate(all_rows, start=2):
        if i in template_row_indices:
            continue
        email = row.get('email', '').strip().lower()
        birthdate_raw = row.get('birthdate', '').strip()

        # 3. Validate required fields
        if not email or not row.get('first_name') or not row.get('last_name'):
            errors.append(f"Row {i}: Missing required fields (email, first_name, last_name).")
            continue

        # 4. Check for duplicate emails within the file
        if email in seen_emails:
            errors.append(f"Row {i}: Duplicate email '{email}' found in the uploaded file.")
            continue
        seen_emails.add(email)

        # 5. Validate birthdate format if present
        birthdate = None
        if birthdate_raw:
            try:
                birthdate = datetime.strptime(birthdate_raw, "%m/%d/%Y").date()
            except ValueError:
                errors.append(f"Row {i}: Invalid birthdate format '{birthdate_raw}' for email '{email}'. Expected MM/DD/YYYY.")
                continue

        # Normalize and validate contact number
        contact_number = row.get('contact_number', '').strip()
        if contact_number and not contact_number.startswith('0'):
            contact_number = f'0{contact_number}'

        account_id = generate_ulid()

        # Create Account model
        db_account = models.Account(
            account_id=account_id,
            email=email,
            password=bcrypt.hash("changeme"),
            account_role=account_role,
            power_plant_id=power_plant_id,
            company_id=company_id,
            account_status='active',
            date_created=datetime.now(),
            date_updated=datetime.now()
        )
        db.add(db_account)

        # Create UserProfile model
        db_profile = models.UserProfile(
            account_id=account_id,
            emp_id=row.get('emp_id'),
            first_name=row['first_name'],
            last_name=row['last_name'],
            middle_name=row.get('middle_name'),
            suffix=row.get('suffix'),
            contact_number=contact_number,
            address=row.get('address'),
            birthdate=birthdate,
            gender=row.get('gender'),
            profile_created=datetime.now(),
            profile_updated=datetime.now()
        )
        db.add(db_profile)

        created.append({
            "account_id": account_id,
            "email": email,
            "password": db_account.password,
            "account_role": db_account.account_role,
            "power_plant_id": db_account.power_plant_id,
            "company_id": db_account.company_id,
            "account_status": "active",
            "first_name": db_profile.first_name,
            "last_name": db_profile.last_name,
            "middle_name": db_profile.middle_name,
            "suffix": db_profile.suffix,
            "contact_number": db_profile.contact_number,
            "address": db_profile.address,
            "birthdate": db_profile.birthdate,
            "gender": db_profile.gender
        })

    # 6. If there are validation errors, return them without committing to DB
    if errors:
        raise HTTPException(status_code=400, detail={"errors": errors})

    db.commit()
    return created



# Route to download CSV template for bulk account upload
@router.get("/bulk/template", response_class=StreamingResponse)
def download_bulk_template():
    header = [
        "email", "emp_id", "first_name", "last_name", "middle_name", "suffix",
        "contact_number", "address", "birthdate", "gender"
    ]
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=header)
    writer.writeheader()
    # Optional sample row
    writer.writerow({
        "email": "user@example.com",
        "emp_id": "EMP001",
        "first_name": "John",
        "last_name": "Doe",
        "middle_name": "A.",
        "suffix": "Jr.",
        "contact_number": "09123456789",
        "address": "123 Main St",
        "birthdate": "MM/DD/YYYY",
        "gender": "Male, Female, Other"
    })
    output.seek(0)
    return StreamingResponse(
        BytesIO(output.getvalue().encode()),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=account_bulk_template.csv"}
    )