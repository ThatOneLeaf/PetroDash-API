from decimal import Decimal
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query, Request
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Union
import logging
import traceback
from fastapi.responses import StreamingResponse
import io
from io import BytesIO
from datetime import datetime
from typing import Optional, List
from app.dependencies import get_db

router = APIRouter()
    
# ================================================================== APIs FOR DASHBOARD =========================================================
# WATER DASHBOARD
# key metrics
@router.get("/abstraction", response_model=Dict)
def get_water_abstraction(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get total water abstraction volume by year
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_abstraction_by_year(
                :company_ids, :quarters, :years
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        data = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        total_volume = sum(row['total_volume'] for row in data)
        unit = data[0]['unit'] if data else 'cubic meters'

        return {
            'total_volume': round(total_volume, 2),
            'unit': unit
        }

    except Exception as e:
        print("Error in water abstraction:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/discharge", response_model=Dict)
def get_water_discharge(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get total water discharge volume by year
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_discharge_by_year(
                :company_ids, :quarters, :years
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        data = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        total_volume = sum(row['total_volume'] for row in data)
        unit = data[0]['unit'] if data else 'cubic meters'

        return {
            'total_volume': round(total_volume, 2),
            'unit': unit
        }

    except Exception as e:
        print("Error in water discharge:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/consumption", response_model=Dict)
def get_water_consumption(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get total water consumption volume by year
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_consumption_by_year(
                :company_ids, :quarters, :years
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        data = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        total_volume = sum(row['total_volume'] for row in data)
        unit = data[0]['unit'] if data else 'cubic meters'

        return {
            'total_volume': round(total_volume, 2),
            'unit': unit
        }

    except Exception as e:
        print("Error in water consumption:", str(e))
        raise HTTPException(status_code=500, detail=str(e))

# pie chart
@router.get("/pie-chart", response_model=Dict)
def get_water_summary_pie(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get summarized water volume for pie chart: Abstracted, Discharged, Consumed
    """
    try:
        # Debug logging
        print(f"Received parameters - company_id: {company_id}, quarter: {quarter}, year: {year}")
        
        # Handle parameter conversion
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None
        
        print(f"Processed parameters - company_ids: {company_ids}, quarters: {quarters}, years: {years}")

        # Validate required parameters
        if not company_ids or not quarters or not years:
            return {
                "data": [],
                "unit": "cubic meters",
                "message": "Missing required parameters"
            }

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_summary(
                ARRAY[:company_ids]::text[], 
                ARRAY[:quarters]::text[], 
                ARRAY[:years]::smallint[]
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        rows = result.fetchall()
        print(f"Database returned {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "cubic meters",
                "message": "No data found for the specified parameters"
            }

        # Aggregate totals
        total_abstracted = sum(row.total_abstracted_volume or 0 for row in rows)
        total_discharged = sum(row.total_discharged_volume or 0 for row in rows)
        total_consumed = sum(row.total_consumption_volume or 0 for row in rows)

        print(f"Totals - Abstracted: {total_abstracted}, Discharged: {total_discharged}, Consumed: {total_consumed}")

        # Calculate total and percentages
        total = total_abstracted + total_discharged + total_consumed
        
        if total == 0:
            return {
                "data": [],
                "unit": "cubic meters",
                "message": "All values are zero"
            }

        percentages = [
            total_abstracted / total * 100,
            total_discharged / total * 100,
            total_consumed / total * 100,
        ]

        data = [
            {
                "label": "Abstracted",
                "value": round(total_abstracted, 2),
                "percentage": round(percentages[0], 2),
                "color": "#3B82F6"
            },
            {
                "label": "Discharged",
                "value": round(total_discharged, 2),
                "percentage": round(percentages[1], 2),
                "color": "#F97316"
            },
            {
                "label": "Consumed",
                "value": round(total_consumed, 2),
                "percentage": round(percentages[2], 2),
                "color": "#10B981"
            },
        ]

        # Filter out zero values
        non_zero_data = [item for item in data if item["value"] > 0]

        return {
            "data": non_zero_data,
            "unit": "cubic meters",
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in pie chart water summary:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# line chart
@router.get("/line-chart", response_model=Dict)
def get_water_summary_line_chart(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get yearly water volume trends for line chart: Abstracted, Discharged, Consumed
    """
    try:
        print(f"Received parameters - company_id: {company_id}, quarter: {quarter}, year: {year}")
        
        # Normalize parameters
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None
        
        print(f"Processed parameters - company_ids: {company_ids}, quarters: {quarters}, years: {years}")

        if not company_ids or not quarters or not years:
            return {
                "data": [],
                "unit": "cubic meters",
                "message": "Missing required parameters"
            }

        # Call the PostgreSQL function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_summary(
                ARRAY[:company_ids]::text[], 
                ARRAY[:quarters]::text[], 
                ARRAY[:years]::smallint[]
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        rows = result.fetchall()
        print(f"Database returned {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "cubic meters",
                "message": "No data found for the specified parameters"
            }

        # Convert query result to DataFrame
        import pandas as pd
        df = pd.DataFrame([{
            "company_id": row.company_id,
            "year": str(row.year),
            "quarter": row.quarter,
            "total_abstracted_volume": float(row.total_abstracted_volume or 0),
            "total_discharged_volume": float(row.total_discharged_volume or 0),
            "total_consumption_volume": float(row.total_consumption_volume or 0)
        } for row in rows])

        # Group by year
        yearly_df = df.groupby("year", as_index=False).sum(numeric_only=True)

        # Prepare data format with labels and colors
        line_chart_data = [
            {
                "label": "Abstracted",
                "data": yearly_df["total_abstracted_volume"].round(2).tolist(),
                "color": "#3B82F6"
            },
            {
                "label": "Discharged",
                "data": yearly_df["total_discharged_volume"].round(2).tolist(),
                "color": "#F97316"
            },
            {
                "label": "Consumed",
                "data": yearly_df["total_consumption_volume"].round(2).tolist(),
                "color": "#10B981"
            }
        ]

        return {
            "data": line_chart_data,
            "labels": yearly_df["year"].tolist(),
            "unit": "cubic meters",
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in line chart water summary:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))