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

# stacked-bar chart
@router.get("/stacked-bar", response_model=Dict)
def get_stacked_bar_summary(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get quarterly water volume totals (abstracted, discharged, consumed) with assigned colors for stacked bar chart.
    """
    try:
        print(f"Received parameters - company_id: {company_id}, quarter: {quarter}, year: {year}")
        
        # Convert inputs into lists if necessary
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

        # Query the database
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

        # Convert to DataFrame with explicit column names
        import pandas as pd
        df = pd.DataFrame([{
            "company_id": row.company_id,
            "year": str(row.year),
            "quarter": row.quarter,
            "total_abstracted_volume": float(row.total_abstracted_volume or 0),
            "total_discharged_volume": float(row.total_discharged_volume or 0),
            "total_consumption_volume": float(row.total_consumption_volume or 0)
        } for row in rows])

        # Ensure correct order of quarters
        quarter_order = ['Q1', 'Q2', 'Q3', 'Q4']
        df['quarter'] = pd.Categorical(df['quarter'].astype(str), categories=quarter_order, ordered=True)

        # Group by quarter and sum - ADD observed=False to suppress warning
        grouped = df.groupby('quarter', as_index=False, observed=False).sum(numeric_only=True).sort_values('quarter')
        
        print("Grouped DataFrame columns:", grouped.columns.tolist())
        print("Grouped DataFrame head:", grouped.head())

        # Define consistent colors
        color_map = {
            "abstracted": "#3B82F6",   # Blue
            "discharged": "#F97316",   # Orange
            "consumed": "#10B981"      # Green
        }

        # Format output for stacked bar chart - SAFE COLUMN ACCESS
        data = []
        for _, row in grouped.iterrows():
            # Safe column access with fallback
            abstracted_col = "total_abstracted_volume" if "total_abstracted_volume" in row else "abstracted_volume"
            discharged_col = "total_discharged_volume" if "total_discharged_volume" in row else "discharged_volume"
            consumed_col = "total_consumption_volume" if "total_consumption_volume" in row else "consumption_volume"
            
            data.append({
                "quarter": row["quarter"],
                "abstracted": {
                    "value": round(row.get(abstracted_col, 0), 2),
                    "color": color_map["abstracted"]
                },
                "discharged": {
                    "value": round(row.get(discharged_col, 0), 2),
                    "color": color_map["discharged"]
                },
                "consumed": {
                    "value": round(row.get(consumed_col, 0), 2),
                    "color": color_map["consumed"]
                }
            })

        print(f"Final data: {data}")

        return {
            "data": data,
            "unit": "cubic meters",
            "message": "Success"
        }

    except Exception as e:
        print("Error in stacked bar water summary:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# water years
@router.get("/water-years", response_model=Dict)
def get_distinct_years(db: Session = Depends(get_db)):
    """
    Get distinct list of years from environment water summary (all data).
    """
    try:
        result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.func_environment_water_summary(NULL, NULL, NULL)
            ORDER BY year ASC
        """))
        
        rows = result.fetchall()
        years = [row.year for row in rows]

        return {
            "data": years,
            "message": "Success",
            "count": len(years)
        }

    except Exception as e:
        print("Error fetching distinct years:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# ELECTRICITY DASHBOARD
# comsumption-source
@router.get("/comsumption-source", response_model=Dict)
def get_distinct_consumption_source(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("""
            SELECT DISTINCT consumption_source 
            FROM gold.func_environment_electric_consumption_by_source(NULL, NULL, NULL, NULL)
            ORDER BY consumption_source ASC
        """))
        
        rows = result.fetchall()
        source = [row.consumption_source for row in rows]

        return {
            "data": source,
            "message": "Success",
            "count": len(source)
        }

    except Exception as e:
        print("Error fetching distinct source:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# electricity-years
@router.get("/electricity-years", response_model=Dict)
def get_distinct_electricity_years(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.func_environment_electric_consumption_by_year(NULL, NULL, NULL)
            ORDER BY year ASC
        """))
        
        rows = result.fetchall()
        years = [row.year for row in rows]

        return {
            "data": years,
            "message": "Success",
            "count": len(years)
        }

    except Exception as e:
        print("Error fetching distinct years:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# key metrics
@router.get("/electricity-key-metrics", response_model=Dict)
def get_electricity_key_metrics(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    consumption_source: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get electricity key metrics (total, peak year, average)
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        consumption_sources = consumption_source if isinstance(consumption_source, list) else [consumption_source] if consumption_source else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_electric_consumption_by_year(
                CAST(:company_ids AS VARCHAR(10)[]),
                CAST(:consumption_sources AS VARCHAR(30)[]),
                CAST(:quarters AS VARCHAR(2)[]),
                CAST(:years AS SMALLINT[])
            )
        """), {
            'company_ids': company_ids,
            'consumption_sources': consumption_sources,
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

        if not data:
            return {
                'total_consumption': 0,
                'unit_of_measurement': None,
                'peak_year': None,
                'peak_consumption': 0,
                'average_consumption': 0
            }

        # Load data into pandas DataFrame for KPI calculation
        df = pd.DataFrame(data)

        # KPI 1: Total Electric Consumption
        total_consumption = df['total_consumption'].sum()

        # KPI 2: Year with Highest Electricity Consumption
        peak_year_data = df.loc[df['total_consumption'].idxmax()]

        # KPI 3: Average Annual Electricity Consumption
        avg_consumption = df['total_consumption'].mean()

        return {
            'total_consumption': round(total_consumption, 2),
            'unit_of_measurement': df['unit_of_measurement'].iloc[0],
            'peak_year': int(peak_year_data['year']),
            'peak_consumption': round(peak_year_data['total_consumption'], 2),
            'average_consumption': round(avg_consumption, 2)
        }

    except Exception as e:
        print("Error in electricity key metrics:", str(e))
        raise HTTPException

@router.get("/elec-pie-chart", response_model=Dict)
def get_electricity_consumption_pie_chart(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    consumption_source: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get summarized electric consumption for pie chart by company
    """
    try:
        # Debug logs
        print(f"Received parameters - company_id: {company_id}, source: {consumption_source}, quarter: {quarter}, year: {year}")

        # Convert inputs to arrays
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        sources = consumption_source if isinstance(consumption_source, list) else [consumption_source] if consumption_source else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        print(f"Processed parameters - company_ids: {company_ids}, sources: {sources}, quarters: {quarters}, years: {years}")

        # Required check
        if not company_ids or not quarters or not years:
            return {
                "data": [],
                "unit": "kWh",
                "message": "Missing required parameters"
            }

        # Execute function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_electric_consumption_by_perc_lvl(
                ARRAY[:company_ids]::text[], 
                ARRAY[:sources]::text[],
                ARRAY[:quarters]::text[], 
                ARRAY[:years]::smallint[]
            )
        """), {
            "company_ids": company_ids,
            "sources": sources,
            "quarters": quarters,
            "years": years
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "kWh",
                "message": "No data found"
            }

        # Data preparation
        data = []
        total = sum(row.total_consumption or 0 for row in rows)
        print(f"Total electric consumption: {total}")

        if total == 0:
            return {
                "data": [],
                "unit": rows[0].unit_of_measurement if rows else "kWh",
                "message": "All values are zero"
            }

        color_palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]  # Extend if needed

        for idx, row in enumerate(rows):
            value = row.total_consumption or 0
            percentage = (value / total) * 100 if total > 0 else 0
            data.append({
                "label": row.company_id,
                "value": round(value, 2),
                "percentage": round(percentage, 2),
                "color": color_palette[idx % len(color_palette)]
            })

        return {
            "data": [item for item in data if item["value"] > 0],
            "unit": rows[0].unit_of_measurement if rows else "kWh",
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in electric consumption pie chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/elect-line-chart", response_model=Dict)
def get_electricity_consumption_line_chart(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    consumption_source: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get electric consumption data for line chart by company across years
    """
    try:
        print(f"Received parameters - company_id: {company_id}, source: {consumption_source}, quarter: {quarter}, year: {year}")

        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        sources = consumption_source if isinstance(consumption_source, list) else [consumption_source] if consumption_source else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        print(f"Processed parameters - company_ids: {company_ids}, sources: {sources}, quarters: {quarters}, years: {years}")

        if not company_ids or not years:
            return {
                "data": [],
                "unit": "kWh",
                "message": "Missing required parameters"
            }

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_electric_consumption_by_year(
                ARRAY[:company_ids]::text[], 
                ARRAY[:sources]::text[],
                ARRAY[:quarters]::text[], 
                ARRAY[:years]::smallint[]
            )
        """), {
            "company_ids": company_ids,
            "sources": sources,
            "quarters": quarters,
            "years": years
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "kWh",
                "message": "No data found"
            }

        from collections import defaultdict

        company_data = defaultdict(list)
        unit = rows[0].unit_of_measurement if rows else "kWh"

        for row in rows:
            company_data[row.company_id].append({
                "year": int(row.year),
                "total_consumption": float(row.total_consumption)
            })

        for company in company_data:
            company_data[company] = sorted(company_data[company], key=lambda x: x["year"])

        color_palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]  # Extend if needed

        # Map colors to company IDs (consistent ordering)
        sorted_company_ids = sorted(company_data.keys())
        color_map = {
            company: color_palette[idx % len(color_palette)]
            for idx, company in enumerate(sorted_company_ids)
        }

        return {
            "data": company_data,
            "colors": color_map,
            "unit": unit,
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in electric consumption line chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/elect-perc-bar-chart", response_model=Dict)
def get_electricity_consumption_bar_chart(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    consumption_source: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get summarized electricity consumption for bar chart by company
    """
    try:
        # Debug logs
        print(f"Received parameters - company_id: {company_id}, source: {consumption_source}, quarter: {quarter}, year: {year}")

        # Convert inputs to lists
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        sources = consumption_source if isinstance(consumption_source, list) else [consumption_source] if consumption_source else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        print(f"Processed parameters - company_ids: {company_ids}, sources: {sources}, quarters: {quarters}, years: {years}")

        if not company_ids or not quarters or not years:
            return {
                "data": [],
                "unit": "kWh",
                "message": "Missing required parameters"
            }

        # Execute function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_electric_consumption_by_perc_lvl(
                ARRAY[:company_ids]::text[],
                ARRAY[:sources]::text[],
                ARRAY[:quarters]::text[],
                ARRAY[:years]::smallint[]
            )
        """), {
            "company_ids": company_ids,
            "sources": sources,
            "quarters": quarters,
            "years": years
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "kWh",
                "message": "No data found"
            }

        # Aggregate total consumption per company
        company_totals = {}
        for row in rows:
            if row.company_id in company_totals:
                company_totals[row.company_id] += float(row.total_consumption or 0)
            else:
                company_totals[row.company_id] = float(row.total_consumption or 0)

        # Sort in descending order
        sorted_totals = sorted(company_totals.items(), key=lambda x: x[1], reverse=True)

        color_palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]  # Extend if needed

        # Prepare response data
        data = []
        for idx, (company, value) in enumerate(sorted_totals):
            data.append({
                "label": company,
                "value": round(value, 2),
                "color": color_palette[idx % len(color_palette)]
            })

        return {
            "data": data,
            "unit": rows[0].unit_of_measurement if rows else "kWh",
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in electricity consumption bar chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/elect-source-bar-chart", response_model=Dict)
def get_electricity_source_bar_chart(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    consumption_source: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get summarized electric consumption by company and source for bar chart.
    """
    try:
        print(f"Received parameters - company_id: {company_id}, source: {consumption_source}, quarter: {quarter}, year: {year}")

        # Convert inputs to arrays
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        sources = consumption_source if isinstance(consumption_source, list) else [consumption_source] if consumption_source else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        print(f"Processed parameters - company_ids: {company_ids}, sources: {sources}, quarters: {quarters}, years: {years}")

        if not company_ids or not quarters or not years:
            return {
                "data": [],
                "unit": "kWh",
                "message": "Missing required parameters"
            }

        # Execute function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_electric_consumption_by_source(
                ARRAY[:company_ids]::text[],
                ARRAY[:sources]::text[],
                ARRAY[:quarters]::text[],
                ARRAY[:years]::smallint[]
            )
        """), {
            "company_ids": company_ids,
            "sources": sources,
            "quarters": quarters,
            "years": years
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "kWh",
                "message": "No data found"
            }

        # Prepare structured data for bar chart
        data = []
        color_palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2"]

        source_color_map = {}
        color_index = 0

        for row in rows:
            source = row.consumption_source
            if source not in source_color_map:
                source_color_map[source] = color_palette[color_index % len(color_palette)]
                color_index += 1

            data.append({
                "company_id": row.company_id,
                "source": row.consumption_source,
                "value": float(row.total_consumption or 0),
                "color": source_color_map[row.consumption_source]
            })

        return {
            "data": [item for item in data if item["value"] > 0],
            "unit": rows[0].unit_of_measurement if rows else "kWh",
            "message": "Success"
        }

    except Exception as e:
        print("Error in electric consumption bar chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/elect-quarter-bar-chart", response_model=Dict)
def get_quarterly_electric_consumption_bar_chart(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    consumption_source: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get quarterly electric consumption per company for bar chart
    """
    try:
        # Convert parameters to array form
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        sources = consumption_source if isinstance(consumption_source, list) else [consumption_source] if consumption_source else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        # Safety check for required fields
        if not company_ids or not quarters or not years:
            return {
                "data": [],
                "unit": "",
                "message": "Missing required parameters"
            }

        # Call stored function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_electric_consumption_by_quarter(
                ARRAY[:company_ids]::text[],
                ARRAY[:sources]::text[],
                ARRAY[:quarters]::text[],
                ARRAY[:years]::smallint[]
            )
        """), {
            "company_ids": company_ids,
            "sources": sources,
            "quarters": quarters,
            "years": years
        })

        rows = result.fetchall()

        if not rows:
            return {
                "data": [],
                "unit": "",
                "message": "No data found"
            }

        # Clean and organize data
        quarter_order = ['Q1', 'Q2', 'Q3', 'Q4']
        valid_rows = []
        unique_companies = set()

        for row in rows:
            if not row.year or not row.quarter or not row.total_consumption or not row.company_id:
                continue

            quarter_cleaned = str(row.quarter).upper().replace(" ", "")
            if quarter_cleaned not in quarter_order:
                continue

            unique_companies.add(row.company_id)
            valid_rows.append({
                "company_id": row.company_id,
                "quarter": quarter_cleaned,
                "value": float(row.total_consumption),
            })

        if not valid_rows:
            return {
                "data": [],
                "unit": rows[0].unit_of_measurement if rows else "",
                "message": "No valid data after filtering"
            }

        # Assign colors
        sorted_companies = sorted(unique_companies)
        color_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
        company_color_dict = {
            company: color_palette[idx % len(color_palette)]
            for idx, company in enumerate(sorted_companies)
        }

        data = []
        for item in valid_rows:
            data.append({
                "company_id": item["company_id"],
                "quarter": item["quarter"],
                "value": round(item["value"], 2),
                "color": company_color_dict[item["company_id"]]
            })

        return {
            "data": data,
            "unit": rows[0].unit_of_measurement if rows else "",
            "message": "Success"
        }

    except Exception as e:
        print("Error in /elect-quarter-bar-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal Server Error")

# DIESEL DASHBOARD
#diesel-years
@router.get("/diesel-years", response_model=Dict)
def get_distinct_diesel_years(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.func_environment_diesel_consumption_by_year(NULL, NULL, NULL)
            ORDER BY year ASC
        """))
        
        rows = result.fetchall()
        years = [row.year for row in rows]

        return {
            "data": years,
            "message": "Success",
            "count": len(years)
        }

    except Exception as e:
        print("Error fetching distinct years:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# diesel-cp-name
@router.get("/diesel-cp-name", response_model=Dict)
def get_distinct_diesel_cp_name(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("""
            SELECT DISTINCT company_property_name 
            FROM gold.func_environment_diesel_consumption_by_year(NULL, NULL, NULL, NULL, NULL)
            ORDER BY company_property_name ASC
        """))
        
        rows = result.fetchall()
        cp_name = [row.company_property_name for row in rows]

        return {
            "data": cp_name,
            "message": "Success",
            "count": len(cp_name)
        }

    except Exception as e:
        print("Error fetching distinct years:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# diesel-cp-type
@router.get("/diesel-cp-type", response_model=Dict)
def get_distinct_diesel_cp_type(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("""
            SELECT DISTINCT company_property_type 
            FROM gold.func_environment_diesel_consumption_by_year(NULL, NULL, NULL, NULL, NULL)
            ORDER BY company_property_type ASC
        """))
        
        rows = result.fetchall()
        cp_type = [row.company_property_type for row in rows]

        return {
            "data": cp_type,
            "message": "Success",
            "count": len(cp_type)
        }

    except Exception as e:
        print("Error fetching distinct years:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
#diesel-key-metrics
@router.get("/diesel-key-metrics", response_model=Dict)
def get_diesel_key_metrics(
    db: Session = Depends(get_db),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    company_property_name: Optional[Union[str, List[str]]] = Query(None),
    company_property_type: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get diesel key metrics (total consumption, average annual consumption + deviation)
    """
    try:
        # Process params into lists
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        company_property_names = company_property_name if isinstance(company_property_name, list) else [company_property_name] if company_property_name else None
        company_property_types = company_property_type if isinstance(company_property_type, list) else [company_property_type] if company_property_type else None
        months = month if isinstance(month, list) else [month] if month else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None

        # Execute function with proper casts
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_year(
                CAST(:company_ids AS VARCHAR(10)[]),
                CAST(:company_property_names AS VARCHAR(30)[]),
                CAST(:company_property_types AS VARCHAR(15)[]),
                CAST(:months AS VARCHAR(10)[]),
                CAST(:years AS SMALLINT[]),
                CAST(:quarters AS VARCHAR(2)[])
            )
        """), {
            'company_ids': company_ids,
            'company_property_names': company_property_names,
            'company_property_types': company_property_types,
            'months': months,
            'years': years,
            'quarters': quarters
        })

        # Convert result to list of dicts
        data = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        if not data:
            return {
                'total_diesel_consumption': 0,
                'unit_of_measurement': None,
                'average_annual_consumption': 0,
                'yearly_deviation': []
            }

        # Load data into pandas DataFrame for KPI calculation
        df = pd.DataFrame(data)

        # KPI 1: Total Diesel Consumption
        total_diesel = df["total_consumption"].sum()

        # KPI 3: Average annual diesel consumption + deviation from avg
        yearly_totals = df.groupby("year")["total_consumption"].sum()
        avg_consumption = yearly_totals.mean()

        deviation_df = yearly_totals.reset_index()
        deviation_df["deviation_from_avg"] = deviation_df["total_consumption"] - avg_consumption

        # Prepare deviations in list format for return
        yearly_deviation = [
            {
                "year": int(row["year"]),
                "total_consumption": round(row["total_consumption"], 2),
                "deviation_from_avg": round(row["deviation_from_avg"], 2)
            }
            for _, row in deviation_df.iterrows()
        ]

        # Return KPIs
        return {
            'total_diesel_consumption': round(total_diesel, 2),
            'unit_of_measurement': df['unit_of_measurement'].iloc[0],
            'average_annual_consumption': round(avg_consumption, 2),
            'yearly_deviation': yearly_deviation
        }

    except Exception as e:
        print("Error in diesel key metrics:", str(e))
        raise HTTPException(status_code=500, detail=str(e))
