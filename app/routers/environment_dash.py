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
from collections import defaultdict

import hashlib
import random

from ..auth_decorators import require_role, office_checker_only, get_current_user_with_roles

def generate_unique_color_map(property_names, palette=None):
    property_names = sorted(set(property_names))  # remove duplicates and sort

    if palette is None:
        palette = [
            "#6a3d9a",  # deep purple
            "#b15928",  # brown
            "#2ca02c",  # green
            "#d62728",  # red
            "#67a5bd",  # muted violet
            "#8c564b",  # saddle brown
            "#e377c2",  # pink
            "#7f7f7f",  # gray
            "#bcbd22",  # olive yellow
            "#c5b0d5",  # lavender
            "#c49c94",  # beige
            "#f7b6d2",  # light pink
            "#c7c7c7",  # light gray
            "#dbdb8d",  # yellow green
            "#17a768",  # teal green
            "#993366",  # plum
            "#6d904f",  # moss green
            "#8c6d31",  # ochre
            "#9e0142",  # dark rose
            "#bf812d"   # golden brown
        ]

    if len(property_names) > len(palette):
        # Generate more colors if not enough
        def random_color():
            return "#{:06x}".format(random.randint(0, 0xFFFFFF))
        while len(palette) < len(property_names):
            color = random_color()
            if color not in palette:
                palette.append(color)

    return {name: palette[i] for i, name in enumerate(property_names)}

router = APIRouter()
    
# ================================================================== APIs FOR DASHBOARD =========================================================
# WATER DASHBOARD
# key metrics
@router.get("/abstraction", response_model=Dict)
def get_water_abstraction(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get total water abstraction volume by year
    """
    try:
        # Convert parameters to proper format
        company_ids = None
        if company_id:
            if isinstance(company_id, list):
                company_ids = [str(cid) for cid in company_id if cid is not None]
            else:
                company_ids = [str(company_id)]
        
        quarters = None
        if quarter:
            if isinstance(quarter, list):
                quarters = [str(q) for q in quarter if q is not None]
            else:
                quarters = [str(quarter)]
        
        years = None  
        if year:
            if isinstance(year, list):
                years = [int(y) for y in year if y is not None]
            else:
                years = [int(year)]

        print(f"Abstraction request - Companies: {company_ids}, Quarters: {quarters}, Years: {years}")

        # Call the unified function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_summary(
                CAST(:company_ids AS VARCHAR(10)[]),
                CAST(:quarters AS VARCHAR(2)[]),
                CAST(:years AS SMALLINT[])
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        data = result.fetchall()
        
        # Sum up all abstraction volumes
        total_volume = 0
        for row in data:
            abstraction_vol = row.total_abstracted_volume or 0
            total_volume += float(abstraction_vol)

        print(f"Total abstraction volume: {total_volume}")

        return {
            'total_volume': round(total_volume, 2),
            'unit': 'cubic meters'
        }

    except Exception as e:
        print(f"Error in water abstraction: {str(e)}")
        return {
            'total_volume': 0.0,
            'unit': 'cubic meters'
        }

@router.get("/discharge", response_model=Dict)
def get_water_discharge(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get total water discharge volume by year
    """
    try:
        # Convert parameters to proper format
        company_ids = None
        if company_id:
            if isinstance(company_id, list):
                company_ids = [str(cid) for cid in company_id if cid is not None]
            else:
                company_ids = [str(company_id)]
        
        quarters = None
        if quarter:
            if isinstance(quarter, list):
                quarters = [str(q) for q in quarter if q is not None]
            else:
                quarters = [str(quarter)]
        
        years = None  
        if year:
            if isinstance(year, list):
                years = [int(y) for y in year if y is not None]
            else:
                years = [int(year)]

        print(f"Discharge request - Companies: {company_ids}, Quarters: {quarters}, Years: {years}")

        # Call the unified function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_summary(
                CAST(:company_ids AS VARCHAR(10)[]),
                CAST(:quarters AS VARCHAR(2)[]),
                CAST(:years AS SMALLINT[])
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        data = result.fetchall()
        
        # Sum up all discharge volumes
        total_volume = 0
        for row in data:
            discharge_vol = row.total_discharged_volume or 0
            total_volume += float(discharge_vol)

        print(f"Total discharge volume: {total_volume}")

        return {
            'total_volume': round(total_volume, 2),
            'unit': 'cubic meters'
        }

    except Exception as e:
        print(f"Error in water discharge: {str(e)}")
        return {
            'total_volume': 0.0,
            'unit': 'cubic meters'
        }

@router.get("/consumption", response_model=Dict)
def get_water_consumption(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get total water consumption volume by year
    """
    try:
        # Convert parameters to proper format
        company_ids = None
        if company_id:
            if isinstance(company_id, list):
                company_ids = [str(cid) for cid in company_id if cid is not None]
            else:
                company_ids = [str(company_id)]
        
        quarters = None
        if quarter:
            if isinstance(quarter, list):
                quarters = [str(q) for q in quarter if q is not None]
            else:
                quarters = [str(quarter)]
        
        years = None  
        if year:
            if isinstance(year, list):
                years = [int(y) for y in year if y is not None]
            else:
                years = [int(year)]

        print(f"Consumption request - Companies: {company_ids}, Quarters: {quarters}, Years: {years}")

        # Call the unified function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_water_summary(
                CAST(:company_ids AS VARCHAR(10)[]),
                CAST(:quarters AS VARCHAR(2)[]),
                CAST(:years AS SMALLINT[])
            )
        """), {
            'company_ids': company_ids,
            'quarters': quarters,
            'years': years
        })

        data = result.fetchall()
        
        # Sum up all consumption volumes
        total_volume = 0
        for row in data:
            consumption_vol = row.total_consumption_volume or 0
            total_volume += float(consumption_vol)

        print(f"Total consumption volume: {total_volume}")

        return {
            'total_volume': round(total_volume, 2),
            'unit': 'cubic meters'
        }

    except Exception as e:
        print(f"Error in water consumption: {str(e)}")
        return {
            'total_volume': 0.0,
            'unit': 'cubic meters'
        }

# pie chart
@router.get("/pie-chart", response_model=Dict)
def get_water_summary_pie(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
#@require_role("R02", "R03")
def get_distinct_years(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
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
def get_distinct_consumption_source(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
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
def get_distinct_electricity_years(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
        total = sum(row.total_consumption or 0 for row in rows)
        print(f"Total electric consumption: {total}")
        
        if total == 0:
            return {
                "data": [],
                "unit": rows[0].unit_of_measurement if rows else "kWh",
                "message": "All values are zero"
            }
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        data = []
        fallback_index = 0
        for row in rows:
            value = row.total_consumption or 0
            percentage = (value / total) * 100 if total > 0 else 0
            
            # Get color from database or use fallback
            color = company_colors.get(row.company_id)
            if not color:
                color = fallback_colors[fallback_index % len(fallback_colors)]
                fallback_index += 1
            
            data.append({
                "label": row.company_id,
                "value": round(value, 2),
                "percentage": round(percentage, 2),
                "color": color
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
        
        # Sort data by year for each company
        for company in company_data:
            company_data[company] = sorted(company_data[company], key=lambda x: x["year"])
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        # Map colors to company IDs (consistent ordering)
        sorted_company_ids = sorted(company_data.keys())
        color_map = {}
        fallback_index = 0
        
        for company in sorted_company_ids:
            # Get color from database or use fallback
            color = company_colors.get(company)
            if not color:
                color = fallback_colors[fallback_index % len(fallback_colors)]
                fallback_index += 1
            color_map[company] = color
        
        print(f"Final color mapping: {color_map}")
        
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        # Prepare response data
        data = []
        fallback_index = 0
        for company, value in sorted_totals:
            # Get color from database or use fallback
            color = company_colors.get(company)
            if not color:
                color = fallback_colors[fallback_index % len(fallback_colors)]
                fallback_index += 1
            
            data.append({
                "label": company,
                "value": round(value, 2),
                "color": color
            })
        
        print(f"Final bar chart data with colors: {[(item['label'], item['color']) for item in data]}")
        
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
                # Prepare structured data for bar chart
        unique_sources = [row.consumption_source for row in rows]
        source_color_map = generate_unique_color_map(unique_sources)

        data = [
            {
                "company_id": row.company_id,
                "source": row.consumption_source,
                "value": float(row.total_consumption or 0),
                "color": source_color_map[row.consumption_source]
            }
            for row in rows if float(row.total_consumption or 0) > 0
        ]

        return {
            "data": data,
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
        
        # Assign colors to companies
        sorted_companies = sorted(unique_companies)
        company_color_dict = {}
        fallback_index = 0
        
        for company in sorted_companies:
            # Get color from database or use fallback
            color = company_colors.get(company)
            if not color:
                color = fallback_colors[fallback_index % len(fallback_colors)]
                fallback_index += 1
            company_color_dict[company] = color
        
        print(f"Final company color mapping: {company_color_dict}")
        
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
@router.get("/diesel-pie-chart", response_model=Dict)
def get_diesel_consumption_pie_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    property_name: Optional[Union[str, List[str]]] = Query(None),
    property_type: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    """
    Get summarized diesel consumption for pie chart by company
    """
    try:
        # Convert inputs to arrays
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        property_names = property_name if isinstance(property_name, list) else [property_name] if property_name else None
        property_types = property_type if isinstance(property_type, list) else [property_type] if property_type else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        months = month if isinstance(month, list) else [month] if month else None
        years = year if isinstance(year, list) else [year] if year else None
        
        print(f"Processed parameters - company_ids: {company_ids}, property_names: {property_names}, property_types: {property_types}, months: {months}, quarters: {quarters}, years: {years}")
        
        if not company_ids or not years or (not months and not quarters):
            return {
                "data": [],
                "unit": "L",
                "message": "Missing required parameters"
            }
        
        # Execute the SQL function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_perc_lvl(
                ARRAY[:company_ids]::text[],
                ARRAY[:property_names]::text[],
                ARRAY[:property_types]::text[],
                ARRAY[:months]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[]
            )
        """), {
            "company_ids": company_ids,
            "property_names": property_names,
            "property_types": property_types,
            "months": months,
            "years": years,
            "quarters": quarters
        })
        
        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows")
        
        if not rows:
            return {
                "data": [],
                "unit": "L",
                "message": "No data found"
            }
        
        total = sum(row.total_consumption or 0 for row in rows)
        print(f"Total diesel consumption: {total}")
        
        if total == 0:
            return {
                "data": [],
                "unit": rows[0].unit_of_measurement if rows else "L",
                "message": "All values are zero"
            }
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        # Format data
        data = []
        fallback_index = 0
        for row in rows:
            value = row.total_consumption or 0
            percentage = (value / total) * 100 if total > 0 else 0
            
            # Get color from database or use fallback
            color = company_colors.get(row.company_id)
            if not color:
                color = fallback_colors[fallback_index % len(fallback_colors)]
                fallback_index += 1
            
            data.append({
                "company_id": row.company_id,
                "value": round(value, 2),
                "percentage": round(percentage, 2),
                "color": color
            })
        
        return {
            "data": [item for item in data if item["value"] > 0],
            "unit": rows[0].unit_of_measurement if rows else "L",
            "total_records": len(rows),
            "message": "Success"
        }
        
    except Exception as e:
        print("Error in diesel consumption pie chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/diesel-cp-chart", response_model=Dict)
def get_diesel_consumption_by_cp_name_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    company_property_name: Optional[Union[str, List[str]]] = Query(None),
    company_property_type: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None)
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        property_names = company_property_name if isinstance(company_property_name, list) else [company_property_name] if company_property_name else None
        property_types = company_property_type if isinstance(company_property_type, list) else [company_property_type] if company_property_type else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        months = month if isinstance(month, list) else [month] if month else None
        years = year if isinstance(year, list) else [year] if year else None

        if not company_ids or not years:
            return {"data": [], "unit": "L", "message": "Missing required parameters"}

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_cp_name(
                ARRAY[:company_ids]::text[],
                ARRAY[:property_names]::text[],
                ARRAY[:property_types]::text[],
                ARRAY[:months]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[]
            )
        """), {
            "company_ids": company_ids,
            "property_names": property_names,
            "property_types": property_types,
            "months": months,
            "years": years,
            "quarters": quarters
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": "L", "message": "No data found"}

        property_totals = {}
        unit = "L"

        for row in rows:
            name = row.company_property_name
            value = float(row.total_consumption or 0)
            unit = row.unit_of_measurement or unit
            property_totals[name] = property_totals.get(name, 0) + value

        if not property_totals:
            return {"data": [], "unit": unit, "message": "All values are zero"}

        total_consumption = sum(property_totals.values())
        if total_consumption == 0:
            return {"data": [], "unit": unit, "message": "All values are zero"}

        # Generate consistent color mapping
        color_map = generate_unique_color_map(property_totals.keys())

        data = []
        for prop, value in sorted(property_totals.items(), key=lambda x: x[1], reverse=True):
            percentage = (value / total_consumption) * 100
            data.append({
                "label": f"{prop}\n{value:,.2f} {unit} ({percentage:.2f}%)",
                "value": round(value, 2),
                "percentage": round(percentage, 2),
                "color": color_map[prop]
            })

        return {
            "data": data,
            "unit": unit,
            "total_records": len(property_totals),
            "message": "Success"
        }

    except Exception as e:
        print("Error in diesel consumption pie chart by company property:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/diesel-line-chart", response_model=Dict)
def get_diesel_consumption_line_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    property_name: Optional[Union[str, List[str]]] = Query(None),
    property_type: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None)
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        property_names = property_name if isinstance(property_name, list) else [property_name] if property_name else None
        property_types = property_type if isinstance(property_type, list) else [property_type] if property_type else None
        months = month if isinstance(month, list) else [month] if month else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_year(
                ARRAY[:company_ids]::text[],
                ARRAY[:property_names]::text[],
                ARRAY[:property_types]::text[],
                ARRAY[:months]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[]
            )
        """), {
            "company_ids": company_ids,
            "property_names": property_names,
            "property_types": property_types,
            "months": months,
            "years": years,
            "quarters": quarters
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": "L", "message": "No data found"}

        grouped = defaultdict(lambda: defaultdict(float))
        unit = rows[0].unit_of_measurement

        for row in rows:
            grouped[row.company_property_name][row.year] += float(row.total_consumption or 0)

        # Generate consistent color mapping
        color_map = generate_unique_color_map(grouped.keys())

        chart_data = []
        for property_name, yearly_data in grouped.items():
            sorted_years = sorted(yearly_data.items())
            chart_data.append({
                "property_name": property_name,
                "color": color_map[property_name],
                "data": [{"year": y, "total_consumption": round(v, 2)} for y, v in sorted_years]
            })

        return {
            "data": chart_data,
            "unit": unit,
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in diesel line chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/diesel-cp-type-chart", response_model=Dict)
def get_diesel_consumption_by_cp_type_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    property_name: Optional[Union[str, List[str]]] = Query(None),
    property_type: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None)
):
    """
    Get diesel consumption summary by company_property_type (used for bar chart)
    """
    try:
        print(f"Received parameters - company_id: {company_id}, property_name: {property_name}, property_type: {property_type}, month: {month}, year: {year}, quarter: {quarter}")

        # Convert to list if not already
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        property_names = property_name if isinstance(property_name, list) else [property_name] if property_name else None
        property_types = property_type if isinstance(property_type, list) else [property_type] if property_type else None
        months = month if isinstance(month, list) else [month] if month else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None

        if not company_ids or not years or not quarters:
            return {
                "data": [],
                "unit": "L",
                "message": "Missing required parameters"
            }

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_cp_type(
                ARRAY[:company_ids]::text[],
                ARRAY[:property_names]::text[],
                ARRAY[:property_types]::text[],
                ARRAY[:months]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[]
            )
        """), {
            "company_ids": company_ids,
            "property_names": property_names,
            "property_types": property_types,
            "months": months,
            "years": years,
            "quarters": quarters
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows")

        if not rows:
            return {
                "data": [],
                "unit": "L",
                "message": "No data found"
            }

        from collections import defaultdict
        grouped = defaultdict(float)
        unit = rows[0].unit_of_measurement

        for row in rows:
            if row.company_property_type:
                grouped[row.company_property_type] += float(row.total_consumption or 0)

        if not grouped:
            return {
                "data": [],
                "unit": unit,
                "message": "All values are zero"
            }

        sorted_data = sorted(grouped.items(), key=lambda x: x[1])

        # Custom color palettes (NOT using the previous one) 27ae60
        palette_1 = ["#16a085", "#2c3e50", "#2980b9", "#8e44ad", "#27ae60"]
        palette_2 = ["#f39c12", "#e67e22", "#e74c3c", "#c0392b", "#d35400"]
        full_palette = palette_1 + palette_2

        chart_data = []
        for idx, (label, value) in enumerate(sorted_data):
            chart_data.append({
                "label": label,
                "value": round(value, 2),
                "color": full_palette[idx % len(full_palette)]
            })

        return {
            "data": chart_data,
            "unit": unit,
            "total_records": len(chart_data),
            "message": "Success"
        }

    except Exception as e:
        print("Error in diesel cp-type chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")
    
@router.get("/diesel-cp-line-chart", response_model=Dict)
def get_diesel_consumption_line_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    company_property_name: Optional[Union[str, List[str]]] = Query(None),
    company_property_type: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None)
):
    """
    Get diesel consumption line chart by company property per month
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        property_names = company_property_name if isinstance(company_property_name, list) else [company_property_name] if company_property_name else None
        property_types = company_property_type if isinstance(company_property_type, list) else [company_property_type] if company_property_type else None
        months = month if isinstance(month, list) else [month] if month else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None

        if not company_ids or not years:
            return {
                "data": [],
                "unit": "L",
                "message": "Missing required parameters"
            }

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_month(
                ARRAY[:company_ids]::text[],
                ARRAY[:property_names]::text[],
                ARRAY[:property_types]::text[],
                ARRAY[:months]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[]
            )
        """), {
            "company_ids": company_ids,
            "property_names": property_names,
            "property_types": property_types,
            "months": months,
            "years": years,
            "quarters": quarters
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} diesel rows")

        if not rows:
            return {
                "data": [],
                "unit": "L",
                "message": "No data found"
            }

        grouped_data = {}
        unit = rows[0].unit_of_measurement if rows else "L"

        for row in rows:
            year = row.year
            month = row.month
            property_name = row.company_property_name
            consumption = float(row.total_consumption or 0)

            if year not in grouped_data:
                grouped_data[year] = {}

            if property_name not in grouped_data[year]:
                grouped_data[year][property_name] = {}

            grouped_data[year][property_name][month] = consumption

        all_properties = sorted({row.company_property_name for row in rows})

        # ✅ Use generate_unique_color_map here
        color_map = generate_unique_color_map(all_properties)

        return {
            "data": grouped_data,
            "properties": all_properties,
            "color_map": color_map,
            "unit": unit,
            "message": "Success"
        }

    except Exception as e:
        print("Error in diesel cp line chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/diesel-quarter-bar-chart", response_model=Dict)
def get_diesel_quarter_bar_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    company_property_name: Optional[Union[str, List[str]]] = Query(None),
    company_property_type: Optional[Union[str, List[str]]] = Query(None),
    month: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None)
):
    """
    Get summarized diesel consumption for bar chart by quarter
    """
    try:
        print(f"Received params - company_id: {company_id}, property_name: {company_property_name}, "
              f"property_type: {company_property_type}, month: {month}, year: {year}, quarter: {quarter}")

        # Normalize parameters
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        property_names = company_property_name if isinstance(company_property_name, list) else [company_property_name] if company_property_name else None
        property_types = company_property_type if isinstance(company_property_type, list) else [company_property_type] if company_property_type else None
        months = month if isinstance(month, list) else [month] if month else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None

        print(f"Processed params - company_ids: {company_ids}, property_names: {property_names}, "
              f"property_types: {property_types}, months: {months}, years: {years}, quarters: {quarters}")

        if not company_ids or not years or not quarters:
            return {
                "data": [],
                "unit": "L",
                "message": "Missing required parameters"
            }

        # Execute the database function
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_diesel_consumption_by_quarter(
                ARRAY[:company_ids]::text[],
                ARRAY[:property_names]::text[],
                ARRAY[:property_types]::text[],
                ARRAY[:months]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[]
            )
        """), {
            "company_ids": company_ids,
            "property_names": property_names,
            "property_types": property_types,
            "months": months,
            "years": years,
            "quarters": quarters
        })

        rows = result.fetchall()
        print(f"Fetched {len(rows)} rows from function")

        if not rows:
            return {
                "data": [],
                "unit": "L",
                "message": "No data found"
            }

        # Group and sum total_consumption by quarter and property_name
        consumption_data = {}
        all_properties = set()
        unit = rows[0].unit_of_measurement if rows else "L"

        for row in rows:
            qtr = row.quarter
            prop = row.company_property_name
            val = float(row.total_consumption or 0)

            all_properties.add(prop)

            if qtr not in consumption_data:
                consumption_data[qtr] = {}

            if prop not in consumption_data[qtr]:
                consumption_data[qtr][prop] = 0

            consumption_data[qtr][prop] += val

        # Create color map
        color_map = generate_unique_color_map(sorted(all_properties))

        # Prepare data for frontend
        data = []
        for quarter_label, properties in sorted(consumption_data.items()):
            for property_name, total in properties.items():
                data.append({
                    "quarter": quarter_label,
                    "property_name": property_name,
                    "total_consumption": round(total, 2)
                })

        return {
            "data": data,
            "color_map": color_map,
            "unit": unit,
            "message": "Success"
        }

    except Exception as e:
        print("Error in diesel quarter bar chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

#diesel-years
@router.get("/diesel-years", response_model=Dict)
def get_distinct_diesel_years(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
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
def get_distinct_diesel_cp_name(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
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
def get_distinct_diesel_cp_type(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
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
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
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

# WASTE DASHBOARD
# hazardous waste section
# hazardous waste waste_type
@router.get("/hazardous-waste-type", response_model=Dict)
def get_distinct_hazardous_waste_type(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT waste_type, unit
            FROM gold.func_environment_hazard_waste_generated_by_year(NULL, NULL, NULL, NULL, NULL)
            ORDER BY waste_type ASC
        """))
        
        rows = result.fetchall()
        waste_type = [{"waste_type": row.waste_type, "unit": row.unit} for row in rows]

        return {
            "data": waste_type,
            "message": "Success",
            "count": len(waste_type)
        }

    except Exception as e:
        print("Error fetching distinct waste_type:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# get years
@router.get("/hazardous-waste-years", response_model=Dict)
def get_distinct_hazardous_waste_years(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.func_environment_hazard_waste_generated_by_year(NULL, NULL, NULL, NULL, NULL)
            ORDER BY year ASC
        """))
        
        rows = result.fetchall()
        year = [row.year for row in rows]

        return {
            "data": year,
            "message": "Success",
            "count": len(year)
        }

    except Exception as e:
        print("Error fetching distinct year:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# get units
@router.get("/hazardous-waste-units", response_model=Dict)
def get_distinct_hazardous_waste_units(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT unit 
            FROM gold.func_environment_hazard_waste_generated_by_year(NULL, NULL, NULL, NULL, NULL)
            ORDER BY unit ASC
        """))
        
        rows = result.fetchall()
        unit = [row.unit for row in rows]

        return {
            "data": unit,
            "message": "Success",
            "count": len(unit)
        }

    except Exception as e:
        print("Error fetching distinct unit:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# hazardous waste key metrics
@router.get("/hazard-waste-key-metrics", response_model=Dict)
def get_hazard_waste_key_metrics(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None)
):
    """
    Get hazard waste key metrics (total, average, breakdown by unit and type)
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        units = unit if isinstance(unit, list) else [unit] if unit else None  # Fixed variable name

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_hazard_waste_generated_by_year(
                ARRAY[:company_ids]::text[], 
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[],
                ARRAY[:waste_types]::text[],
                ARRAY[:units]::text[]
            )
        """), {
            'company_ids': company_ids,
            'years': years,
            'quarters': quarters,
            'waste_types': waste_types,
            'units': units  # Fixed parameter name
        })

        rows = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        if not rows:
            return {
                "kilograms": {
                    "total_generated": 0,
                    "average_per_year": 0,
                    "yearly_breakdown": [],
                    "waste_type_average": []
                },
                "liters": {
                    "total_generated": 0,
                    "average_per_year": 0,
                    "yearly_breakdown": [],
                    "waste_type_average": []
                },
                "combined": {
                    "total_generated": 0,
                    "average_per_year": 0,
                    "most_generated_waste_type": None
                }
            }

        # Debug: Print all rows to see what we're getting
        print("Debug - All rows:")
        for row in rows:
            print(f"  {row}")

        # Separate by unit
        kg_data = [row for row in rows if row['unit'] == 'Kilogram']
        liter_data = [row for row in rows if row['unit'] == 'Liter']

        print(f"Debug - KG data count: {len(kg_data)}")
        print(f"Debug - Liter data count: {len(liter_data)}")

        # Get unique waste types for debugging
        kg_waste_types = list(set([row['waste_type'] for row in kg_data]))
        liter_waste_types = list(set([row['waste_type'] for row in liter_data]))
        
        print(f"Debug - Unique KG waste types: {kg_waste_types}")
        print(f"Debug - Unique Liter waste types: {liter_waste_types}")

        # Compute KG metrics
        total_kg = sum(row['total_generate'] for row in kg_data)
        kg_yearly = {}
        
        # Calculate yearly totals for KG
        for row in kg_data:
            kg_yearly[row['year']] = kg_yearly.get(row['year'], 0) + row['total_generate']

        # Calculate waste type totals across all years for KG
        kg_by_type_totals = {}
        for row in kg_data:
            waste_type = row['waste_type']
            kg_by_type_totals[waste_type] = kg_by_type_totals.get(waste_type, 0) + row['total_generate']

        # Calculate average per year for each waste type (total/number of years that waste type appears)
        kg_waste_type_years = {}
        for row in kg_data:
            waste_type = row['waste_type']
            if waste_type not in kg_waste_type_years:
                kg_waste_type_years[waste_type] = set()
            kg_waste_type_years[waste_type].add(row['year'])

        avg_kg_per_year = sum(kg_yearly.values()) / len(kg_yearly) if kg_yearly else 0
        
        kg_by_type_avg = []
        for waste_type, total in kg_by_type_totals.items():
            years_count = len(kg_waste_type_years[waste_type])
            avg = total / years_count if years_count > 0 else 0
            kg_by_type_avg.append({
                "waste_type": waste_type, 
                "average_generated": round(avg, 2)
            })

        print(f"Debug - KG waste type averages: {kg_by_type_avg}")

        # Compute Liter metrics (same logic)
        total_liters = sum(row['total_generate'] for row in liter_data)
        liter_yearly = {}
        
        # Calculate yearly totals for Liters
        for row in liter_data:
            liter_yearly[row['year']] = liter_yearly.get(row['year'], 0) + row['total_generate']

        # Calculate waste type totals across all years for Liters
        liter_by_type_totals = {}
        for row in liter_data:
            waste_type = row['waste_type']
            liter_by_type_totals[waste_type] = liter_by_type_totals.get(waste_type, 0) + row['total_generate']

        # Calculate average per year for each waste type
        liter_waste_type_years = {}
        for row in liter_data:
            waste_type = row['waste_type']
            if waste_type not in liter_waste_type_years:
                liter_waste_type_years[waste_type] = set()
            liter_waste_type_years[waste_type].add(row['year'])

        avg_liters_per_year = sum(liter_yearly.values()) / len(liter_yearly) if liter_yearly else 0
        
        liter_by_type_avg = []
        for waste_type, total in liter_by_type_totals.items():
            years_count = len(liter_waste_type_years[waste_type])
            avg = total / years_count if years_count > 0 else 0
            liter_by_type_avg.append({
                "waste_type": waste_type, 
                "average_generated": round(avg, 2)
            })

        print(f"Debug - Liter waste type averages: {liter_by_type_avg}")

        # Combined Summary
        total_combined = total_kg + total_liters
        avg_combined = avg_kg_per_year + avg_liters_per_year

        # Most Generated Waste Type (across all units)
        all_by_type = {}
        for row in rows:
            all_by_type[row['waste_type']] = all_by_type.get(row['waste_type'], 0) + row['total_generate']

        top_type = max(all_by_type.items(), key=lambda x: x[1]) if all_by_type else (None, 0)

        return {
            "kilograms": {
                "total_generated": round(total_kg, 2),
                "average_per_year": round(avg_kg_per_year, 2),
                "yearly_breakdown": [{"year": y, "total_generated": round(v, 2)} for y, v in sorted(kg_yearly.items())],
                "waste_type_average": kg_by_type_avg
            },
            "liters": {
                "total_generated": round(total_liters, 2),
                "average_per_year": round(avg_liters_per_year, 2),
                "yearly_breakdown": [{"year": y, "total_generated": round(v, 2)} for y, v in sorted(liter_yearly.items())],
                "waste_type_average": liter_by_type_avg
            },
            "combined": {
                "total_generated": round(total_combined, 2),
                "average_per_year": round(avg_combined, 2),
                "most_generated_waste_type": {
                    "waste_type": top_type[0],
                    "total_generated": round(top_type[1], 2)
                } if top_type[0] else None
            }
        }

    except Exception as e:
        print("Error in hazard waste key metrics:", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")

# hazardous waste generated line chart (use this for Hazardous Waste Generated in Year)
@router.get("/hazard-waste-generated-line-chart", response_model=Dict)
def get_hazard_waste_generated_line_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None)
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_hazard_waste_generated_by_year(
                ARRAY[:company_ids]::text[], 
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """), {
            "company_ids": company_ids,
            "years": years,
            "quarters": quarters,
            "waste_types": waste_types,
            "unit": unit
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}

        # Grouping by waste_type and year
        grouped = defaultdict(lambda: defaultdict(float))
        for row in rows:
            grouped[row.waste_type][row.year] += float(row.total_generate or 0)

        unit = rows[0].unit if rows else "kg or L"
        color_map = generate_unique_color_map(grouped.keys())

        chart_data = []
        for waste_type, yearly_data in grouped.items():
            sorted_years = sorted(yearly_data.items())
            chart_data.append({
                "waste_type": waste_type,
                "color": color_map[waste_type],
                "data": [{"year": int(y), "total_generate": round(v, 2)} for y, v in sorted_years]
            })

        return {
            "data": chart_data,
            "unit": unit,
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in hazard waste line chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# hazardous waste type bar chart (use this for Hazardous Waste Composition by Type Pie Chart and for Total Hazardous Waste Generated by Waste Type Bar Chart)
@router.get("/hazard-waste-type-bar-chart", response_model=Dict)
def get_hazard_waste_type_bar_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None)
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_hazard_waste_generated_by_waste_type(
                ARRAY[:company_ids]::text[], 
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """), {
            "company_ids": company_ids,
            "years": years,
            "quarters": quarters,
            "waste_types": waste_types,
            "unit": unit
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}

        data = [{
            "company_id": r.company_id,
            "waste_type": r.waste_type,
            "unit": r.unit,
            "total_generate": float(r.total_generate or 0)
        } for r in rows]

        # Get all unique waste types for color mapping
        waste_types = [d["waste_type"] for d in data]
        color_map = generate_unique_color_map(waste_types)

        # Add color to each record
        for d in data:
            d["color"] = color_map[d["waste_type"]]

        return {
            "data": data,
            "unit": data[0]["unit"] if data else "",
            "total_records": len(data),
            "message": "Success"
        }

    except Exception as e:
        print("Error in hazard waste type bar chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# hazardous waste type bar chart by quarter (use for Total Hazardous Waste Generated per Quarter by Waste Type Bar Chart)
@router.get("/hazard-waste-quarter-bar-chart", response_model=Dict)
def get_hazard_waste_quarter_bar_chart_by_quarter(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None

        query = text("""
            SELECT * FROM gold.func_environment_hazard_waste_generated_by_quarter(
                ARRAY[:company_ids]::text[], 
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """)
        result = db.execute(query, {
            "company_ids": company_ids,
            "years": years,
            "quarters": quarters,
            "waste_types": waste_types,
            "unit": unit
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}

        data = [{
            "company_id": r.company_id,
            "year": r.year,
            "quarter": r.quarter,
            "waste_type": r.waste_type,
            "unit": r.unit,
            "total_generate": float(r.total_generate or 0)
        } for r in rows]

        # Unique waste types for color mapping
        waste_types_list = [d["waste_type"] for d in data]
        color_map = generate_unique_color_map(waste_types_list)

        for d in data:
            d["color"] = color_map[d["waste_type"]]

        return {
            "data": data,
            "unit": data[0]["unit"] if data else "",
            "total_records": len(data),
            "message": "Success"
        }

    except Exception as e:
        print("Error in /hazard-waste-type-bar-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")
    
# hazard waste percentage pie chart (use this for Total Hazardous Waste Generated by Company Combine Pie Chart)
@router.get("/hazard-waste-perc-pie-chart", response_model=Dict)
def get_hazard_waste_percentage_pie_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None
        
        query = text("""
            SELECT * FROM gold.func_environment_hazard_waste_generated_by_perc_lvl(
                ARRAY[:company_ids]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:quarters]::text[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "years": years,
            "quarters": quarters,
            "waste_types": waste_types,
            "unit": unit
        })
        
        rows = result.fetchall()
        
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}
        
        data = [{
            "company_id": r.company_id,
            "unit": r.unit,
            "total_generate": float(r.total_generate or 0)
        } for r in rows]
        
        total_all = sum(d["total_generate"] for d in data)
        
        if total_all == 0:
            return {"data": [], "unit": "", "message": "No waste generated"}
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        def generate_color_map(company_data, company_colors_dict, fallback_palette):
            """Generate color mapping using database colors with fallback"""
            unique_companies = sorted(set(d["company_id"] for d in company_data))
            color_map = {}
            fallback_index = 0
            
            for company in unique_companies:
                # Get color from database or use fallback
                color = company_colors_dict.get(company)
                if not color:
                    color = fallback_palette[fallback_index % len(fallback_palette)]
                    fallback_index += 1
                color_map[company] = color
            
            return color_map
        
        # Generate color mapping using database colors
        color_map = generate_color_map(data, company_colors, fallback_colors)
        print(f"Final color mapping: {color_map}")
        
        # Add percentage and color to each data item
        for d in data:
            d["percentage"] = round((d["total_generate"] / total_all) * 100, 2)
            d["color"] = color_map[d["company_id"]]
        
        unique_units = list(sorted(set(d["unit"] for d in data)))
        
        return {
            "data": data,
            "unit": unique_units,
            "total_generate": round(total_all, 2),
            "message": "Success"
        }
        
    except Exception as e:
        print("Error in /hazard-waste-perc-pie-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# hazardous waste (disposed) section
# get disposed hazardous waste type
@router.get("/hazardous-waste-dis-type", response_model=Dict)
def get_distinct_hazardous_waste_dis_type(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT waste_type, unit
            FROM gold.func_environment_hazard_waste_disposed_by_year(NULL, NULL, NULL, NULL)
            ORDER BY waste_type ASC
        """))
        
        rows = result.fetchall()
        waste_type = [{"waste_type": row.waste_type, "unit": row.unit} for row in rows]

        return {
            "data": waste_type,
            "message": "Success",
            "count": len(waste_type)
        }

    except Exception as e:
        print("Error fetching distinct waste_type:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# get years for hazardous waste disposed
@router.get("/hazardous-waste-dis-years", response_model=Dict)
def get_distinct_hazardous_waste_dis_years(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.func_environment_hazard_waste_disposed_by_year(NULL, NULL, NULL, NULL)
            ORDER BY year ASC
        """))
        
        rows = result.fetchall()
        year = [row.year for row in rows]

        return {
            "data": year,
            "message": "Success",
            "count": len(year)
        }

    except Exception as e:
        print("Error fetching distinct year:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# get units for hazardous waste disposed
@router.get("/hazardous-waste-dis-units", response_model=Dict)
def get_distinct_hazardous_waste_dis_units(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT unit 
            FROM gold.func_environment_hazard_waste_disposed_by_year(NULL, NULL, NULL, NULL)
            ORDER BY unit ASC
        """))
        
        rows = result.fetchall()
        unit = [row.unit for row in rows]

        return {
            "data": unit,
            "message": "Success",
            "count": len(unit)
        }

    except Exception as e:
        print("Error fetching distinct unit:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# hazardous waste key metrics
@router.get("/hazard-waste-dis-key-metrics", response_model=Dict)
def get_hazard_waste_dis_key_metrics(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None)
):
    """
    Get hazard waste key metrics (total, average, breakdown by unit and type)
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_hazard_waste_disposed_by_year(
                ARRAY[:company_ids]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """), {
            'company_ids': company_ids,
            'years': years,
            'waste_types': waste_types,
            'unit': unit
        })

        rows = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        if not rows:
            return {
                "kilograms": {
                    "total_disposed": 0,
                    "average_per_year": 0,
                    "yearly_breakdown": [],
                    "waste_type_average": []
                },
                "liters": {
                    "total_disposed": 0,
                    "average_per_year": 0,
                    "yearly_breakdown": [],
                    "waste_type_average": []
                },
                "combined": {
                    "total_disposed": 0,
                    "average_per_year": 0,
                    "most_disposed_waste_type": None
                }
            }

        # Separate by unit
        kg_data = [row for row in rows if row['unit'] == 'Kilogram']
        liter_data = [row for row in rows if row['unit'] == 'Liter']

        # Compute KG metrics
        total_kg = sum(row['total_disposed'] for row in kg_data)
        kg_yearly = {}
        kg_by_type = {}

        for row in kg_data:
            kg_yearly[row['year']] = kg_yearly.get(row['year'], 0) + row['total_disposed']
            kg_by_type[row['waste_type']] = kg_by_type.get(row['waste_type'], []) + [row['total_disposed']]

        avg_kg_per_year = sum(kg_yearly.values()) / len(kg_yearly) if kg_yearly else 0
        kg_by_type_avg = [
            {"waste_type": k, "average_disposed": round(sum(v)/len(v), 2)} for k, v in kg_by_type.items()
        ]

        # Compute Liter metrics
        total_liters = sum(row['total_disposed'] for row in liter_data)
        liter_yearly = {}
        liter_by_type = {}

        for row in liter_data:
            liter_yearly[row['year']] = liter_yearly.get(row['year'], 0) + row['total_disposed']
            liter_by_type[row['waste_type']] = liter_by_type.get(row['waste_type'], []) + [row['total_disposed']]

        avg_liters_per_year = sum(liter_yearly.values()) / len(liter_yearly) if liter_yearly else 0
        liter_by_type_avg = [
            {"waste_type": k, "average_disposed": round(sum(v)/len(v), 2)} for k, v in liter_by_type.items()
        ]

        # Combined Summary
        total_combined = total_kg + total_liters
        avg_combined = avg_kg_per_year + avg_liters_per_year

        # Most Disposed Waste Type
        all_by_type = {}
        for row in rows:
            all_by_type[row['waste_type']] = all_by_type.get(row['waste_type'], 0) + row['total_disposed']

        top_type = max(all_by_type.items(), key=lambda x: x[1]) if all_by_type else (None, 0)

        return {
            "kilograms": {
                "total_disposed": round(total_kg, 2),
                "average_per_year": round(avg_kg_per_year, 2),
                "yearly_breakdown": [{"year": y, "total_disposed": round(v, 2)} for y, v in sorted(kg_yearly.items())],
                "waste_type_average": kg_by_type_avg
            },
            "liters": {
                "total_disposed": round(total_liters, 2),
                "average_per_year": round(avg_liters_per_year, 2),
                "yearly_breakdown": [{"year": y, "total_disposed": round(v, 2)} for y, v in sorted(liter_yearly.items())],
                "waste_type_average": liter_by_type_avg
            },
            "combined": {
                "total_disposed": round(total_combined, 2),
                "average_per_year": round(avg_combined, 2),
                "most_generated_waste_type": {
                    "waste_type": top_type[0],
                    "total_disposed": round(top_type[1], 2)
                }
            }
        }

    except Exception as e:
        print("Error in hazard waste disposed key metrics:", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")

# hazardous waste disposed line chart (use this for Hazardous Waste Disposed by Company)
@router.get("/hazard-waste-dis-perc-pie-chart", response_model=Dict)
def get_hazard_waste_disposed_percentage_pie_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None
        
        query = text("""
            SELECT * FROM gold.func_environment_hazard_waste_disposed_by_perc_lvl(
                ARRAY[:company_ids]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "years": years,
            "waste_types": waste_types,
            "unit": unit
        })
        
        rows = result.fetchall()
        
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}
        
        data = [{
            "company_id": r.company_id,
            "unit": r.unit,
            "total_disposed": float(r.total_disposed or 0)
        } for r in rows]
        
        total_all = sum(d["total_disposed"] for d in data)
        
        if total_all == 0:
            return {"data": [], "unit": "", "message": "No waste disposed"}
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        def generate_color_map(company_data, company_colors_dict, fallback_palette):
            """Generate color mapping using database colors with fallback"""
            unique_companies = sorted(set(d["company_id"] for d in company_data))
            color_map = {}
            fallback_index = 0
            
            for company in unique_companies:
                # Get color from database or use fallback
                color = company_colors_dict.get(company)
                if not color:
                    color = fallback_palette[fallback_index % len(fallback_palette)]
                    fallback_index += 1
                color_map[company] = color
            
            return color_map
        
        # Generate color mapping using database colors
        color_map = generate_color_map(data, company_colors, fallback_colors)
        print(f"Final color mapping: {color_map}")
        
        # Add percentage and color to each data item
        for d in data:
            d["percentage"] = round((d["total_disposed"] / total_all) * 100, 2)
            d["color"] = color_map[d["company_id"]]
        
        unique_units = list(sorted(set(d["unit"] for d in data)))
        
        return {
            "data": data,
            "unit": unique_units,
            "total_disposed": round(total_all, 2),
            "message": "Success"
        }
        
    except Exception as e:
        print("Error in /hazard-waste-dis-perc-pie-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# hazardous waste disposed by type chart (use this for Hazardous Waste Disposed by Waste Type Pie Chart and for Total Hazardous Waste Disposed by Waste Type Bar Chart)
@router.get("/hazard-waste-dis-type-chart", response_model=Dict)
def get_hazard_waste_disposed_by_type_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        # Normalize input parameters
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None

        query = text("""
            SELECT * FROM gold.func_environment_hazard_waste_disposed_by_waste_type(
                ARRAY[:company_ids]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """)
        result = db.execute(query, {
            "company_ids": company_ids,
            "years": years,
            "waste_types": waste_types,
            "unit": unit
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": [], "message": "No data found"}

        data = [{
            "company_id": r.company_id,
            "waste_type": r.waste_type,
            "unit": r.unit,
            "total_disposed": float(r.total_disposed or 0)
        } for r in rows]

        total_all = sum(d["total_disposed"] for d in data)
        if total_all == 0:
            return {"data": [], "unit": [], "message": "No waste disposed"}

        waste_type_names = [d["waste_type"] for d in data]
        color_map = generate_unique_color_map(waste_type_names)

        for d in data:
            d["percentage"] = round((d["total_disposed"] / total_all) * 100, 2)
            d["color"] = color_map[d["waste_type"]]

        return {
            "data": data,
            "unit": sorted(set(d["unit"] for d in data)),
            "total_disposed": round(total_all, 2),
            "message": "Success"
        }

    except Exception as e:
        print("Error in /hazard-waste-dis-type-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# hazardous waste disposed line chart (use this for Hazardous Waste Disposed in Year)
@router.get("/hazard-waste-dis-line-chart", response_model=Dict)
def get_hazard_waste_dis_line_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None)
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_hazard_waste_disposed_by_year(
                ARRAY[:company_ids]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """), {
            "company_ids": company_ids,
            "years": years,
            "waste_types": waste_types,
            "unit": unit
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}

        grouped = defaultdict(lambda: defaultdict(float))
        for row in rows:
            grouped[row.waste_type][row.year] += float(row.total_disposed or 0)

        unit_value = rows[0].unit if rows else "kg or L"
        color_map = generate_unique_color_map(grouped.keys())

        chart_data = []
        for waste_type, yearly_data in grouped.items():
            sorted_years = sorted(yearly_data.items())
            chart_data.append({
                "waste_type": waste_type,
                "color": color_map[waste_type],
                "data": [{"year": int(y), "total_disposed": round(v, 2)} for y, v in sorted_years]
            })

        return {
            "data": chart_data,
            "unit": unit_value,
            "total_records": len(rows),
            "message": "Success"
        }

    except Exception as e:
        print("Error in hazard waste disposed line chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# hazardous waste disposed percentage bar chart (use this for Hazardous Disposed Yearly Comparison Bar Chart)
@router.get("/hazard-waste-dis-perc-bar-chart", response_model=Dict)
def get_hazard_waste_dis_perc_bar_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    waste_type: Optional[Union[str, List[str]]] = Query(None),
    unit: Optional[Union[str, List[str]]] = Query(None)
):
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        waste_types = waste_type if isinstance(waste_type, list) else [waste_type] if waste_type else None
        unit = unit if isinstance(unit, list) else [unit] if unit else None
        
        result = db.execute(text("""
            SELECT * FROM gold.func_environment_hazard_waste_disposed_by_year(
                ARRAY[:company_ids]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:waste_types]::text[],
                ARRAY[:unit]::text[]
            )
        """), {
            "company_ids": company_ids,
            "years": years,
            "waste_types": waste_types,
            "unit": unit
        })
        
        rows = result.fetchall()
        
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}
        
        # Group by company_id, each having per-year records
        company_year_data = defaultdict(lambda: defaultdict(float))
        for row in rows:
            company_year_data[row.company_id][row.year] += float(row.total_disposed or 0)
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        # Assign colors to companies
        company_ids_sorted = sorted(company_year_data.keys())
        color_map = {}
        fallback_index = 0
        
        for company_id in company_ids_sorted:
            # Get color from database or use fallback
            color = company_colors.get(company_id)
            if not color:
                color = fallback_colors[fallback_index % len(fallback_colors)]
                fallback_index += 1
            color_map[company_id] = color
        
        print(f"Final color mapping: {color_map}")
        
        unit_value = rows[0].unit if rows else "kg or L"
        
        chart_data = []
        for company_id, yearly_data in company_year_data.items():
            year_data = [{"year": int(y), "total_disposed": round(v, 2)} for y, v in sorted(yearly_data.items())]
            chart_data.append({
                "company_id": company_id,
                "color": color_map[company_id],
                "data": year_data
            })
        
        return {
            "data": chart_data,
            "unit": unit_value,
            "total_records": len(rows),
            "message": "Success"
        }
        
    except Exception as e:
        print("Error in hazard waste disposed perc bar chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# non hazard generated section
# get non hazardous metrics
@router.get("/non-hazardous-metrics", response_model=Dict)
def get_distinct_hazardous_metrics(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT metrics, unit_of_measurement
            FROM gold.func_environment_non_hazard_waste_by_year(NULL, NULL, NULL, NULL, NULL)
            ORDER BY metrics ASC
        """))
        
        rows = result.fetchall()
        metrics = [{"metrics": row.metrics, "unit_of_measurement": row.unit_of_measurement} for row in rows]

        return {
            "data": metrics,
            "message": "Success",
            "count": len(metrics)
        }

    except Exception as e:
        print("Error fetching distinct metrics:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# get years for hazardous waste disposed
@router.get("/non-hazardous-waste-years", response_model=Dict)
def get_distinct_non_hazardous_waste_years(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT year 
            FROM gold.func_environment_non_hazard_waste_by_year(NULL, NULL, NULL, NULL)
            ORDER BY year ASC
        """))
        
        rows = result.fetchall()
        year = [row.year for row in rows]

        return {
            "data": year,
            "message": "Success",
            "count": len(year)
        }

    except Exception as e:
        print("Error fetching distinct year:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    
# get units for hazardous waste disposed
@router.get("/non-hazardous-waste-units", response_model=Dict)
def get_distinct_hazardous_waste_dis_units(db: Session = Depends(get_db), current_user = Depends(get_current_user_with_roles("R02", "R03", "R04"))):
    try:
        result = db.execute(text("""
            SELECT DISTINCT unit_of_measurement 
            FROM gold.func_environment_non_hazard_waste_by_year(NULL, NULL, NULL, NULL)
            ORDER BY unit_of_measurement ASC
        """))
        
        rows = result.fetchall()
        unit_of_measurement = [row.unit_of_measurement for row in rows]

        return {
            "data": unit_of_measurement,
            "message": "Success",
            "count": len(unit_of_measurement)
        }

    except Exception as e:
        print("Error fetching distinct unit_of_measurement:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# non hazardous waste key metrics
@router.get("/non-haz-waste-key-metrics", response_model=Dict)
def get_non_hazard_waste_key_metrics(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    metrics: Optional[Union[str, List[str]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    unit_of_measurement: Optional[Union[str, List[str]]] = Query(None)
):
    """
    Get hazard waste key metrics (total, average, breakdown by unit and type)
    """
    try:
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        metrics = metrics if isinstance(metrics, list) else [metrics] if metrics else None
        quarter = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        years = year if isinstance(year, list) else [year] if year else None
        unit_of_measurement = unit_of_measurement if isinstance(unit_of_measurement, list) else [unit_of_measurement] if unit_of_measurement else None

        result = db.execute(text("""
            SELECT * FROM gold.func_environment_non_hazard_waste_by_year(
                CAST(:company_ids AS VARCHAR(10)[]),
                CAST(:metrics AS VARCHAR(20)[]),
                CAST(:quarter AS VARCHAR(2)[]),
                CAST(:years AS SMALLINT[]),
                CAST(:unit_of_measurement AS VARCHAR(15)[])
            )
        """), {
            'company_ids': company_ids,
            'metrics': metrics,
            'quarter': quarter,
            'years': years,
            'unit_of_measurement': unit_of_measurement
        })

        rows = [
            {
                key: float(value) if isinstance(value, Decimal) else value
                for key, value in row._mapping.items()
            }
            for row in result
        ]

        if not rows:
            return {
                "kilograms": {
                    "total_waste": 0,
                    "average_per_year": 0,
                    "yearly_breakdown": [],
                    "metrics_average": []
                },
                "pieces": {
                    "total_waste": 0,
                    "average_per_year": 0,
                    "yearly_breakdown": [],
                    "metrics_average": []
                },
                "combined": {
                    "total_waste": 0,
                    "average_per_year": 0,
                    "most_generated_metrics": None
                }
            }

        # Separate by unit
        kg_data = [row for row in rows if row['unit_of_measurement'] == 'Kilogram']
        pieces_data = [row for row in rows if row['unit_of_measurement'] == 'Pieces']

        # Compute KG metrics
        total_kg = sum(row['total_waste'] for row in kg_data)
        kg_yearly = {}
        kg_by_type = {}

        for row in kg_data:
            kg_yearly[row['year']] = kg_yearly.get(row['year'], 0) + row['total_waste']
            kg_by_type[row['metrics']] = kg_by_type.get(row['metrics'], []) + [row['total_waste']]

        avg_kg_per_year = sum(kg_yearly.values()) / len(kg_yearly) if kg_yearly else 0
        kg_by_type_avg = [
            {"metrics": k, "average_waste": round(sum(v)/len(v), 2)} for k, v in kg_by_type.items()
        ]

        # Compute Pieces metrics
        total_pieces = sum(row['total_waste'] for row in pieces_data)
        pieces_yearly = {}
        pieces_by_type = {}

        for row in pieces_data:
            pieces_yearly[row['year']] = pieces_yearly.get(row['year'], 0) + row['total_waste']
            pieces_by_type[row['metrics']] = pieces_by_type.get(row['metrics'], []) + [row['total_waste']]

        avg_pieces_per_year = sum(pieces_yearly.values()) / len(pieces_yearly) if pieces_yearly else 0
        pieces_by_type_avg = [
            {"metrics": k, "average_waste": round(sum(v)/len(v), 2)} for k, v in pieces_by_type.items()
        ]

        # Combined Summary
        total_combined = total_kg + total_pieces
        avg_combined = avg_kg_per_year + avg_pieces_per_year

        # Most Generated Waste Type
        all_by_type = {}
        for row in rows:
            all_by_type[row['metrics']] = all_by_type.get(row['metrics'], 0) + row['total_waste']

        top_type = max(all_by_type.items(), key=lambda x: x[1]) if all_by_type else (None, 0)

        return {
            "kilograms": {
                "total_waste": round(total_kg, 2),
                "average_per_year": round(avg_kg_per_year, 2),
                "yearly_breakdown": [{"year": y, "total_waste": round(v, 2)} for y, v in sorted(kg_yearly.items())],
                "metrics_average": kg_by_type_avg
            },
            "pieces": {
                "total_waste": round(total_pieces, 2),
                "average_per_year": round(avg_pieces_per_year, 2),
                "yearly_breakdown": [{"year": y, "total_waste": round(v, 2)} for y, v in sorted(pieces_yearly.items())],
                "metrics_average": pieces_by_type_avg
            },
            "combined": {
                "total_waste": round(total_combined, 2),
                "average_per_year": round(avg_combined, 2),
                "most_generated_metrics": {
                    "metrics": top_type[0],
                    "total_waste": round(top_type[1], 2)
                }
            }
        }

    except Exception as e:
        print("Error in hazard waste disposed key metrics:", str(e))
        raise HTTPException(status_code=500, detail="Internal Server Error")

# non hazardous waste percentage pie chart (use this for Distribution of Non-Hazardous Waste Generated by Company Pie Chart)
@router.get("/non-hazard-waste-perc-pie-chart", response_model=Dict)
def get_non_hazard_waste_percentage_pie_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    metrics: Optional[Union[str, List[str]]] = Query(None),
    unit_of_measurement: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        # Convert parameters to lists if they're single values
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        metrics_list = metrics if isinstance(metrics, list) else [metrics] if metrics else None
        units = unit_of_measurement if isinstance(unit_of_measurement, list) else [unit_of_measurement] if unit_of_measurement else None
        
        # Execute the PostgreSQL function
        query = text("""
            SELECT * FROM gold.func_environment_non_hazard_waste_by_perc_lvl(
                ARRAY[:company_ids]::text[],
                ARRAY[:metrics]::text[],
                ARRAY[:quarters]::text[],
                ARRAY[:years]::smallint[],
                ARRAY[:units]::text[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "metrics": metrics_list,
            "quarters": quarters,
            "years": years,
            "units": units
        })
        
        rows = result.fetchall()
        
        if not rows:
            return {"data": [], "unit": "", "message": "No data found"}
        
        # Transform the data
        data = [{
            "company_id": r.company_id,
            "unit_of_measurement": r.unit_of_measurement,
            "total_waste": float(r.total_waste or 0)
        } for r in rows]
        
        # Calculate total waste across all companies
        total_all = sum(d["total_waste"] for d in data)
        
        if total_all == 0:
            return {"data": [], "unit": "", "message": "No waste data available"}
        
        # Get company colors from database
        company_color_result = db.execute(text("""
            SELECT company_id, company_name, color 
            FROM ref.company_main
        """))
        
        company_colors = {row.company_id: row.color for row in company_color_result.fetchall()}
        print(f"Fetched company colors: {company_colors}")
        
        # Fallback color palette for companies without assigned colors
        fallback_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
        
        def generate_color_map(company_data, company_colors_dict, fallback_palette):
            """Generate color mapping using database colors with fallback"""
            unique_companies = sorted(set(d["company_id"] for d in company_data))
            color_map = {}
            fallback_index = 0
            
            for company in unique_companies:
                # Get color from database or use fallback
                color = company_colors_dict.get(company)
                if not color:
                    color = fallback_palette[fallback_index % len(fallback_palette)]
                    fallback_index += 1
                color_map[company] = color
            
            return color_map
        
        # Generate color mapping using database colors
        color_map = generate_color_map(data, company_colors, fallback_colors)
        print(f"Final color mapping: {color_map}")
        
        # Add percentage and color to each data point
        for d in data:
            d["percentage"] = round((d["total_waste"] / total_all) * 100, 2)
            d["color"] = color_map[d["company_id"]]
        
        # Get unique units for response
        unique_units = list(sorted(set(d["unit_of_measurement"] for d in data)))
        
        return {
            "data": data,
            "unit": unique_units,
            "total_waste": round(total_all, 2),
            "message": "Success"
        }
        
    except Exception as e:
        print("Error in /non-hazard-waste-perc-pie-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# non hazardous waste metrics bar chart (use this for Total Non-Hazardous Waste by Metrics per Company Bar Chart)
@router.get("/non-hazard-waste-metrics-bar-chart", response_model=Dict)
def get_non_hazard_waste_metrics_bar_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    metrics: Optional[Union[str, List[str]]] = Query(None),
    unit_of_measurement: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        # Convert parameters to lists if they're single values
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        metrics_list = metrics if isinstance(metrics, list) else [metrics] if metrics else None
        units = unit_of_measurement if isinstance(unit_of_measurement, list) else [unit_of_measurement] if unit_of_measurement else None

        # Execute the PostgreSQL function
        query = text("""
            SELECT * FROM gold.func_environment_non_hazard_waste_by_metrics(
                ARRAY[:company_ids]::varchar(10)[], 
                ARRAY[:metrics]::varchar(20)[],
                ARRAY[:quarters]::varchar(2)[],
                ARRAY[:years]::smallint[],
                ARRAY[:units]::varchar(15)[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "metrics": metrics_list,
            "quarters": quarters,
            "years": years,
            "units": units
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "companies": [], "metrics": [], "units": [], "message": "No data found"}

        # Transform raw data
        raw_data = [{
            "company_id": r.company_id,
            "metrics": r.metrics,
            "unit_of_measurement": r.unit_of_measurement,
            "total_waste": float(r.total_waste or 0)
        } for r in rows]

        # Get unique values for chart structure
        unique_companies = sorted(set(d["company_id"] for d in raw_data))
        unique_metrics = sorted(set(d["metrics"] for d in raw_data))
        unique_units = sorted(set(d["unit_of_measurement"] for d in raw_data))

        # Generate color mapping for metrics (stacked bar segments)
        metrics_color_map = generate_unique_color_map(unique_metrics)

        # Prepare data structure for stacked bar chart
        # Each company will be a bar, each metric will be a stack segment
        chart_data = []
        
        for company in unique_companies:
            company_data = {
                "company_id": company,
                "metrics_data": [],
                "total_waste": 0
            }
            
            # Get all metrics data for this company
            company_records = [d for d in raw_data if d["company_id"] == company]
            
            for metric in unique_metrics:
                # Find the waste amount for this company-metric combination
                metric_record = next(
                    (r for r in company_records if r["metrics"] == metric), 
                    None
                )
                
                waste_amount = metric_record["total_waste"] if metric_record else 0
                
                company_data["metrics_data"].append({
                    "metrics": metric,
                    "total_waste": waste_amount,
                    "color": metrics_color_map[metric],
                    "unit_of_measurement": metric_record["unit_of_measurement"] if metric_record else ""
                })
                
                company_data["total_waste"] += waste_amount
            
            # Round the total waste
            company_data["total_waste"] = round(company_data["total_waste"], 2)
            chart_data.append(company_data)

        # Create metrics summary with colors for legend
        metrics_legend = [
            {
                "metrics": metric,
                "color": metrics_color_map[metric]
            }
            for metric in unique_metrics
        ]

        # Calculate overall totals
        grand_total = sum(d["total_waste"] for d in chart_data)

        return {
            "data": chart_data,
            "companies": unique_companies,
            "metrics": unique_metrics,
            "metrics_legend": metrics_legend,
            "units": unique_units,
            "grand_total": round(grand_total, 2),
            "message": "Success"
        }

    except Exception as e:
        print("Error in /non-hazard-waste-metrics-bar-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# non hazardous waste metrics line chart (use this for Non-Hazardous Waste by Metrics Over Time Line Chart)
@router.get("/non-hazard-waste-metrics-line-chart", response_model=Dict)
def get_non_hazard_waste_metrics_line_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    metrics: Optional[Union[str, List[str]]] = Query(None),
    unit_of_measurement: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        # Convert parameters to lists if they're single values
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        metrics_list = metrics if isinstance(metrics, list) else [metrics] if metrics else None
        units = unit_of_measurement if isinstance(unit_of_measurement, list) else [unit_of_measurement] if unit_of_measurement else None

        # Execute the PostgreSQL function
        query = text("""
            SELECT * FROM gold.func_environment_non_hazard_waste_by_year(
                ARRAY[:company_ids]::varchar(10)[], 
                ARRAY[:metrics]::varchar(20)[],
                ARRAY[:quarters]::varchar(2)[],
                ARRAY[:years]::smallint[],
                ARRAY[:units]::varchar(15)[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "metrics": metrics_list,
            "quarters": quarters,
            "years": years,
            "units": units
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "companies": [], "metrics": [], "years": [], "units": [], "message": "No data found"}

        # Transform raw data
        raw_data = [{
            "company_id": r.company_id,
            "year": int(r.year),
            "metrics": r.metrics,
            "unit_of_measurement": r.unit_of_measurement,
            "total_waste": float(r.total_waste or 0)
        } for r in rows]

        # Get unique values for chart structure
        unique_companies = sorted(set(d["company_id"] for d in raw_data))
        unique_metrics = sorted(set(d["metrics"] for d in raw_data))
        unique_years = sorted(set(d["year"] for d in raw_data))
        unique_units = sorted(set(d["unit_of_measurement"] for d in raw_data))

        # Generate color mapping for each metric (not company-metric combination)
        line_color_map = generate_unique_color_map(unique_metrics)

        # Prepare data structure for line chart
        # Each metric will be a single line combining all companies
        chart_data = []
        
        for metric in unique_metrics:
            # Create a single line for this metric, combining all companies
            line_data = {
                "metrics": metric,
                "color": line_color_map[metric],
                "data_points": [],
                "total_waste_sum": 0,
                "companies_included": []  # Track which companies contribute to this metric
            }
            
            # Create data points for each year by combining all companies
            for year in unique_years:
                # Sum waste amounts for this metric-year combination across all companies
                year_total = 0
                year_unit = ""
                companies_for_year = []
                
                for company in unique_companies:
                    data_point = next(
                        (r for r in raw_data 
                         if r["company_id"] == company and r["metrics"] == metric and r["year"] == year), 
                        None
                    )
                    
                    if data_point:
                        year_total += data_point["total_waste"]
                        year_unit = data_point["unit_of_measurement"]  # Assuming same unit for same metric
                        companies_for_year.append(company)
                
                line_data["data_points"].append({
                    "year": year,
                    "total_waste": round(year_total, 2),
                    "unit_of_measurement": year_unit,
                    "companies_contributing": companies_for_year
                })
                
                line_data["total_waste_sum"] += year_total
            
            # Get all companies that contribute to this metric
            line_data["companies_included"] = sorted(set(
                company for point in line_data["data_points"] 
                for company in point["companies_contributing"]
            ))
            
            # Round the total waste sum
            line_data["total_waste_sum"] = round(line_data["total_waste_sum"], 2)
            
            # Only include lines that have some data (not all zeros)
            if line_data["total_waste_sum"] > 0:
                chart_data.append(line_data)

        # Create legend data for the lines
        legend_data = [
            {
                "metrics": line["metrics"],
                "color": line["color"],
                "label": line["metrics"],
                "companies_included": line["companies_included"],
                "total_companies": len(line["companies_included"])
            }
            for line in chart_data
        ]

        # Calculate overall statistics
        grand_total = sum(d["total_waste_sum"] for d in chart_data)

        # Prepare summary by year for additional insights
        year_summary = []
        for year in unique_years:
            year_metrics_summary = []
            year_total = 0
            
            for metric in unique_metrics:
                metric_year_total = 0
                for company in unique_companies:
                    data_point = next(
                        (r for r in raw_data 
                         if r["company_id"] == company and r["metrics"] == metric and r["year"] == year), 
                        None
                    )
                    if data_point:
                        metric_year_total += data_point["total_waste"]
                
                if metric_year_total > 0:
                    year_metrics_summary.append({
                        "metrics": metric,
                        "total_waste": round(metric_year_total, 2)
                    })
                    year_total += metric_year_total
            
            year_summary.append({
                "year": year,
                "total_waste": round(year_total, 2),
                "metrics_breakdown": year_metrics_summary
            })

        # Add company breakdown summary for reference
        company_summary = []
        for company in unique_companies:
            company_total = sum(
                r["total_waste"] for r in raw_data 
                if r["company_id"] == company
            )
            company_metrics = sorted(set(
                r["metrics"] for r in raw_data 
                if r["company_id"] == company
            ))
            
            company_summary.append({
                "company_id": company,
                "total_waste": round(company_total, 2),
                "metrics_available": company_metrics
            })

        return {
            "data": chart_data,
            "companies": unique_companies,
            "metrics": unique_metrics,
            "years": unique_years,
            "units": unique_units,
            "legend": legend_data,
            "year_summary": year_summary,
            "company_summary": company_summary,
            "grand_total": round(grand_total, 2),
            "message": "Success"
        }

    except Exception as e:
        print("Error in /non-hazard-waste-metrics-line-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")

# non hazardous waste metrics heatmap (use this for Non-Hazardous Waste by Year and Metric Heatmap)
@router.get("/non-hazard-waste-metrics-heatmap", response_model=Dict)
def get_non_hazard_waste_metrics_heatmap(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    metrics: Optional[Union[str, List[str]]] = Query(None),
    unit_of_measurement: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        # Convert parameters to lists if they're single values
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        metrics_list = metrics if isinstance(metrics, list) else [metrics] if metrics else None
        units = unit_of_measurement if isinstance(unit_of_measurement, list) else [unit_of_measurement] if unit_of_measurement else None

        # Execute the PostgreSQL function
        query = text("""
            SELECT * FROM gold.func_environment_non_hazard_waste_by_year(
                ARRAY[:company_ids]::varchar(10)[], 
                ARRAY[:metrics]::varchar(20)[],
                ARRAY[:quarters]::varchar(2)[],
                ARRAY[:years]::smallint[],
                ARRAY[:units]::varchar(15)[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "metrics": metrics_list,
            "quarters": quarters,
            "years": years,
            "units": units
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "companies": [], "metrics": [], "years": [], "units": [], "message": "No data found"}

        # Transform raw data
        raw_data = [{
            "company_id": r.company_id,
            "year": int(r.year),
            "metrics": r.metrics,
            "unit_of_measurement": r.unit_of_measurement,
            "total_waste": float(r.total_waste or 0)
        } for r in rows]

        # Get unique values for heatmap structure
        unique_companies = sorted(set(d["company_id"] for d in raw_data))
        unique_metrics = sorted(set(d["metrics"] for d in raw_data))
        unique_years = sorted(set(d["year"] for d in raw_data))
        unique_units = sorted(set(d["unit_of_measurement"] for d in raw_data))

        # Calculate min and max values for color intensity scaling
        all_waste_values = [d["total_waste"] for d in raw_data if d["total_waste"] > 0]
        min_waste = min(all_waste_values) if all_waste_values else 0
        max_waste = max(all_waste_values) if all_waste_values else 0

        # Prepare heatmap data structure
        # X-axis: Years, Y-axis: Companies, Color intensity: Waste amount
        # Each metric will be a separate heatmap or layer
        heatmap_data = []
        
        for metric in unique_metrics:
            metric_data = {
                "metrics": metric,
                "heatmap_cells": [],
                "metric_total": 0
            }
            
            # Create cells for each company-year combination
            for company in unique_companies:
                for year in unique_years:
                    # Find the waste amount for this company-year-metric combination
                    cell_data = next(
                        (r for r in raw_data 
                         if r["company_id"] == company and r["year"] == year and r["metrics"] == metric), 
                        None
                    )
                    
                    waste_amount = cell_data["total_waste"] if cell_data else 0
                    unit = cell_data["unit_of_measurement"] if cell_data else ""
                    
                    # Calculate intensity (0-1 scale for heatmap coloring)
                    intensity = 0
                    if max_waste > 0 and waste_amount > 0:
                        intensity = (waste_amount - min_waste) / (max_waste - min_waste)
                    
                    cell = {
                        "company_id": company,
                        "year": year,
                        "total_waste": waste_amount,
                        "unit_of_measurement": unit,
                        "intensity": round(intensity, 4),  # 0-1 scale for color intensity
                        "x": unique_years.index(year),  # X position in grid
                        "y": unique_companies.index(company)  # Y position in grid
                    }
                    
                    metric_data["heatmap_cells"].append(cell)
                    metric_data["metric_total"] += waste_amount
            
            metric_data["metric_total"] = round(metric_data["metric_total"], 2)
            heatmap_data.append(metric_data)

        # Create summary statistics for each company and year
        company_totals = []
        for company in unique_companies:
            company_total = sum(
                cell["total_waste"] 
                for metric_data in heatmap_data 
                for cell in metric_data["heatmap_cells"] 
                if cell["company_id"] == company
            )
            company_totals.append({
                "company_id": company,
                "total_waste": round(company_total, 2)
            })

        year_totals = []
        for year in unique_years:
            year_total = sum(
                cell["total_waste"] 
                for metric_data in heatmap_data 
                for cell in metric_data["heatmap_cells"] 
                if cell["year"] == year
            )
            year_totals.append({
                "year": year,
                "total_waste": round(year_total, 2)
            })

        # Calculate overall statistics
        grand_total = sum(metric_data["metric_total"] for metric_data in heatmap_data)

        return {
            "data": heatmap_data,
            "companies": unique_companies,
            "metrics": unique_metrics,
            "years": unique_years,
            "units": unique_units,
            "company_totals": company_totals,
            "year_totals": year_totals,
            "scale": {
                "min_waste": round(min_waste, 2),
                "max_waste": round(max_waste, 2)
            },
            "grid_dimensions": {
                "x_axis": "years",
                "y_axis": "companies", 
                "width": len(unique_years),
                "height": len(unique_companies)
            },
            "grand_total": round(grand_total, 2),
            "message": "Success"
        }

    except Exception as e:
        print("Error in /non-hazard-waste-metrics-heatmap:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")
    
# non hazardous waste quarter bar chart (use this for Non-Hazardous Waste by Quarter Stacked Bar Chart)
@router.get("/non-hazard-waste-quarter-bar-chart", response_model=Dict)
def get_non_hazard_waste_quarter_bar_chart(
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user_with_roles("R02", "R03", "R04")),
    company_id: Optional[Union[str, List[str]]] = Query(None),
    year: Optional[Union[int, List[int]]] = Query(None),
    quarter: Optional[Union[str, List[str]]] = Query(None),
    metrics: Optional[Union[str, List[str]]] = Query(None),
    unit_of_measurement: Optional[Union[str, List[str]]] = Query(None),
):
    try:
        # Convert parameters to lists if they're single values
        company_ids = company_id if isinstance(company_id, list) else [company_id] if company_id else None
        years = year if isinstance(year, list) else [year] if year else None
        quarters = quarter if isinstance(quarter, list) else [quarter] if quarter else None
        metrics_list = metrics if isinstance(metrics, list) else [metrics] if metrics else None
        units = unit_of_measurement if isinstance(unit_of_measurement, list) else [unit_of_measurement] if unit_of_measurement else None

        # Execute the PostgreSQL function
        query = text("""
            SELECT * FROM gold.func_environment_non_hazard_waste_by_quarter(
                ARRAY[:company_ids]::varchar(10)[], 
                ARRAY[:metrics]::varchar(20)[],
                ARRAY[:quarters]::varchar(2)[],
                ARRAY[:years]::smallint[],
                ARRAY[:units]::varchar(15)[]
            )
        """)
        
        result = db.execute(query, {
            "company_ids": company_ids,
            "metrics": metrics_list,
            "quarters": quarters,
            "years": years,
            "units": units
        })

        rows = result.fetchall()
        if not rows:
            return {"data": [], "companies": [], "metrics": [], "quarters": [], "years": [], "units": [], "message": "No data found"}

        # Transform raw data
        raw_data = [{
            "company_id": r.company_id,
            "year": int(r.year),
            "quarter": r.quarter,
            "metrics": r.metrics,
            "unit_of_measurement": r.unit_of_measurement,
            "total_waste": float(r.total_waste or 0)
        } for r in rows]

        # Get unique values for chart structure
        unique_companies = sorted(set(d["company_id"] for d in raw_data))
        unique_metrics = sorted(set(d["metrics"] for d in raw_data))
        unique_quarters = sorted(set(d["quarter"] for d in raw_data), key=lambda x: ["Q1", "Q2", "Q3", "Q4"].index(x))
        unique_years = sorted(set(d["year"] for d in raw_data))
        unique_units = sorted(set(d["unit_of_measurement"] for d in raw_data))

        # Generate color mapping for metrics (stacked bar segments)
        metrics_color_map = generate_unique_color_map(unique_metrics)

        # Create quarter-year combinations for x-axis labels
        quarter_year_combinations = []
        for year in unique_years:
            for quarter in unique_quarters:
                # Check if this combination exists in the data
                if any(d["year"] == year and d["quarter"] == quarter for d in raw_data):
                    quarter_year_combinations.append(f"{quarter} {year}")

        # Prepare data structure for stacked bar chart
        # X-axis: Quarter-Year combinations, Y-axis: Total waste, Stacks: Metrics
        chart_data = []
        
        for year in unique_years:
            for quarter in unique_quarters:
                # Check if this quarter-year combination has data
                quarter_data = [d for d in raw_data if d["year"] == year and d["quarter"] == quarter]
                if not quarter_data:
                    continue
                
                bar_data = {
                    "quarter": quarter,
                    "year": year,
                    "quarter_year_label": f"{quarter} {year}",
                    "metrics_data": [],
                    "total_waste": 0
                }
                
                # Aggregate data by metrics for this quarter-year across all companies
                for metric in unique_metrics:
                    # Sum all companies' waste for this quarter-year-metric combination
                    metric_total = sum(
                        d["total_waste"] for d in quarter_data 
                        if d["metrics"] == metric
                    )
                    
                    # Get unit from any record with this metric (they should be consistent)
                    metric_unit = ""
                    metric_record = next((d for d in quarter_data if d["metrics"] == metric), None)
                    if metric_record:
                        metric_unit = metric_record["unit_of_measurement"]
                    
                    if metric_total > 0:  # Only include metrics with data
                        bar_data["metrics_data"].append({
                            "metrics": metric,
                            "total_waste": round(metric_total, 2),
                            "color": metrics_color_map[metric],
                            "unit_of_measurement": metric_unit
                        })
                        
                        bar_data["total_waste"] += metric_total
                
                # Round total waste and add to chart data if there's any data
                if bar_data["total_waste"] > 0:
                    bar_data["total_waste"] = round(bar_data["total_waste"], 2)
                    chart_data.append(bar_data)

        # Create metrics legend with colors
        metrics_legend = [
            {
                "metrics": metric,
                "color": metrics_color_map[metric]
            }
            for metric in unique_metrics
        ]

        # Calculate summary statistics
        company_summary = []
        for company in unique_companies:
            company_total = sum(
                d["total_waste"] for d in raw_data 
                if d["company_id"] == company
            )
            company_summary.append({
                "company_id": company,
                "total_waste": round(company_total, 2)
            })

        quarter_summary = []
        for bar in chart_data:
            quarter_summary.append({
                "quarter": bar["quarter"],
                "year": bar["year"],
                "quarter_year_label": bar["quarter_year_label"],
                "total_waste": bar["total_waste"]
            })

        # Calculate overall total
        grand_total = sum(bar["total_waste"] for bar in chart_data)

        return {
            "data": chart_data,
            "companies": unique_companies,
            "metrics": unique_metrics,
            "quarters": unique_quarters,
            "years": unique_years,
            "units": unique_units,
            "metrics_legend": metrics_legend,
            "company_summary": company_summary,
            "quarter_summary": quarter_summary,
            "quarter_year_labels": [bar["quarter_year_label"] for bar in chart_data],
            "grand_total": round(grand_total, 2),
            "message": "Success"
        }

    except Exception as e:
        print("Error in /non-hazard-waste-quarter-bar-chart:", str(e))
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")