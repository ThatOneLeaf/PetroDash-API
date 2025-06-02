# Template definitions with headers, data types, and example values
TEMPLATE_DEFINITIONS = {
    "company_property": {
        "filename": "company_property_template.xlsx",
        "sheet_name": "Company Property",
        "headers": ["company_id", "cp_name", "cp_type"],
        "data_types": ["VRCHAR(10)", "VARCHAR(30)", "VARCHAR(15)"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "company property (ex: Generator Set, Grass Cutter, etc.)", "company property type (ex: Equipment, Vehicle)"],
        "descriptions": [
            "Referenced Company ID",
            "Property Name",
            "Type: Equipment or Vehicle"
        ]
    },
    "water_abstraction": {
        "filename": "water_abstraction_template.xlsx",
        "sheet_name": "Water Abstraction",
        "headers": ["company_id", "year", "month", "quarter", "volume", "unit_of_measurement"],
        "data_types": ["VARCHAR(10)", "SMALLINT", "VARCHAR(10)", "VARCHAR(2)", "DOUBLE", "VARCHAR(15)"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "year (ex: 2023)", "month name (ex: January, February)", "quarter (ex: Q1, Q2, Q3, Q4)", "water volume (ex: 1250.50)", "unit of measurement (ex: cubic meter)"],
        "descriptions": [
            "Referenced Company ID",
            "Year (YYYY)",
            "Month name",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Volume (decimal allowed)",
            "Unit of measurement"
        ]
    },
    "water_discharge": {
        "filename": "water_discharge_template.xlsx",
        "sheet_name": "Water Discharge",
        "headers": ["company_id", "year", "quarter", "volume", "unit_of_measurement"],
        "data_types": ["VARCHAR(10)", "SMALLINT", "VARCHAR(2)", "DOUBLE", "VARCHAR(15)"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "year (ex: 2023)", "month name (ex: January, February)", "quarter (ex: Q1, Q2, Q3, Q4)", "water volume (ex: 1250.50)", "unit of measurement (ex: cubic meter)"],
        "descriptions": [
            "Referenced Company ID",
            "Year (YYYY)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Volume (decimal allowed)",
            "Unit of measurement"
        ]
    },
    "water_consumption": {
        "filename": "water_consumption_template.xlsx",
        "sheet_name": "Water Consumption",
        "headers": ["company_id", "year", "quarter", "volume", "unit_of_measurement"],
        "data_types": ["VARCHAR(10)", "SMALLINT", "VARCHAR(2)", "DOUBLE", "VARCHAR(15)"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "year (ex: 2023)", "month name (ex: January, February)", "quarter (ex: Q1, Q2, Q3, Q4)", "water volume in cubic meter (ex: 1250.50)", "unit of measurement (ex: cubic meter)"],
        "descriptions": [
            "Referenced Company ID",
            "Year (YYYY)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Volume (decimal allowed)",
            "Unit of measurement"
        ]
    },
    "diesel_consumption": {
        "filename": "diesel_consumption_template.xlsx",
        "sheet_name": "Diesel Consumption",
        "headers": ["company_id", "cp_id", "unit_of_measurement", "consumption", "date"],
        "data_types": ["VARCHAR(10)", "VARCHAR(20)", "VARCHAR(15)", "DOUBLE", "DATE"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "company property (ex: Generator Set, Grass Cutter, etc.)", "unit of measurement (ex: Liter)", "diesel consumption in liter (ex: 234.789)", "date (YYYY-MM-DD)"],
        "descriptions": [
            "Referenced Company ID",
            "Referenced Company Property ID",
            "Unit of measurement",
            "Consumption amount (decimal allowed)",
            "Date (YYYY-MM-DD)"
        ]
    },
    "electric_consumption": {
        "filename": "electric_consumption_template.xlsx",
        "sheet_name": "Electric Consumption",
        "headers": ["company_id", "source", "unit_of_measurement", "consumption", "quarter", "year"],
        "data_types": ["VARCHAR(10)", "VARCHAR(20)", "VARCHAR(15)", "DOUBLE", "VARCHAR(2)", "SMALLINT"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "source (ex: Control Building, Logistics Station)", "unit of measurement (ex: kWh)", "electricity consumption in kWh (ex: 1234.56)", "quarter (ex: Q1, Q2, Q3, Q4)", "year (ex: 2024)"],
        "descriptions": [
            "Referenced Company ID",
            "Source location",
            "Unit of measurement",
            "Consumption amount (decimal allowed)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Year (YYYY)"
        ]
    },
    "non_hazard_waste": {
        "filename": "non_hazard_waste_template.xlsx",
        "sheet_name": "Non-Hazard Waste",
        "headers": ["company_id", "metrics", "unit_of_measurement", "waste", "month", "quarter", "year"],
        "data_types": ["VARCHAR(10)", "VARCHAR(50)", "VARCHAR(15)", "DOUBLE", "VARCHAR(10)", "VARCHAR(2)", "SMALLINT"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "metrics (ex: Residual, Scrap Metal, PET Bottles, Food, Compostable, etc.)", "unit of measurement (ex: Kilogram, Pieces)", "non-hazardous waste generated (ex: 125.50)", "month name (ex: January, February)", "quarter (ex: Q1, Q2, Q3, Q4)", "year (ex: 2024)"],
        "descriptions": [
            "Referenced Company ID",
            "Waste metrics/type",
            "Unit of measurement",
            "Waste amount (decimal allowed)",
            "Month name",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Year (YYYY)"
        ]
    },
    "hazard_waste_generated": {
        "filename": "hazard_waste_generated_template.xlsx",
        "sheet_name": "Hazard Waste Generated",
        "headers": ["company_id", "metrics", "unit_of_measurement", "waste_generated", "quarter", "year"],
        "data_types": ["VARCHAR(10)", "VARCHAR(50)", "VARCHAR(15)", "DOUBLE", "VARCHAR(2)", "SMALLINT"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "metrics (ex: Battery, Used Oil, Electronic Waste, etc.)", "unit of measurement (ex: Kilogram, Ton, Liter)", "hazardous waste generated (ex: 89.25)", "month name (ex: January, February)", "year (ex: 2024)"],
        "descriptions": [
            "Referenced Company ID",
            "Waste metrics/type",
            "Unit of measurement",
            "Waste generated amount (decimal allowed)",
            "Quarter (Q1, Q2, Q3, Q4)",
            "Year (YYYY)"
        ]
    },
    "hazard_waste_disposed": {
        "filename": "hazard_waste_disposed_template.xlsx",
        "sheet_name": "Hazard Waste Disposed",
        "headers": ["company_id", "metrics", "unit_of_measurement", "waste_disposed", "year"],
        "data_types": ["VARCHAR(10)", "VARCHAR(50)", "VARCHAR(15)", "DOUBLE", "SMALLINT"],
        "examples": ["company id (ex: PSC, MGI, PWEI, etc.)", "metrics (ex: Battery, Used Oil, Electronic Waste, etc.)", "unit of measurement (ex: Kilogram, Ton, Liter)", "hazardous waste generated (ex: 89.25)", "year (ex: 2024)"],
        "descriptions": [
            "Referenced Company ID",
            "Waste metrics/type",
            "Unit of measurement",
            "Waste disposed amount (decimal allowed)",
            "Year (YYYY)"
        ]
    }
}