import pandas as pd
import io
from ..template.envi_template_config import TEMPLATE_DEFINITIONS


def create_excel_template(table_type: str, include_examples: bool = True) -> io.BytesIO:
    """Create Excel template for a specific table type"""
    if table_type not in TEMPLATE_DEFINITIONS:
        raise ValueError(f"Unknown table type: {table_type}")
    
    template = TEMPLATE_DEFINITIONS[table_type]
    
    # Create Excel writer object
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Create main data sheet
        df_data = pd.DataFrame(columns=template["headers"])
        
        # Add example row if requested
        if include_examples:
            example_row = {header: example for header, example in zip(template["headers"], template["examples"])}
            df_data = pd.concat([df_data, pd.DataFrame([example_row])], ignore_index=True)
        
        df_data.to_excel(writer, sheet_name=template["sheet_name"], index=False)
        
        # Create instructions sheet
        instructions_data = {
            "Column": template["headers"],
            "Data Type": template["data_types"],
            "Description": template["descriptions"],
            "Example": template["examples"]
        }
        df_instructions = pd.DataFrame(instructions_data)
        df_instructions.to_excel(writer, sheet_name="Instructions", index=False)
        
        # Format the sheets
        workbook = writer.book
        
        # Format main sheet
        main_sheet = writer.sheets[template["sheet_name"]]
        for col in range(len(template["headers"])):
            main_sheet.column_dimensions[chr(65 + col)].width = 20
        
        # Format instructions sheet
        instructions_sheet = writer.sheets["Instructions"]
        for col in range(4):  # 4 columns in instructions
            instructions_sheet.column_dimensions[chr(65 + col)].width = 25
    
    output.seek(0)
    return output


def create_all_templates() -> io.BytesIO:
    """Create a single Excel file with all templates as separate sheets"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Create overview sheet
        overview_data = {
            "Sheet Name": [],
            "Table Name": [],
            "Description": [],
            "Record Count Columns": []
        }
        
        for table_type, template in TEMPLATE_DEFINITIONS.items():
            overview_data["Sheet Name"].append(template["sheet_name"])
            overview_data["Table Name"].append(f"bronze.envi_{table_type}")
            overview_data["Description"].append(f"Template for {table_type.replace('_', ' ').title()}")
            overview_data["Record Count Columns"].append(len(template["headers"]))
            
            # Create individual template sheet
            df_data = pd.DataFrame(columns=template["headers"])
            
            # Add example row
            example_row = {header: example for header, example in zip(template["headers"], template["examples"])}
            df_data = pd.concat([df_data, pd.DataFrame([example_row])], ignore_index=True)
            
            df_data.to_excel(writer, sheet_name=template["sheet_name"], index=False)
            
            # Format sheet
            sheet = writer.sheets[template["sheet_name"]]
            for col in range(len(template["headers"])):
                sheet.column_dimensions[chr(65 + col)].width = 20
        
        # Add overview sheet
        df_overview = pd.DataFrame(overview_data)
        df_overview.to_excel(writer, sheet_name="Overview", index=False)
        
        # Format overview sheet
        overview_sheet = writer.sheets["Overview"]
        for col in range(4):
            overview_sheet.column_dimensions[chr(65 + col)].width = 25
    
    output.seek(0)
    return output


def get_table_mapping():
    """Get mapping of table names to template keys"""
    return {
        "envi_company_property": "company_property",
        "envi_water_abstraction": "water_abstraction",
        "envi_water_discharge": "water_discharge",
        "envi_water_consumption": "water_consumption",
        "envi_diesel_consumption": "diesel_consumption",
        "envi_electric_consumption": "electric_consumption",
        "envi_non_hazard_waste": "non_hazard_waste",
        "envi_hazard_waste_generated": "hazard_waste_generated",
        "envi_hazard_waste_disposed": "hazard_waste_disposed"
    }