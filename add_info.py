import arcpy
import json
import os

class CadastralInfoManager:
    def __init__(self, workspace, json_path):
        self.workspace = workspace
        arcpy.env.workspace = workspace
        
        # Load cadastral codes from JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            json_array = json.load(f)
            # Convert JSON array to dictionary for faster lookups
            self.cadastral_codes = {
                str(item['Codigo_Municipal_Catastral']): item
                for item in json_array  
            }
        
        print(f"Loaded {len(self.cadastral_codes)} municipal codes")
        
        # Define field mappings
        self.field_definitions = {
            'Nombre_Municipio': ('TEXT', 255),
            'Nombre_Isla': ('TEXT', 50),
            'Codigo_Municipal_ISTAC': ('LONG', None),
            'Codigo_Isla_INE': ('LONG', None)
        }

    def manage_fields(self, feature_class):
        """Create or update required fields"""
        existing_fields = [field.name for field in arcpy.ListFields(feature_class)]
        
        # Remove fields defined in field_definitions
        for field in existing_fields:
            if field in self.field_definitions:
                arcpy.DeleteField_management(feature_class, field)
                print(f"Deleted field: {field}")

        # Add new fields
        for field_name, (field_type, field_length) in self.field_definitions.items():
            arcpy.AddField_management(
                feature_class, 
                field_name, 
                field_type, 
                field_length=field_length
            )
            print(f"Added field: {field_name}")

    def update_cadastral_info(self, feature_class):
        """Update feature class with cadastral information"""
        try:
            # Get all fields including the lookup field and our new fields
            fields = ['Codigo_Municipal_Catastral'] + list(self.field_definitions.keys())
            
            print(f"\nUpdating fields for: {os.path.basename(feature_class)}")
            
            # Get field indices for updating
            with arcpy.da.UpdateCursor(feature_class, fields) as cursor:
                field_indices = {field: i for i, field in enumerate(fields)}
                print(f"Field positions: {field_indices}")
                
                updates = 0
                for row in cursor:
                    municipal_code = str(row[field_indices['Codigo_Municipal_Catastral']])
                    
                    if municipal_code in self.cadastral_codes:
                        info = self.cadastral_codes[municipal_code]
                        # Update using field names instead of positions
                        row[field_indices['Nombre_Municipio']] = info.get('Nombre_Municipio', '')
                        row[field_indices['Nombre_Isla']] = info.get('Nombre_Isla', '')
                        row[field_indices['Codigo_Municipal_ISTAC']] = info.get('Codigo_Municipal_ISTAC', 0)
                        row[field_indices['Codigo_Isla_INE']] = info.get('Codigo_Isla_INE', 0)
                        cursor.updateRow(row)
                        updates += 1
                
                print(f"Updated {updates} rows")
                
        except Exception as e:
            print(f"Error updating {feature_class}: {str(e)}")

    def process_feature_classes(self, feature_types, dataset_prefix):
        """Process multiple feature classes"""
        for feature_type in feature_types:
            feature_class = f"{self.workspace}\\{dataset_prefix}\\{dataset_prefix}_{feature_type}"
            
            if arcpy.Exists(feature_class):
                print(f"\nProcessing: {feature_class}")
                self.manage_fields(feature_class)
                self.update_cadastral_info(feature_class)
            else:
                print(f"Feature class not found: {feature_class}")