import arcpy
import json
import os

class CadastralInfoManager:
    def __init__(self, workspace, json_path):
        """
        Inicializa el gestor de información catastral.

        Args:
            workspace (str): Ruta de la geodatabase de trabajo
            json_path (str): Ruta al archivo JSON con códigos catastrales
        """
        self.workspace = workspace
        arcpy.env.workspace = workspace

        # Resolver ruta al JSON si es relativa
        if not os.path.isabs(json_path):
            json_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),  # Subir un nivel desde utils
                json_path
            )
    
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"No se encuentra el archivo JSON en: {json_path}")
        
        # Sube los códigos catastrales desde el JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            json_array = json.load(f)
            # Convertir JSON a diccionario para acceso rápido
            self.cadastral_codes = {
                str(item['Codigo_Municipal_Catastral']): item
                for item in json_array  
            }
        
        # Definiciones de campos requeridos
        self.field_definitions = {
            'Nombre_Municipio': ('TEXT', 255),
            'Nombre_Isla': ('TEXT', 50),
            'Codigo_Municipal_ISTAC': ('LONG', None),
            'Codigo_Isla_INE': ('LONG', None)
        }

    def manage_fields(self, feature_class):
        """
        Gestiona los campos requeridos en una feature class.

        Args:
            feature_class (str): Ruta de la feature class a procesar

        Proceso:
        1. Lista campos existentes
        2. Elimina campos definidos si existen
        3. Añade nuevos campos con tipos específicos
        """
        existing_fields = [field.name for field in arcpy.ListFields(feature_class)]
        
        # Borrar campos existentes que están en la definición
        for field in existing_fields:
            if field in self.field_definitions:
                arcpy.DeleteField_management(feature_class, field)

        # Agregar nuevos campos
        for field_name, (field_type, field_length) in self.field_definitions.items():
            arcpy.AddField_management(
                feature_class, 
                field_name, 
                field_type, 
                field_length=field_length
            )

    def update_cadastral_info(self, feature_class):
        """
        Actualiza información catastral en una feature class.

        Args:
            feature_class (str): Feature class a actualizar

        Proceso:
        1. Configura cursor de actualización
        2. Mapea índices de campos
        3. Actualiza registros con información del JSON
        4. Reporta número de actualizaciones
        """
        try:
            # Adquirir lookup de campos nuevos a actualizar
            fields = ['Codigo_Municipal_Catastral'] + list(self.field_definitions.keys())
            
            # Adquirir campos que serán actualizados
            with arcpy.da.UpdateCursor(feature_class, fields) as cursor:
                field_indices = {field: i for i, field in enumerate(fields)}
                
                updates = 0
                for row in cursor:
                    municipal_code = str(row[field_indices['Codigo_Municipal_Catastral']])
                    
                    if municipal_code in self.cadastral_codes:
                        info = self.cadastral_codes[municipal_code]
                        # Actualizar campos con información del JSON
                        row[field_indices['Nombre_Municipio']] = info.get('Nombre_Municipio', '')
                        row[field_indices['Nombre_Isla']] = info.get('Nombre_Isla', '')
                        row[field_indices['Codigo_Municipal_ISTAC']] = info.get('Codigo_Municipal_ISTAC', 0)
                        row[field_indices['Codigo_Isla_INE']] = info.get('Codigo_Isla_INE', 0)
                        cursor.updateRow(row)
                        updates += 1
                
        except Exception as e:
            print(f"Error updating {feature_class}: {str(e)}")

    def process_feature_classes(self, feature_types, dataset_prefix):
        """
        Procesa múltiples feature classes.

        Args:
            feature_types (list): Tipos de features a procesar
            dataset_prefix (str): Prefijo del dataset (Rustico/Urbano)

        Proceso:
        1. Itera sobre tipos de features
        2. Verifica existencia
        3. Gestiona campos
        4. Actualiza información
        """
        for feature_type in feature_types:
            feature_class = f"{self.workspace}\\{dataset_prefix}\\{dataset_prefix}_{feature_type}"
            
            if arcpy.Exists(feature_class):
                self.manage_fields(feature_class)
                self.update_cadastral_info(feature_class)
            else:
                print(f"Feature class not found: {feature_class}")