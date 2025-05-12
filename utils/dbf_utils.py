# -*- coding: utf-8 -*-
import os
import arcpy
import re
import json

class DBFProcessor:
    """
    Clase para procesar archivos DBF del catastro y convertirlos a tablas en geodatabase.
    
    Funcionalidades:
    - Procesa archivos DBF rústicos y urbanos
    - Extrae códigos municipales de los nombres
    - Añade información catastral desde JSON
    - Fusiona tablas por tipo al final del proceso
    """
    
    def __init__(self, json_path="cod_catastrales.json"):
        """
        Inicialización con constantes y configuración.
        
        Args:
            json_path (str): Ruta al archivo JSON con códigos catastrales
        
        Atributos:
            PREFIX_MAPPING (dict): Mapeo de prefijos a tipos de dataset
            ALLOWED_PATTERNS (dict): Patrones permitidos para tipos de tabla
            CODE_FIELD (str): Nombre del campo para código municipal
            workspace (str): Espacio de trabajo actual
            cadastral_codes (dict): Códigos catastrales cargados del JSON
        """
        # Mapeo de prefijos
        self.PREFIX_MAPPING = {
            "rA": "Rustico",
            "uA": "Urbano"
        }

        # Definición de patrones permitidos
        self.ALLOWED_PATTERNS = {
            "Carvia": r".*_Carvia\.dbf$",
            "RUCULTIVO": r".*_RUCULTIVO\.dbf$",
            "RUSUBPARCELA": r".*_RUSUBPARCELA\.dbf$"
        }

        self.CODE_FIELD = "COD_MUNI"
        self.workspace = None
        
        # Resolver ruta al JSON si es relativa
        if not os.path.isabs(json_path):
            json_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                "cod_catastrales.json"
            )

        if not os.path.exists(json_path):
            raise FileNotFoundError(f"No se encuentra el archivo de códigos catastrales en: {json_path}")
        
        # Cargar códigos catastrales
        with open(json_path, 'r', encoding='utf-8') as f:
            json_array = json.load(f)
            self.cadastral_codes = {
                str(item['Codigo_Municipal_Catastral']): item
                for item in json_array  
            }

        # Definición de campos a añadir
        self.field_definitions = {
            'Nombre_Municipio': ('TEXT', 255, 'Nombre del Municipio'),
            'Nombre_Isla': ('TEXT', 50, 'Nombre de la Isla'),
            'Codigo_Municipal_ISTAC': ('LONG', None, 'Código ISTAC'),
            'Codigo_Isla_INE': ('LONG', None, 'Código INE Isla')
        }

    def process_directory(self, input_dirs, final_gdb):
        """
        Proceso principal de procesamiento de DBFs.
        
        Args:
            input_dirs (list): Lista de directorios de entrada
            final_gdb (str): Ruta a la geodatabase final
        
        Proceso:
        1. Configura el espacio de trabajo
        2. Procesa cada directorio de entrada
        3. Fusiona todas las tablas al final
        """
        try:
            # Establecer el espacio de trabajo
            self.workspace = final_gdb
            arcpy.env.workspace = self.workspace

            for input_dir in input_dirs:
                if not os.path.exists(input_dir):
                    print(f"Warning: Directory does not exist: {input_dir}")
                    continue
                
                self._process_dbf_files(input_dir, final_gdb)
            
            # Hacer merge de todas las tablas procesadas
            self.merge_tables(final_gdb)
            
        except Exception as e:
            print(f"Error in process_directory: {str(e)}")
            raise

    def _process_dbf_files(self, input_dir, final_gdb):
        """
        Procesa archivos DBF encontrados en un directorio.
        
        Args:
            input_dir (str): Directorio con archivos DBF
            final_gdb (str): Geodatabase donde guardar las tablas
        
        Proceso:
        1. Filtra archivos DBF relevantes
        2. Extrae información del nombre
        3. Crea y puebla tablas en la geodatabase
        4. Añade información catastral adicional
        """
        for root, _, files in os.walk(input_dir):
            # Filtrar archivos DBF relevantes
            dbf_files = [f for f in files if any(re.match(pattern, f, re.IGNORECASE) 
                        for pattern in self.ALLOWED_PATTERNS.values())]
            
            if dbf_files:
                for file in dbf_files:
                    
                    # Extraer código municipal
                    municipal_code = self._extract_municipal_code(file)
                    
                    if not municipal_code:
                        print(f"No municipal code found in: {file}")
                        continue

                    # Determinar dataset y tipo de tabla
                    prefix_match = re.search(r'[ru]A', file, re.IGNORECASE)
                    if not prefix_match:
                        continue

                    prefix = prefix_match.group()

                    if prefix not in self.PREFIX_MAPPING:
                        continue

                    dataset = self.PREFIX_MAPPING[prefix]
                    table_type = self._extract_table_type(file)
                    if not table_type:
                        continue

                    # Crear tabla en GDB
                    table_name = f"{dataset}_{table_type}_{municipal_code}"

                    try:
                        target_table = os.path.join(final_gdb, table_name)
                        
                        if arcpy.Exists(target_table):
                            arcpy.Delete_management(target_table)
                        
                        arcpy.TableToTable_conversion(
                            in_rows=os.path.join(root, file),
                            out_path=final_gdb,
                            out_name=table_name
                        )

                        # Añadir y poblar campo de código municipal
                        arcpy.AddField_management(
                            target_table, 
                            self.CODE_FIELD, 
                            "LONG", 
                            field_alias="Código Municipal Catastral"
                        )
                        
                        with arcpy.da.UpdateCursor(target_table, [self.CODE_FIELD]) as cursor:
                            for row in cursor:
                                row[0] = municipal_code
                                cursor.updateRow(row)

                        # Añadir información adicional del JSON
                        self._add_cadastral_info(target_table, municipal_code)

                    except Exception as e:
                        print(f"Error procesando tabla {file}: {str(e)}")
                        continue

    def _extract_municipal_code(self, filename):
        """Extraer código municipal del nombre"""
        pattern = re.compile(r"^(\d+)")
        match = pattern.match(filename)
        return int(match.group(1)) if match else None

    def _extract_table_type(self, filename):
        """Extraer tipo de tabla del nombre"""
        for table_type, pattern in self.ALLOWED_PATTERNS.items():
            if re.match(pattern, filename, re.IGNORECASE):
                return table_type
        return None

    def _add_cadastral_info(self, table, municipal_code):
        """Añadir información del JSON de códigos catastrales"""
        try:
            code_info = self.cadastral_codes.get(str(municipal_code))
            
            if not code_info:
                print(f"No se encuentra información para código {municipal_code}")
                return

            # Añadir campos usando las definiciones
            for field_name, (field_type, field_length, field_alias) in self.field_definitions.items():
                try:
                    arcpy.AddField_management(
                        table,
                        field_name[:10],  # Limitar nombre a 10 caracteres
                        field_type,
                        field_length=field_length,
                        field_alias=field_alias
                    )
                    
                    # Poblar campo si existe en el JSON
                    if field_name in code_info:
                        value = code_info[field_name]
                        expression = f"'{value}'" if field_type == 'TEXT' else str(value)
                        arcpy.CalculateField_management(
                            table, 
                            field_name[:10],
                            expression,
                            "PYTHON3"
                        )
                
                except Exception as e:
                    print(f"Error añadiendo campo {field_name}: {str(e)}")

        except Exception as e:
            print(f"Error añadiendo información catastral: {str(e)}")

    def merge_tables(self, final_gdb):
        """
        Fusiona tablas por tipo después del procesamiento.
        
        Args:
            final_gdb (str): Geodatabase con las tablas a fusionar
        
        Proceso:
        1. Agrupa tablas por tipo
        2. Fusiona cada grupo en una tabla final
        3. Limpia tablas individuales
        4. Crea tablas finales:
           - Urbano_Carvia
           - Rustico_Carvia
           - Rustico_RUCULTIVO
           - Rustico_RUSUBPARCELA
        """
        try:
            # Definir grupos de tablas para el merge
            merged_tables = {
                "Urbano_Carvia": [],
                "Rustico_Carvia": [],
                "Rustico_RUCULTIVO": [],
                "Rustico_RUSUBPARCELA": []
            }

            # Extraer todas las tablas de la geodatabase
            arcpy.env.workspace = final_gdb
            tables = arcpy.ListTables()

            # Agrupar tablas por tipo
            for table in tables:
                if "_Carvia_" in table:
                    if "Rustico" in table:
                        merged_tables["Rustico_Carvia"].append(os.path.join(final_gdb, table))
                    else:
                        merged_tables["Urbano_Carvia"].append(os.path.join(final_gdb, table))
                elif "_RUCULTIVO_" in table:
                    merged_tables["Rustico_RUCULTIVO"].append(os.path.join(final_gdb, table))
                elif "_RUSUBPARCELA_" in table:
                    merged_tables["Rustico_RUSUBPARCELA"].append(os.path.join(final_gdb, table))

            # Merge tablas por tipo
            for merged_name, table_list in merged_tables.items():
                if table_list:
                    output_table = os.path.join(final_gdb, merged_name)
                    
                    if arcpy.Exists(output_table):
                        arcpy.Delete_management(output_table)

                    arcpy.Merge_management(table_list, output_table)
                    
                    for table in table_list:
                        arcpy.Delete_management(table)

        except Exception as e:
            print(f"Error merging tables: {str(e)}")
            raise