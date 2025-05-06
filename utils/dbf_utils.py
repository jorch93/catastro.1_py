# -*- coding: utf-8 -*-
import os
import arcpy
import re
import json

class DBFProcessor:
    """Procesa archivos DBF del catastro"""
    
    def __init__(self, json_path="cod_catastrales.json"):
        """
        Inicializar constantes y cargar códigos catastrales
        
        Args:
            json_path (str): Ruta al archivo JSON con códigos catastrales (default: cod_catastrales.json)
        """
        # Mapeo de prefijos a datasets
        self.PREFIX_MAPPING = {
            "rA": "Rustico",
            "uA": "Urbano"
        }

        # Define allowed table types and their patterns
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
        
        # Definiciones de campos requeridos
        self.field_definitions = {
            'Nombre_Municipio': ('TEXT', 255),
            'Nombre_Isla': ('TEXT', 50),
            'Codigo_Municipal_ISTAC': ('LONG', None),
            'Codigo_Isla_INE': ('LONG', None)
        }

    def process_directory(self, input_dirs, final_gdb):
        """Proceso principal de DBFs"""
        try:
            # Set workspace globally for the class
            self.workspace = final_gdb
            arcpy.env.workspace = self.workspace
            print(f"Setting workspace to: {self.workspace}")

            for input_dir in input_dirs:
                if not os.path.exists(input_dir):
                    print(f"Warning: Directory does not exist: {input_dir}")
                    continue
                
                self._process_dbf_files(input_dir, final_gdb)
            
            # Merge all tables at the end
            print("\nMerging all processed tables...")
            self.merge_tables(final_gdb)
            
        except Exception as e:
            print(f"Error in process_directory: {str(e)}")
            raise

    def _process_dbf_files(self, input_dir, final_gdb):
        """Procesar archivos DBF encontrados siguiendo el patrón de GDBProcessor"""
        
        for root, _, files in os.walk(input_dir):
            # Filter for relevant DBF files first
            dbf_files = [f for f in files if any(re.match(pattern, f, re.IGNORECASE) 
                        for pattern in self.ALLOWED_PATTERNS.values())]
            
            if dbf_files:  # Only print if we found matching files
                print(f"Scanning folder: {root}")
                for file in dbf_files:
                    print(f"Found DBF file: {file}")
                    
                    # Extract information from filename
                    municipal_code = self._extract_municipal_code(file)
                    
                    if not municipal_code:
                        print(f"No municipal code found in: {file}")
                        continue
                    
                    print(f"Successfully found municipal code: {municipal_code}")

                    # 2. Determinar dataset y tipo de tabla
                    prefix_match = re.search(r'[ru]A', file, re.IGNORECASE)
                    if not prefix_match:
                        continue

                    prefix = prefix_match.group()  # Remove .lower() to keep original case

                    if prefix not in self.PREFIX_MAPPING:
                        continue

                    dataset = self.PREFIX_MAPPING[prefix]
                    table_type = self._extract_table_type(file)
                    if not table_type:
                        continue

                    print(f"\nProcesando: {file}")
                    print(f"- Dataset: {dataset}")
                    print(f"- Tipo: {table_type}")
                    print(f"- Código: {municipal_code}")

                    # 3. Crear tabla en GDB (not in feature dataset)
                    table_name = f"{dataset}_{table_type}_{municipal_code}"  # Added municipal code to make unique tables

                    try:
                        target_table = os.path.join(final_gdb, table_name)  # Changed to use final_gdb directly
                        
                        if arcpy.Exists(target_table):
                            print(f"- Eliminando tabla existente: {table_name}")
                            arcpy.Delete_management(target_table)
                        
                        print(f"- Convirtiendo DBF a tabla: {table_name}")
                        arcpy.TableToTable_conversion(
                            in_rows=os.path.join(root, file),
                            out_path=final_gdb,  # Changed from dataset_path to final_gdb
                            out_name=table_name
                        )

                        # Añadir y poblar campo de código municipal
                        print(f"- Añadiendo código municipal")
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

                        # 4. Añadir información adicional del JSON
                        self._add_cadastral_info(target_table, municipal_code)
                        
                        print(f"✓ Tabla completada: {table_name}")

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
            # Use optimized dictionary lookup
            code_info = self.cadastral_codes.get(str(municipal_code))
            
            if not code_info:
                print(f"No se encuentra información para código {municipal_code}")
                return

            # Añadir campos adicionales del JSON
            for field, value in code_info.items():
                if field != 'Codigo_Municipal_Catastral':
                    field_name = field[:10]  # Limitar nombre a 10 caracteres
                    arcpy.AddField_management(table, field_name, "TEXT", field_alias=field)
                    arcpy.CalculateField_management(table, field_name, f"'{value}'", "PYTHON3")

        except Exception as e:
            print(f"Error añadiendo información catastral: {str(e)}")

    def merge_tables(self, final_gdb):
        """Merge tables by type after processing"""
        try:
            # Define the final merged table names
            merged_tables = {
                "Urbano_Carvia": [],
                "Rustico_Carvia": [],
                "Rustico_RUCULTIVO": [],
                "Rustico_RUSUBPARCELA": []
            }

            # Get all tables in the geodatabase
            arcpy.env.workspace = final_gdb
            tables = arcpy.ListTables()

            # Group tables by type
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

            # Merge tables by type
            for merged_name, table_list in merged_tables.items():
                if table_list:
                    print(f"\nMerging tables for {merged_name}...")
                    output_table = os.path.join(final_gdb, merged_name)
                    
                    if arcpy.Exists(output_table):
                        print(f"Removing existing merged table: {merged_name}")
                        arcpy.Delete_management(output_table)

                    print(f"Merging {len(table_list)} tables...")
                    arcpy.Merge_management(table_list, output_table)
                    
                    print(f"Cleaning up individual tables...")
                    for table in table_list:
                        arcpy.Delete_management(table)
                    
                    print(f"✓ Successfully created {merged_name}")

        except Exception as e:
            print(f"Error merging tables: {str(e)}")
            raise