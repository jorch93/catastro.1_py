# -*- coding: utf-8 -*-
import os
import arcpy
import re
import shutil
import tempfile

class DBFProcessor:
    """
    Clase para procesar y consolidar tablas DBF con las siguientes capacidades:
    - Consolidación de tablas por sufijo
    - Adición automática de códigos municipales
    - Manejo seguro de archivos temporales
    - Normalización de campos
    """

    def __init__(self):
        """
        Inicialización con constantes y configuración
        
        Atributos:
            MUNICIPAL_CODE_FIELD (str): Nombre del campo de código municipal
            SUFFIXES (list): Lista de sufijos a procesar
        """
        # Campo temporal de 8 caracteres (cumple límite de 10)
        self.TEMP_FIELD = "COD_MUNI"
        
        # Campo final (se usará solo después de la conversión a GDB)
        self.MUNICIPAL_CODE_FIELD = "Codigo_Municipal_Catastral"
        
        self.SUFFIX_MAPPING = {
            "Rustico": ["rA_Carvia", "rA_RUCULTIVO", "rA_RUSUBPARCELA"],
            "Urbano": ["uA_Carvia"]
        }

    def _consolidate_tables(self, tables, suffix, final_gdb, dataset):
        """
        Consolida tablas DBF en dataset específico.
        
        Args:
            tables (list): Lista de tablas a procesar
            suffix (str): Sufijo de las tablas
            final_gdb (str): Ruta de la geodatabase
            dataset (str): Nombre del dataset (Rustico/Urbano)
        """
        if not tables:
            return
            
        temp_dir = tempfile.mkdtemp(prefix="dbf_temp_")
        try:
            first_table = tables[0]
            dataset_path = os.path.join(final_gdb, dataset)
            output_table = os.path.join(dataset_path, f"{dataset}_{suffix}")
            
            print(f"\nProcesando tablas {suffix} en {dataset}:")
            print(f"- Tabla inicial: {first_table[0]}")
            
            # Procesar primera tabla
            modified_table = self._process_table(first_table[0], temp_dir, first_table[2])
            if not modified_table or not os.path.exists(modified_table):
                print(f"Error: No se pudo procesar {first_table[0]}")
                return
                
            # Crear tabla en GDB
            if arcpy.Exists(output_table):
                arcpy.Delete_management(output_table)
                
            print(f"- Convirtiendo a GDB: {os.path.basename(modified_table)}")
            try:
                arcpy.conversion.TableToGeodatabase(modified_table, dataset_path)
                # Renombrar si es necesario
                temp_name = os.path.join(dataset_path, os.path.splitext(os.path.basename(modified_table))[0])
                if arcpy.Exists(temp_name) and temp_name != output_table:
                    arcpy.Rename_management(temp_name, output_table)
            except arcpy.ExecuteError as e:
                print(f"Error en conversión:")
                print(f"- Input: {modified_table}")
                print(f"- Output: {output_table}")
                print(f"- Error: {str(e)}")
                raise

            # Procesar tablas adicionales
            if len(tables) > 1:
                modified_tables = []
                for table, _, code in tables[1:]:
                    processed = self._process_table(table, temp_dir, code)
                    if processed and os.path.exists(processed):
                        modified_tables.append(processed)
                
                if modified_tables:
                    arcpy.Append_management(
                        inputs=modified_tables,
                        target=output_table,
                        schema_type="TEST"
                    )
                    
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def process_directory(self, input_dirs, final_gdb):
        try:
            for dataset, input_dir in zip(["Rustico", "Urbano"], input_dirs):
                print(f"\nProcesando {dataset}:")
                results = {suffix: [] for suffix in self.SUFFIX_MAPPING[dataset]}
                
                # Encontrar archivos
                current_results = self._find_dbf_by_suffix(input_dir, dataset)
                for suffix in self.SUFFIX_MAPPING[dataset]:
                    if current_results[suffix]:
                        print(f"- Encontradas {len(current_results[suffix])} tablas {suffix}")
                        self._consolidate_tables(
                            current_results[suffix], 
                            suffix, 
                            final_gdb,
                            dataset
                        )
            
            self._normalize_fields(final_gdb)
            
        except Exception as e:
            print(f"Error en proceso principal: {str(e)}")
            raise

    def _find_dbf_by_suffix(self, directory, dataset):
        """
        Encuentra archivos DBF por sufijo recursivamente en subdirectorios.
        
        Args:
            directory (str): Directorio raíz a examinar
            dataset (str): Tipo de dataset (Rustico/Urbano)
        
        Returns:
            dict: Diccionario con sufijos como claves y listas de tuplas (ruta, nombre, código)
        """
        # Inicializar resultados con sufijos del dataset específico
        results = {suffix: [] for suffix in self.SUFFIX_MAPPING[dataset]}
        
        if not os.path.exists(directory):
            print(f"Error: Directorio no existe: {directory}")
            return results
            
        print(f"\nBuscando en {dataset}:")
        print(f"Directorio raíz: {directory}")
        
        # Recorrer subdirectorios
        for root, dirs, files in os.walk(directory):
            print(f"\nEscaneando subdirectorio: {os.path.basename(root)}")
            print(f"- Subdirectorios encontrados: {len(dirs)}")
            print(f"- Archivos encontrados: {len(files)}")
            
            for file in files:
                if not file.lower().endswith('.dbf'):
                    continue
                    
                file_path = os.path.join(root, file)
                base_name = os.path.splitext(file)[0]
                municipal_code = self._extract_municipal_code(file)
                
                if municipal_code is None:
                    print(f"- Ignorando {file} (sin código municipal)")
                    continue
                
                # Verificar sufijos del dataset actual
                for suffix in self.SUFFIX_MAPPING[dataset]:
                    if base_name.lower().endswith(suffix.lower()):
                        results[suffix].append((file_path, base_name, municipal_code))
                        print(f"✓ DBF válido: {file}")
                        print(f"  Código: {municipal_code}, Sufijo: {suffix}")
                        break
        
        # Resumen
        found_files = sum(len(files) for files in results.values())
        print(f"\nResumen de {dataset}:")
        for suffix, files in results.items():
            if files:
                print(f"- {suffix}: {len(files)} archivos")
        print(f"Total: {found_files} archivos DBF válidos")
        
        return results

    def _extract_municipal_code(self, filename):
        """
        Extrae el código municipal del nombre del archivo.
        
        Args:
            filename (str): Nombre del archivo
            
        Returns:
            int: Código municipal o None si no se encuentra
        """
        pattern = re.compile(r"^(\d+)")
        match = pattern.match(filename)
        
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    def _process_table(self, table_path, temp_dir, municipal_code):
        """Procesa una tabla individual."""
        try:
            output_path = os.path.join(temp_dir, os.path.basename(table_path))
            shutil.copy2(table_path, output_path)
            
            # Verificar campo temporal existente
            field_names = [f.name for f in arcpy.ListFields(output_path)]
            if self.TEMP_FIELD not in field_names:
                print(f"- Añadiendo campo temporal {self.TEMP_FIELD}")
                try:
                    arcpy.AddField_management(
                        in_table=output_path,
                        field_name=self.TEMP_FIELD,  # Using 8-char field name
                        field_type="LONG",
                        field_alias="Código Municipal Catastral"
                    )
                except arcpy.ExecuteError as e:
                    print(f"Error añadiendo campo: {str(e)}")
                    return None
            
            # Actualizar valores
            print(f"- Actualizando código municipal: {municipal_code}")
            with arcpy.da.UpdateCursor(output_path, [self.TEMP_FIELD]) as cursor:
                for row in cursor:
                    row[0] = municipal_code
                    cursor.updateRow(row)
                    
            return output_path
            
        except Exception as e:
            print(f"Error procesando tabla {table_path}: {str(e)}")
            return None

    def _manage_field(self, table, field_name, field_type="LONG", overwrite=True):
        """Gestiona la creación o modificación de campos."""
        if overwrite and field_name in [f.name for f in arcpy.ListFields(table)]:
            arcpy.DeleteField_management(table, field_name)
        if not arcpy.ListFields(table, field_name):
            arcpy.AddField_management(
                in_table=table,
                field_name=field_name,
                field_type=field_type,
                field_alias="Código Municipal Catastral" if field_name == self.TEMP_FIELD else field_name
            )

    def _rename_field(self, table, old_name, new_name):
        """
        Renombra un campo en una tabla.
        
        Args:
            table (str): Nombre de la tabla
            old_name (str): Nombre actual del campo
            new_name (str): Nuevo nombre del campo
        """
        try:
            self._manage_field(table, new_name)
            arcpy.CalculateField_management(table, new_name, f"!{old_name}!", "PYTHON3")
            arcpy.DeleteField_management(table, old_name)
        except Exception as e:
            print(f"Error renombrando campo {old_name} a {new_name}: {str(e)}")

    def _normalize_fields(self, final_gdb):
        """
        Normaliza los campos en todas las tablas de la geodatabase.
        
        Args:
            final_gdb (str): Ruta de la geodatabase final
        """
        arcpy.env.workspace = final_gdb
        
        for table in arcpy.ListTables():
            try:
                print(f"\nProcesando tabla: {table}")
                table_path = os.path.join(final_gdb, table)
                
                # Verificar si existe el campo temporal
                if "COD_MUNI" in [f.name for f in arcpy.ListFields(table_path)]:
                    print(f"1. Creando campo '{self.MUNICIPAL_CODE_FIELD}'...")
                    arcpy.AddField_management(
                        in_table=table_path,
                        field_name=self.MUNICIPAL_CODE_FIELD,
                        field_type="LONG",
                        field_alias="Código Municipal Catastral"
                    )
                    
                    print(f"2. Copiando valores desde 'COD_MUNI'...")
                    arcpy.CalculateField_management(
                        in_table=table_path,
                        field=self.MUNICIPAL_CODE_FIELD,
                        expression="!COD_MUNI!",
                        expression_type="PYTHON3"
                    )
                    
                    print(f"3. Eliminando campo temporal 'COD_MUNI'...")
                    arcpy.DeleteField_management(
                        in_table=table_path,
                        drop_field="COD_MUNI"
                    )
                    
                    print(f"✓ Campo normalizado en {table}")
                    
            except Exception as e:
                print(f"Error procesando tabla {table}: {str(e)}")
                continue