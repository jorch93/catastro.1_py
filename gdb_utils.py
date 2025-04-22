# -*- coding: utf-8 -*-
import arcpy
import os
import multiprocessing
from contextlib import contextmanager
from add_info import CadastralInfoManager

class GDBProcessor:
    def __init__(self):
        # Define constants
        self.RUSTICO_DATASET = "Rustico"
        self.URBANO_DATASET = "Urbano"
        self.DATASETS = [self.RUSTICO_DATASET, self.URBANO_DATASET]
        
        # Single definition for features
        self.FEATURES = ["ALTIPUN", "CONSTRU", "EJES", "ELEMLIN", 
                        "ELEMPUN", "ELEMTEX", "HOJAS", "LIMITES", 
                        "MAPA", "MASA", "PARCELA", "SUBPARCE"]
        
        self.SPATIAL_REF = arcpy.SpatialReference(32628)
        self.MUNICIPAL_CODE_FIELD = "Codigo_Municipal_Catastral"
        
        # Set chunks and workers
        self.CHUNKS_PER_TYPE = 4
        self.max_workers = self.CHUNKS_PER_TYPE * 2  # 8 total chunks
        
        print(f"\nConfiguración de procesamiento:")
        print(f"Workers configurados: {self.max_workers} (1 por chunk)")

    # Administrador de contexto para cambios de espacio de trabajo
    @contextmanager
    def _managed_workspace(self, workspace):
        """Mantiene el espacio de trabajo original y lo restaura al final"""
        original = arcpy.env.workspace
        try:
            arcpy.env.workspace = workspace
            yield
        finally:
            arcpy.env.workspace = original

    def process_directory(self, input_dirs, final_gdb):
        """Process directories using chunking strategy"""
        base_temp_dir = os.path.join(os.path.dirname(final_gdb), "temp_processing")
        chunk_temp_dir = os.path.join(base_temp_dir, "chunks")
        try:
            os.makedirs(chunk_temp_dir, exist_ok=True)
            
            # Create and process chunks
            chunk_gdbs = self._create_balanced_chunks(input_dirs, chunk_temp_dir)
            print(f"\nChunks creados: {len(chunk_gdbs)}")
            
            # Process chunks in parallel
            with multiprocessing.Pool(processes=self.max_workers) as pool:
                tasks = [(input_files, gdb_path) for gdb_path, input_files in chunk_gdbs.items()]
                results = pool.starmap(self._process_chunk_gdb, tasks)
            
            if not any(results):
                raise Exception("No chunks processed successfully")

            # Merge chunks into final GDB
            print("\nMerging chunks into final GDB...")
            self._merge_final_gdbs(chunk_gdbs.keys(), final_gdb)

            # Compactar la geodatabase
            print("\nCompactando geodatabase final...")
            arcpy.management.Compact(final_gdb)
            
            # Only cleanup after successful merge
            print("\nLimpiando directorios temporales...")
            self._cleanup_temp_dir(base_temp_dir)
                
        except Exception as e:
            print(f"Error en proceso principal: {str(e)}")
            raise

    def _create_balanced_chunks(self, input_dirs, temp_dir):
        """Create balanced chunks based on file sizes"""
        chunk_gdbs = {}
        
        for idx, input_dir in enumerate(input_dirs):
            print(f"\nProcesando directorio: {input_dir}")
            # Fix prefix detection
            is_rustico = any(r in input_dir for r in ["Rustico", "Rústico"])
            prefix = "R" if is_rustico else "U"
            chunk_offset = idx * self.CHUNKS_PER_TYPE
            
            # Debug info
            print(f"Tipo: {'Rústico' if is_rustico else 'Urbano'}")
            print(f"Prefijo: {prefix}")
            print(f"Offset: {chunk_offset}")
            
            # Collect files with sizes
            files_with_size = []
            total_size = 0
            for root, _, files in os.walk(input_dir):
                for f in files:
                    if f.lower().endswith('.shp'):
                        path = os.path.join(root, f)
                        size = os.path.getsize(path)
                        files_with_size.append((path, size))
                        total_size += size
            
            # Create chunks based on total size
            chunks = [[] for _ in range(self.CHUNKS_PER_TYPE)]
            chunk_sizes = [0] * self.CHUNKS_PER_TYPE
            
            # Sort by size descending for better distribution
            for file_path, size in sorted(files_with_size, key=lambda x: x[1], reverse=True):
                smallest_idx = chunk_sizes.index(min(chunk_sizes))
                chunks[smallest_idx].append(file_path)
                chunk_sizes[smallest_idx] += size
            
            # Create GDBs with unique names
            for i, chunk_files in enumerate(chunks):
                if chunk_files:
                    total_chunk_size = sum(os.path.getsize(f) for f in chunk_files)
                    chunk_num = i + chunk_offset
                    chunk_name = f"chunk_{prefix}{chunk_num}.gdb"
                    gdb_path = os.path.join(temp_dir, chunk_name)
                    
                    print(f"Chunk {prefix}{chunk_num}: {len(chunk_files)} archivos, {total_chunk_size/1024/1024:.2f} MB")
                    print(f"Creando GDB: {chunk_name}")
                    
                    arcpy.CreateFileGDB_management(temp_dir, os.path.basename(gdb_path))
                    chunk_gdbs[gdb_path] = chunk_files
        
        return chunk_gdbs

    def _process_chunk_gdb(self, input_files, chunk_gdb):
        """Process a single chunk GDB independently"""
        try:
            print(f"\nProcesando chunk GDB: {chunk_gdb}")
            with self._managed_workspace(chunk_gdb):
                # 1. Create datasets first
                self._create_datasets(chunk_gdb)
                print("Datasets creados")

                # 2. Import shapefiles
                for shp_path in input_files:
                    try:
                        base_name = os.path.splitext(os.path.basename(shp_path))[0]
                        fc_name = f"T{base_name}" if not base_name.startswith('T') else base_name
                        
                        if not arcpy.Exists(fc_name):
                            arcpy.FeatureClassToFeatureClass_conversion(
                                shp_path,
                                chunk_gdb,
                                fc_name
                            )
                            print(f"Importado: {fc_name}")
                    except Exception as e:
                        print(f"Error importando {shp_path}: {str(e)}")
                        continue
                
                # 3. Setup municipal codes
                self._setup_municipal_code_field(chunk_gdb)
                print("Códigos municipales configurados")
                
                # 4. Create feature classes
                self._create_feature_classes(chunk_gdb)
                print("Feature classes creadas")
                
                # 5. Append feature classes
                self._append_feature_classes(chunk_gdb)
                print("Append completado")
                
                # 6. Add and update cadastral information
                json_path = os.path.join(os.path.dirname(__file__), "cod_catastrales.json")
                if os.path.exists(json_path):
                    cadastral_manager = CadastralInfoManager(chunk_gdb, json_path)
                    for dataset in self.DATASETS:
                        print(f"\nUpdating cadastral info for {dataset}...")
                        cadastral_manager.process_feature_classes(self.FEATURES, dataset)
                    print("Cadastral info update completado")
                else:
                    print("Warning: cod_catastrales.json not found, skipping cadastral info update")
                
                # 7. Clear workspace before returning
                arcpy.env.workspace = None
                return True
                
        except Exception as e:
            print(f"Error processing chunk {chunk_gdb}: {str(e)}")
            return False
        finally:
            # Ensure workspace is cleared
            arcpy.env.workspace = None

    def _merge_final_gdbs(self, chunk_gdbs, final_gdb):
        """Sequential merge of all chunk GDBs"""
        if not arcpy.Exists(final_gdb):
            arcpy.CreateFileGDB_management(
                os.path.dirname(final_gdb),
                os.path.basename(final_gdb)
            )
        
        try:
            self._create_datasets(final_gdb)
            
            for dataset in self.DATASETS:
                for feature in self.FEATURES:
                    self._merge_feature_type(chunk_gdbs, final_gdb, dataset, feature)
                    
        except Exception as e:
            print(f"Error merging GDBs: {str(e)}")
            raise

    def _merge_feature_type(self, chunk_gdbs, final_gdb, dataset, feature):
        """Merge a single feature type from all chunks"""
        target_fc = os.path.join(final_gdb, dataset, f"{dataset}_{feature}")
        source_fcs = []
        
        for chunk_gdb in chunk_gdbs:
            fc_path = os.path.join(chunk_gdb, dataset, f"{dataset}_{feature}")
            if arcpy.Exists(fc_path):
                source_fcs.append(fc_path)
        
        if source_fcs:
            try:
                if len(source_fcs) == 1:
                    arcpy.FeatureClassToFeatureClass_conversion(
                        source_fcs[0],
                        os.path.dirname(target_fc),
                        os.path.basename(target_fc)
                    )
                else:
                    arcpy.Merge_management(source_fcs, target_fc)
                print(f"Merged: {os.path.basename(target_fc)}")
            except Exception as e:
                print(f"Error merging {feature}: {str(e)}")

    # Limpieza de directorios temporales, método estático para evitar dependencias
    @staticmethod
    def _cleanup_temp_dir(temp_dir):
        """Limpiar directorios temporales con reintentos"""
        if not os.path.exists(temp_dir):
            return

        # Clear workspace and cache
        arcpy.env.workspace = None
        arcpy.ClearWorkspaceCache_management()
    
        # Import needed modules
        import time
        import shutil
        from pathlib import Path

        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            try:
                # First try to remove lock files
                for lock_file in Path(temp_dir).rglob("*.lock"):
                    try:
                        lock_file.unlink()
                    except:
                        pass

                # Wait for resources to be released
                time.sleep(retry_delay)
            
                # Try to remove the directory
                shutil.rmtree(temp_dir, ignore_errors=True)
            
                # Verify removal
                if not os.path.exists(temp_dir):
                    print("Limpieza completada con éxito")
                    return
            
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Reintento {attempt + 1} de limpieza: {str(e)}")
                    time.sleep(retry_delay)
                else:
                    print(f"No se pudo limpiar completamente después de {max_retries} intentos")
                    print(f"Error final: {str(e)}")
                
                    # List remaining lock files for debugging
                    try:
                        for lock_file in Path(temp_dir).rglob("*.lock"):
                            print(f"Archivo bloqueado: {lock_file}")
                    except:
                        pass

    def _setup_municipal_code_field(self, gdb_path):
        arcpy.env.workspace = gdb_path

        for fc in arcpy.ListFeatureClasses():
            try:
                if not arcpy.ListFields(fc, self.MUNICIPAL_CODE_FIELD):
                    arcpy.AddField_management(fc, self.MUNICIPAL_CODE_FIELD, "LONG")

                codigo_municipal = int(os.path.basename(fc)[1:6])
                with arcpy.da.UpdateCursor(fc, [self.MUNICIPAL_CODE_FIELD]) as cursor:
                    for row in cursor:
                        row[0] = codigo_municipal
                        cursor.updateRow(row)
                print(f"Código municipal calculado para: {fc}")
            except Exception as e:
                print(f"Error procesando {fc}: {str(e)}")

    def _create_datasets(self, gdb_path):
        """
        Crea los datasets con verificación explícita.
        """
        arcpy.env.workspace = gdb_path
        
        for dataset in ["Rustico", "Urbano"]:
            dataset_path = os.path.join(gdb_path, dataset)
            if not arcpy.Exists(dataset_path):
                try:
                    print(f"Creando dataset {dataset}...")
                    arcpy.CreateFeatureDataset_management(
                        gdb_path, 
                        dataset, 
                        self.SPATIAL_REF
                    )
                    if not arcpy.Exists(dataset_path):
                        raise arcpy.ExecuteError(f"No se pudo verificar la creación del dataset {dataset}")
                    print(f"Dataset creado exitosamente: {dataset}")
                except arcpy.ExecuteError as e:
                    print(f"Error creando dataset {dataset}: {str(e)}")
                    raise

    def _create_feature_classes(self, gdb_path):
        arcpy.env.workspace = gdb_path

        for dataset in self.DATASETS:
            dataset_path = os.path.join(gdb_path, dataset)
            
            for feature in self.FEATURES:
                fc_name = f"{dataset}_{feature}"
                if not arcpy.Exists(os.path.join(dataset_path, fc_name)):
                    template = self._find_template(gdb_path, feature)
                    if template:
                        arcpy.CreateFeatureclass_management(
                            dataset_path, fc_name, 
                            template=template,
                            spatial_reference=self.SPATIAL_REF
                        )
                        print(f"Feature class creada: {fc_name}")

    def _append_feature_classes(self, gdb_path):
        """Sequential append of feature classes with geometry validation"""
        with self._managed_workspace(gdb_path):
            for dataset in self.DATASETS:
                dataset_path = os.path.join(gdb_path, dataset)

                for feature in self.FEATURES:
                    target_fc = os.path.join(dataset_path, f"{dataset}_{feature}")
                    if not arcpy.Exists(target_fc):
                        continue

                    # Get target geometry type
                    target_geom = arcpy.Describe(target_fc).shapeType
                    
                    # Filter source features by matching geometry
                    source_fcs = []
                    for fc in arcpy.ListFeatureClasses():
                        if feature in fc.upper() and fc != os.path.basename(target_fc):
                            try:
                                if arcpy.Describe(fc).shapeType == target_geom:
                                    source_fcs.append(fc)
                            except:
                                continue

                    if source_fcs:
                        try:
                            arcpy.Append_management(source_fcs, target_fc, "TEST")
                            print(f"Append completado para: {os.path.basename(target_fc)}")
                        except Exception as e:
                            print(f"Error en append de {os.path.basename(target_fc)}: {str(e)}")

    def _find_template(self, gdb_path, feature_type):
        """
        Find template feature class with matching geometry type.
        Special handling for ALTIPUN to ensure Point geometry.
        """
        arcpy.env.workspace = gdb_path
        feature_type_upper = feature_type.upper()

        # Get all potential matches
        matches = [fc for fc in arcpy.ListFeatureClasses() if feature_type_upper in fc.upper()]
        
        if not matches:
            return None

        # Special handling for ALTIPUN
        if feature_type_upper == "ALTIPUN":
            # Try to find Point geometry first
            for fc in matches:
                try:
                    desc = arcpy.Describe(fc)
                    if desc.shapeType == "Point":
                        print(f"Using Point geometry template for {feature_type}: {fc}")
                        return fc
                except:
                    continue
            
            print(f"Warning: No Point geometry found for {feature_type}, using first available template")
        
        # For non-ALTIPUN or if no Point geometry found
        first_match = matches[0]
        geometry_type = arcpy.Describe(first_match).shapeType
        print(f"Using template for {feature_type}: {first_match} ({geometry_type})")
        return first_match
