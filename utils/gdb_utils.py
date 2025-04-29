# -*- coding: utf-8 -*-
import arcpy
import os
import multiprocessing
from contextlib import contextmanager
from utils.add_info import CadastralInfoManager

class GDBProcessor:
    """
    Clase para procesar y gestionar Geodatabases (GDB) con las siguientes capacidades:
    - Procesamiento en paralelo de chunks
    - Gestión de datasets rústicos y urbanos
    - Manejo de múltiples features catastrales
    - Control automático de espacios de trabajo
    """

    def __init__(self):
        """
        Inicialización con constantes y configuración:
        
        Atributos:
            RUSTICO_DATASET (str): Nombre del dataset rústico
            URBANO_DATASET (str): Nombre del dataset urbano
            DATASETS (list): Lista de datasets disponibles
            FEATURES (list): Lista de features catastrales
            SPATIAL_REF: Referencia espacial (32628)
            MUNICIPAL_CODE_FIELD (str): Campo de código municipal
            CHUNKS_PER_TYPE (int): Chunks por tipo de dataset
            max_workers (int): Número máximo de workers paralelos
        """
        self.RUSTICO_DATASET = "Rustico"
        self.URBANO_DATASET = "Urbano"
        self.DATASETS = [self.RUSTICO_DATASET, self.URBANO_DATASET]
        
        self.FEATURES = ["ALTIPUN", "CONSTRU", "EJES", "ELEMLIN", 
                        "ELEMPUN", "ELEMTEX", "HOJAS", "LIMITES", 
                        "MAPA", "MASA", "PARCELA", "SUBPARCE"]
        
        self.SPATIAL_REF = arcpy.SpatialReference(32628)
        self.MUNICIPAL_CODE_FIELD = "Codigo_Municipal_Catastral"
        
        # Configuración de procesamiento segura
        cpu_count = multiprocessing.cpu_count()
        cpu_percent = 0.70  # 70% utilización
        MIN_FREE_CORES = 2  # Mínimo cores libres
        
        # Calcular cores disponibles manteniendo mínimo libre
        used_cores = int(cpu_count * cpu_percent)
        free_cores = cpu_count - used_cores
        if free_cores < MIN_FREE_CORES:
            used_cores = cpu_count - MIN_FREE_CORES
        
        self.CHUNKS_PER_TYPE = max(2, used_cores // 2)
        self.max_workers = used_cores
        
        print(f"\nConfiguración de procesamiento:")
        print(f"CPUs totales: {cpu_count}")
        print(f"CPUs utilizados: {used_cores} ({int(used_cores/cpu_count * 100)}%)")
        print(f"CPUs libres: {cpu_count - used_cores}")
        print(f"Chunks por tipo: {self.CHUNKS_PER_TYPE}")

    # Administrador de contexto para cambios de espacio de trabajo
    @contextmanager
    def _managed_workspace(self, workspace):
        """
        Gestiona cambios de espacio de trabajo.
        
        Args:
            workspace (str): Espacio de trabajo temporal
        
        Comportamiento:
        - Guarda workspace original
        - Cambia al nuevo workspace
        - Restaura workspace original al finalizar
        """
        original = arcpy.env.workspace
        try:
            arcpy.env.workspace = workspace
            yield
        finally:
            arcpy.env.workspace = original

    def process_directory(self, input_dirs, final_gdb):
        """
        Procesa directorios usando estrategia de chunks.
        
        Args:
            input_dirs (list): Lista de directorios de entrada
            final_gdb (str): Ruta de la geodatabase final
        
        Proceso:
        1. Crea directorio temporal
        2. Genera chunks balanceados
        3. Procesa chunks en paralelo
        4. Combina resultados en GDB final
        5. Limpia archivos temporales
        """
        base_temp_dir = os.path.join(os.path.dirname(final_gdb), "temp_processing")
        chunk_temp_dir = os.path.join(base_temp_dir, "chunks")
        try:
            os.makedirs(chunk_temp_dir, exist_ok=True)
            
            # Crear y procesar chunks
            chunk_gdbs = self._create_balanced_chunks(input_dirs, chunk_temp_dir)
            print(f"\nChunks creados: {len(chunk_gdbs)}")
            
            # Procesar chunks en paralelo
            with multiprocessing.Pool(processes=self.max_workers) as pool:
                tasks = [(input_files, gdb_path) for gdb_path, input_files in chunk_gdbs.items()]
                results = pool.starmap(self._process_chunk_gdb, tasks)
            
            if not any(results):
                raise Exception("No chunks processed successfully")

            # Fusionar chunks en GDB final
            print("\nMerging chunks into final GDB...")
            self._merge_final_gdbs(chunk_gdbs.keys(), final_gdb)

            # Compactar la geodatabase
            print("\nCompactando geodatabase final...")
            arcpy.management.Compact(final_gdb)
            
            # Limpieza de directorios temporales
            print("\nLimpiando directorios temporales...")
            self._cleanup_temp_dir(base_temp_dir)
                
        except Exception as e:
            print(f"Error en proceso principal: {str(e)}")
            raise

    def _create_balanced_chunks(self, input_dirs, temp_dir):
        """
        Crea chunks balanceados basados en tamaños de archivo.

        Args:
            input_dirs (list): Lista de directorios que contienen archivos shapefile
            temp_dir (str): Directorio temporal para almacenar las GDBs de chunks

        Returns:
            dict: Diccionario con rutas de GDB como claves y listas de archivos como valores

        Proceso:
        1. Para cada directorio de entrada:
            - Detecta si es rústico o urbano
            - Calcula offset basado en el índice
            - Recopila archivos y tamaños
            - Distribuye archivos en chunks equilibrados
            - Crea GDBs para cada chunk
        """
        chunk_gdbs = {}
        
        for idx, input_dir in enumerate(input_dirs):
            print(f"\nProcesando directorio: {input_dir}")
            # Detección de prefijo
            is_rustico = any(r in input_dir for r in ["Rustico", "Rústico"])
            prefix = "R" if is_rustico else "U"
            chunk_offset = idx * self.CHUNKS_PER_TYPE
            
            # Debug info
            print(f"Tipo: {'Rústico' if is_rustico else 'Urbano'}")
            print(f"Prefijo: {prefix}")
            print(f"Offset: {chunk_offset}")
            
            # Recopilar archivos y tamaños
            files_with_size = []
            total_size = 0
            for root, _, files in os.walk(input_dir):
                for f in files:
                    if f.lower().endswith('.shp'):
                        path = os.path.join(root, f)
                        size = os.path.getsize(path)
                        files_with_size.append((path, size))
                        total_size += size
            
            # Crear chunks balanceados por tamaño
            chunks = [[] for _ in range(self.CHUNKS_PER_TYPE)]
            chunk_sizes = [0] * self.CHUNKS_PER_TYPE
            
            # Ordenar archivos por tamaño
            for file_path, size in sorted(files_with_size, key=lambda x: x[1], reverse=True):
                smallest_idx = chunk_sizes.index(min(chunk_sizes))
                chunks[smallest_idx].append(file_path)
                chunk_sizes[smallest_idx] += size
            
            # Crear GDBs para cada chunk
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
        """
        Procesa una GDB de chunk de forma independiente.

        Args:
            input_files (list): Lista de archivos shapefile a procesar
            chunk_gdb (str): Ruta de la geodatabase de chunk

        Returns:
            bool: True si el procesamiento fue exitoso, False en caso de error

        Proceso:
        1. Configuración inicial de workspace
        2. Creación de datasets
        3. Importación de shapefiles
        4. Configuración de códigos municipales
        5. Creación de feature classes
        6. Append de feature classes
        7. Actualización de información catastral
        """
        try:
            print(f"\nProcesando chunk GDB: {chunk_gdb}")
            with self._managed_workspace(chunk_gdb):
                # 1. Crear datasets
                self._create_datasets(chunk_gdb)
                print("Datasets creados")

                # 2. Importar shapefiles
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
                
                # 3. Configurar códigos municipales
                self._setup_municipal_code_field(chunk_gdb)
                print("Códigos municipales configurados")
                
                # 4. Crear feature classes
                self._create_feature_classes(chunk_gdb)
                print("Feature classes creadas")
                
                # 5. Append feature classes
                self._append_feature_classes(chunk_gdb)
                print("Append completado")
                
                # 6. Añadir y actualizar información catastral
                cadastral_manager = CadastralInfoManager(chunk_gdb, "cod_catastrales.json")
                for dataset in self.DATASETS:
                    print(f"\nUpdating cadastral info for {dataset}...")
                    cadastral_manager.process_feature_classes(self.FEATURES, dataset)
                print("Cadastral info update completado")
                
                # 7. Limpeza de workspace antes de finalizar
                arcpy.env.workspace = None
                return True
                
        except Exception as e:
            print(f"Error processing chunk {chunk_gdb}: {str(e)}")
            return False
        finally:
            # Asegurar que el espacio de trabajo está limpio
            arcpy.env.workspace = None

    def _merge_final_gdbs(self, chunk_gdbs, final_gdb):
        """
        Combina todas las GDBs de chunks.
        
        Args:
            chunk_gdbs (list): Lista de GDBs de chunks
            final_gdb (str): Ruta de GDB final
        
        Proceso:
        1. Crea GDB final si no existe
        2. Crea datasets necesarios
        3. Combina features por tipo y dataset
        """
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
        """
        Combina un tipo específico de feature desde todos los chunks en la GDB final.

        Args:
            chunk_gdbs (list): Lista de GDBs de chunks a combinar
            final_gdb (str): Ruta de la geodatabase final
            dataset (str): Nombre del dataset ('Rustico' o 'Urbano')
            feature (str): Tipo de feature a combinar ('ALTIPUN', 'CONSTRU', etc.)

        Comportamiento:
        1. Construye rutas de feature classes objetivo y fuente
        2. Recopila feature classes existentes de los chunks
        3. Realiza conversión directa si hay una sola fuente
        4. Ejecuta merge si hay múltiples fuentes
        5. Manejo de errores por feature class
        """
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

    @staticmethod
    def _cleanup_temp_dir(temp_dir):
        """
        Limpia directorios temporales.
        
        Args:
            temp_dir (str): Directorio a limpiar
        
        Características:
        - Reintentos automáticos
        - Limpieza de archivos .lock
        - Verificación de eliminación
        - Reporte de errores detallado
        """
        if not os.path.exists(temp_dir):
            return

        # Limpiar workspace y cache
        arcpy.env.workspace = None
        arcpy.ClearWorkspaceCache_management()
    
        # Importar módulos necesarios
        import time
        import shutil
        from pathlib import Path

        max_retries = 3
        retry_delay = 2  # segundos

        for attempt in range(max_retries):
            try:
                # Primero intentar eliminar archivos .lock
                for lock_file in Path(temp_dir).rglob("*.lock"):
                    try:
                        lock_file.unlink()
                    except:
                        pass

                # Esperar a que los archivos se liberen
                time.sleep(retry_delay)
            
                # Intentar eliminar el directorio temporal
                shutil.rmtree(temp_dir, ignore_errors=True)
            
                # Verificar si el directorio fue eliminado
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
                
                    # Listar archivos bloqueados si la limpieza falla
                    try:
                        for lock_file in Path(temp_dir).rglob("*.lock"):
                            print(f"Archivo bloqueado: {lock_file}")
                    except:
                        pass

    def _setup_municipal_code_field(self, gdb_path):
        """
        Configura y actualiza el campo de código municipal en todas las feature classes.

        Args:
            gdb_path (str): Ruta de la geodatabase a procesar

        Proceso:
        1. Configura workspace temporal
        2. Itera sobre feature classes
        3. Añade campo de código municipal si no existe
        4. Extrae y actualiza códigos municipales
        5. Manejo de errores por feature class
        """
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
        """
        Crea feature classes en los datasets especificados.
        """
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
        """
        Append secuencial a los feature classes de los datasets.
        """
        with self._managed_workspace(gdb_path):
            for dataset in self.DATASETS:
                dataset_path = os.path.join(gdb_path, dataset)

                for feature in self.FEATURES:
                    target_fc = os.path.join(dataset_path, f"{dataset}_{feature}")
                    if not arcpy.Exists(target_fc):
                        continue

                    # Adquirir tipo de geometría
                    target_geom = arcpy.Describe(target_fc).shapeType
                    
                    # Filtrar feature class de origen por geometría
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
        Encontrar el template de feature class para un tipo específico.
        Maneja casos especiales como ALTIPUN.
        """
        arcpy.env.workspace = gdb_path
        feature_type_upper = feature_type.upper()

        # Adquirir posibles templates
        matches = [fc for fc in arcpy.ListFeatureClasses() if feature_type_upper in fc.upper()]
        
        if not matches:
            return None

        # Manejo de excepción ALTIPUN
        if feature_type_upper == "ALTIPUN":
            # Búsqueda de geometría Point
            for fc in matches:
                try:
                    desc = arcpy.Describe(fc)
                    if desc.shapeType == "Point":
                        print(f"Using Point geometry template for {feature_type}: {fc}")
                        return fc
                except:
                    continue
            
            print(f"Warning: No Point geometry found for {feature_type}, using first available template")
        
        # Para los casos generales, usar el primer match
        first_match = matches[0]
        geometry_type = arcpy.Describe(first_match).shapeType
        print(f"Using template for {feature_type}: {first_match} ({geometry_type})")
        return first_match
