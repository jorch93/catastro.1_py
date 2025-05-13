# -*- coding: utf-8 -*-
import os
import zipfile
import patoolib
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

class ZipExtractor:
    """
    Clase para manejar la extracción de archivos ZIP, incluyendo:
    - Archivos ZIP divididos (.zip + .z01)
    - ZIPs anidados
    - Extracción en paralelo de múltiples archivos
    """
    
    ZIP_EXTENSIONS = ('.zip', '.z01')

    def process_directory(self, input_dir, output_dir=None):
        """
        Procesa todos los archivos ZIP en el directorio de entrada.
        
        Args:
            input_dir (str): Ruta del directorio que contiene los archivos ZIP
            output_dir (str, opcional): Directorio donde se guardarán los archivos extraídos.
                                  Si no se especifica, usa el directorio de entrada.
        
        Comportamiento:
        1. Verifica si el directorio de entrada existe
        2. Localiza todos los archivos .zip en el directorio
        3. Crea un ThreadPool para procesamiento paralelo
        4. Para cada ZIP:
           - Crea un subdirectorio con el nombre del ZIP
           - Extrae el contenido usando extract_nested_zip()
           - Procesa recursivamente cualquier ZIP encontrado
        """
        if not os.path.isdir(input_dir):
            print(f"El directorio {input_dir} no existe.")
            return

        output_dir = output_dir or input_dir
        zip_files = [f for f in os.listdir(input_dir) if f.lower().endswith('.zip')]

        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(
                    self.extract_nested_zip,
                    os.path.join(input_dir, file),
                    os.path.join(output_dir, os.path.splitext(file)[0])
                ): file for file in zip_files
            }

            for future in tqdm(as_completed(futures), 
                               total=len(futures), 
                               desc="Procesando archivos ZIP [1/7]", 
                               unit="file",
                               leave=False):
                zip_output_dir = future.result()
                if zip_output_dir and os.path.exists(zip_output_dir):
                    self.extract_all_zips_in_subdirectories(zip_output_dir)

    def extract_nested_zip(self, input_zip, output_dir):
        """
        Extrae un archivo ZIP que contiene un único ZIP anidado.
        
        Args:
            input_zip (str): Ruta del archivo ZIP a extraer
            output_dir (str): Directorio donde se extraerá el contenido
        
        Returns:
            str: Directorio de salida si la extracción fue exitosa
            None: Si ocurre algún error
        
        Proceso:
        1. Verifica existencia del archivo
        2. Crea directorio temporal
        3. Extrae el ZIP principal
        4. Busca ZIPs anidados
        5. Maneja casos especiales (.z01)
        6. Limpia archivos temporales
        """
        if not os.path.isfile(input_zip):
            print(f"El archivo {input_zip} no existe o no es un archivo válido.")
            return None

        temp_extract_dir = os.path.join(output_dir, "temp_extracted")
        os.makedirs(temp_extract_dir, exist_ok=True)

        try:
            with zipfile.ZipFile(input_zip, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_dir)

            nested_files = [
                f for f in os.listdir(temp_extract_dir) 
                if f.lower().endswith(self.ZIP_EXTENSIONS)
            ]

            if not nested_files:
                print(f"No se encontró archivo ZIP anidado en {input_zip}")
                return None

            nested_file = nested_files[0]
            nested_path = os.path.join(temp_extract_dir, nested_file)

            if nested_file.lower().endswith('.z01'):
                main_zip = nested_path[:-4] + '.zip'
                if os.path.exists(main_zip):
                    try:
                        patoolib.extract_archive(main_zip, outdir=output_dir)
                        os.remove(main_zip)
                    except Exception as e:
                        print(f"Error al extraer archivo dividido {main_zip}: {e}")
                        return None
            else:
                with zipfile.ZipFile(nested_path, 'r') as zip_ref:
                    zip_ref.extractall(output_dir)

            return output_dir

        except zipfile.BadZipFile:
            print(f"Archivo ZIP inválido: {input_zip}")
            return None
        finally:
            self._cleanup_directory(temp_extract_dir)

    def extract_all_zips_in_subdirectories(self, output_dir):
        """
        Extrae recursivamente todos los ZIPs en subdirectorios.
        
        Args:
            output_dir (str): Directorio base donde buscar archivos ZIP
        
        Proceso:
        1. Recorre recursivamente todos los subdirectorios
        2. Identifica archivos ZIP
        3. Extrae en paralelo usando ThreadPoolExecutor
        4. Elimina los ZIPs procesados
        """
        def extract_zip(zip_path, extract_folder):
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_folder)
                os.remove(zip_path)
            except zipfile.BadZipFile:
                print(f"Archivo ZIP inválido: {zip_path}")

        # Adquirir todos los archivos ZIP y sus subdirectorios de destino
        zip_tasks = []
        for folder_path, _, _ in os.walk(output_dir):
            zip_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.zip')]
            for zip_file in zip_files:
                zip_path = os.path.join(folder_path, zip_file)
                zip_tasks.append((zip_path, folder_path))
        
        # Procesar archivos ZIP en paralelo
        with ThreadPoolExecutor() as executor:
            executor.map(lambda x: extract_zip(*x), zip_tasks)

    def _cleanup_directory(self, directory):
        """
        Elimina directorios temporales y su contenido.
        
        Args:
            directory (str): Directorio a eliminar
        
        Seguridad:
        - Solo elimina directorios que contengan "temp_extracted" o "nested_temp"
        - Elimina archivos y subdirectorios de forma segura
        """
        if "temp_extracted" in directory or "nested_temp" in directory:
            for root, dirs, files in os.walk(directory, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))
            os.rmdir(directory)