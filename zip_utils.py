# -*- coding: utf-8 -*-
import os
import zipfile
import patoolib
from concurrent.futures import ThreadPoolExecutor

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
        """
        if not os.path.isdir(input_dir):
            print(f"El directorio {input_dir} no existe.")
            return

        output_dir = output_dir or input_dir
        files = [f for f in os.listdir(input_dir) if f.lower().endswith('.zip')]

        # Process ZIP files in parallel
        with ThreadPoolExecutor() as executor:
            futures = []
            for file in files:
                input_zip = os.path.join(input_dir, file)
                zip_output_dir = os.path.join(output_dir, os.path.splitext(file)[0])
                os.makedirs(zip_output_dir, exist_ok=True)
                
                print(f"Procesando archivo: {file}")
                futures.append(
                    executor.submit(self.extract_nested_zip, input_zip, zip_output_dir)
                )
            
            # Wait for all extractions to complete
            for future in futures:
                zip_output_dir = future.result()
                if zip_output_dir and os.path.exists(zip_output_dir):
                    self.extract_all_zips_in_subdirectories(zip_output_dir)

        print("Todos los archivos ZIP han sido descomprimidos en sus respectivas carpetas.")

    def extract_nested_zip(self, input_zip, output_dir):
        """
        Extrae un archivo ZIP que contiene un único ZIP anidado.
        Maneja el caso especial de archivos ZIP divididos (.z01).
        
        Args:
            input_zip (str): Ruta del archivo ZIP a extraer
            output_dir (str): Directorio donde se extraerá el contenido
        Returns:
            str: Directorio de salida si la extracción fue exitosa, None en caso contrario
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
                        print(f"Archivo dividido extraído: {main_zip}")
                        os.remove(main_zip)
                    except Exception as e:
                        print(f"Error al extraer archivo dividido {main_zip}: {e}")
                        return None
            else:
                with zipfile.ZipFile(nested_path, 'r') as zip_ref:
                    zip_ref.extractall(output_dir)
                print(f"Archivo ZIP anidado extraído: {nested_path}")

            return output_dir

        except zipfile.BadZipFile:
            print(f"Archivo ZIP inválido: {input_zip}")
            return None
        finally:
            self._cleanup_directory(temp_extract_dir)

    def extract_all_zips_in_subdirectories(self, output_dir):
        """
        Extrae todos los archivos ZIP en los subdirectorios, manteniendo la estructura de carpetas.
        
        Args:
            output_dir (str): Directorio base donde buscar archivos ZIP
        """
        def extract_zip(zip_path, extract_folder):
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_folder)
                os.remove(zip_path)
            except zipfile.BadZipFile:
                print(f"Archivo ZIP inválido: {zip_path}")

        # Collect all ZIP files and their target folders
        zip_tasks = []
        for folder_path, _, _ in os.walk(output_dir):
            zip_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.zip')]
            for zip_file in zip_files:
                zip_path = os.path.join(folder_path, zip_file)
                zip_tasks.append((zip_path, folder_path))
        
        # Process ZIP files in parallel
        with ThreadPoolExecutor() as executor:
            executor.map(lambda x: extract_zip(*x), zip_tasks)

    def _cleanup_directory(self, directory):
        """
        Elimina un directorio y todo su contenido, solo si es un directorio temporal.
        
        Args:
            directory (str): Directorio a eliminar
        """
        if "temp_extracted" in directory or "nested_temp" in directory:
            for root, dirs, files in os.walk(directory, topdown=False):
                for file in files:
                    os.remove(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))
            os.rmdir(directory)