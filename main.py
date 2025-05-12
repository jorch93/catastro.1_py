# -*- coding: utf-8 -*-
import time
import os
import json
from utils.zip_utils import ZipExtractor
from utils.file_utils import FileOrganizer
from utils.gdb_utils import GDBProcessor
from utils.dbf_utils import DBFProcessor

def load_config(path="config.json"):
    with open(path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    required = ["input", "output", "gdb"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Faltan claves requeridas en el config.json: {', '.join(missing)}")

    return config

def main():
    start_time = time.time()

    try:
        # Subir configuración de JSON en el directorio actual
        config = load_config()

        # Extraer y organizar archivos
        zip_extractor = ZipExtractor()
        file_organizer = FileOrganizer()

        zip_extractor.process_directory(config["input"], config["output"])
        file_organizer.organize_files(config["output"])

        # Procesar GDB
        input_dirs = [
            os.path.join(config["output"], "Rústico"),
            os.path.join(config["output"], "Urbano")
        ]

        processor = GDBProcessor()
        processor.process_directory(input_dirs, config["gdb"])

        # Procesar tablas DBF después de GDB
        dbf_processor = DBFProcessor()
        dbf_processor.process_directory(input_dirs, config["gdb"])

    except Exception as e:
        print(f"\nError durante el procesamiento: {str(e)}")
        raise
    finally:
        elapsed_minutes = (time.time() - start_time) / 60
        print(f"\nTiempo total de ejecución: {elapsed_minutes:.2f} minutos")

if __name__ == "__main__":
    main()
