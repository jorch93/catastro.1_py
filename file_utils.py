# -*- coding: utf-8 -*-
import os
import shutil

class FileOrganizer:
    """
    Clase para organizar archivos catastrales en las categorías Rústico y Urbano.
    """
    
    def __init__(self):
        self.RUSTICO_CODE = "RA"
        self.URBANO_CODE = "UA"
        self.CATEGORY_DIRS = {
            self.RUSTICO_CODE: "Rústico",
            self.URBANO_CODE: "Urbano"
        }

    def organize_files(self, base_dir):
        """
        Organiza los archivos catastrales en directorios Rústico y Urbano.
        
        Args:
            base_dir (str): Directorio base que contiene los archivos extraídos
        """
        # Create category directories
        category_paths = self._create_category_dirs(base_dir)
        
        # Walk through all subdirectories
        for root, _, _ in os.walk(base_dir):
            if any(category in root for category in self.CATEGORY_DIRS.values()):
                continue  # Skip if we're already in a category directory
                
            folder_name = os.path.basename(root)
            category = self._get_category(folder_name)
            
            if category:
                target_dir = category_paths[category]
                # First move the contents
                self._move_contents(root, target_dir)
                # Then rename files in the target directories
                for item in os.listdir(target_dir):
                    item_path = os.path.join(target_dir, item)
                    if os.path.isdir(item_path):
                        self._rename_files_in_directory(item_path)

        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path) and item not in self.CATEGORY_DIRS.values():
                shutil.rmtree(item_path)

    def _create_category_dirs(self, base_dir):
        """Creates and returns paths for Rustico and Urbano directories."""
        category_paths = {}
        for code, category in self.CATEGORY_DIRS.items():
            path = os.path.join(base_dir, category)
            os.makedirs(path, exist_ok=True)
            category_paths[category] = path
        return category_paths

    def _get_category(self, folder_name):
        """Determines if a folder is Rústico or Urbano based on its name."""
        if len(folder_name) >= 5:
            type_code = folder_name[3:5]
            return self.CATEGORY_DIRS.get(type_code)
        return None

    def _move_contents(self, source_dir, target_dir):
        """Moves contents to their target directory."""
        for item in os.listdir(source_dir):
            src_path = os.path.join(source_dir, item)
            if not os.path.isdir(src_path):
                continue

            dst_path = os.path.join(target_dir, item)
            
            if os.path.exists(dst_path):
                # Merge contents if destination exists
                for content in os.listdir(src_path):
                    content_src = os.path.join(src_path, content)
                    content_dst = os.path.join(dst_path, content)
                    if not os.path.exists(content_dst):
                        shutil.move(content_src, content_dst)
            else:
                # Move entire folder if destination doesn't exist
                shutil.move(src_path, dst_path)

    def _rename_files_in_directory(self, directory):
        """
        Renombra los archivos dentro de un directorio usando los primeros 7 caracteres
        del nombre del directorio como prefijo.
        
        Args:
            directory (str): Directorio que contiene los archivos a renombrar
        """
        dir_name = os.path.basename(directory)
        prefix = dir_name[:7] + "_"

        for file in os.listdir(directory):
            if not file.lower().endswith(('.zip', '.zip')):
                file_path = os.path.join(directory, file)
                if os.path.isfile(file_path):
                    new_name = f"{prefix}{file}"
                    new_path = os.path.join(directory, new_name)
                    os.rename(file_path, new_path)