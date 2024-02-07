import os
from .logger import setup_logging

# Initializing helper classes and functions
logger = setup_logging(__name__)

class ProjectDirectory:
    def __init__(self, name, root='./data') -> None:
        self.root = root
        self.name = os.path.join(self.root, name)
        self.created_directories = {}
        if not os.path.exists(self.name):
            os.makedirs(self.name)
            logger.info(f"Created Project Directory in path {self.name}")
        
        # Create default snowflake stage directory
        self.create_ds_if_not_exists('snowflake_stage')

    def create_ds_if_not_exists(self, *directories) -> None:
        """
        For each given directory name, create the directory if it does not exist.
        """
        for directory in directories:
            path = os.path.join(self.name, directory)
            if not os.path.exists(path):
                os.makedirs(path)
                logger.info(f"Created subdirectories at path {path}")
            self.created_directories[directory] = path

    def get_directories(self, query):
        """
        Return a list of full paths of the directories that have been created.
        """
        return self.created_directories.get(query)
    

def has_csv_files(folder_path):
        """
        Check if the given folder contains any CSV files
        """
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            # Check if the file is a CSV file
            if file_name.endswith('.csv') and os.path.isfile(file_path):
                return True

        # No CSV files were found
        return False

def py_file_name():
    return os.path.basename(__file__).replace('.py','')