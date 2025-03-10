import pandas as pd
from core.custom_logger import CustomLogger
from core.db_manager import DBManager
from core.config_manager import ConfigManager
from data.validator import process_and_insert  # Import the validator

class Migration:
    """
    Class to migrate data from CSV files to a PostgreSQL database.
    """
    def __init__(self, config_path="config.yaml"):
        self.logger = CustomLogger(self.__class__.__name__)
        self.logger.info("Initializing migration.")
        self.config = ConfigManager.load_config(config_path)
        db_manager = DBManager(config_path)
        self.engine = db_manager.get_engine()
        self.logger.info("Database connection established.")

    def read_csv_files(self):
        """
        Reads the CSV files specified in the configuration.
        Returns DataFrames for hired_employees, departments, and jobs.
        """
        self.logger.info("Reading CSV files with column metadata...")
        csv_config = self.config['csv']
        
        hired_conf = csv_config['hired_employees']
        hired_employees = pd.read_csv(hired_conf['path'], header=None, names=hired_conf['columns'])
        
        dept_conf = csv_config['departments']
        departments = pd.read_csv(dept_conf['path'], header=0, names=dept_conf['columns'])
        
        jobs_conf = csv_config['jobs']
        jobs = pd.read_csv(jobs_conf['path'], header=0, names=jobs_conf['columns'])
        
        self.logger.info("CSV files read and columns assigned successfully.")
        return hired_employees, departments, jobs

    def migrate_data(self):
        """
        Orchestrates the data migration:
          - Reads the CSV files.
          - Validates and inserts the data into the database.
        """
        self.logger.info("Starting data migration...")
        hired_employees, departments, jobs = self.read_csv_files()
        self.logger.info("Inserting data into the database with validation...")
        try:
            process_and_insert(hired_employees, self.config['csv']['hired_employees']['columns'], "hired_employees", self.engine, process_type="migration")
            process_and_insert(departments, self.config['csv']['departments']['columns'], "departments", self.engine, process_type="migration")
            process_and_insert(jobs, self.config['csv']['jobs']['columns'], "jobs", self.engine, process_type="migration")
            self.logger.info("Migration completed successfully.")
        except Exception as e:
            self.logger.error(f"Error during migration: {e}")
            raise e

if __name__ == "__main__":
    migration = Migration()
    migration.migrate_data()
