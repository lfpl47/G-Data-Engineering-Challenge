# data/migration.py
import pandas as pd
from core.custom_logger import CustomLogger
from core.db_manager import DBManager
from core.config_manager import ConfigManager
from data.validator import process_and_insert  # Importamos el validador

class Migration:
    """
    Clase para migrar datos desde archivos CSV a la base de datos PostgreSQL.
    """
    def __init__(self, config_path="config.yaml"):
        self.logger = CustomLogger(self.__class__.__name__)
        self.logger.info("Inicializando migración.")
        self.config = ConfigManager.load_config(config_path)
        db_manager = DBManager(config_path)
        self.engine = db_manager.get_engine()
        self.logger.info("Conexión a la base de datos establecida.")

    def read_csv_files(self):
        """
        Lee los archivos CSV especificados en la configuración.
        Retorna DataFrames para hired_employees, departments y jobs.
        """
        self.logger.info("Leyendo archivos CSV con metadatos de columnas...")
        csv_config = self.config['csv']
        
        hired_conf = csv_config['hired_employees']
        hired_employees = pd.read_csv(hired_conf['path'], header=None, names=hired_conf['columns'])
        
        dept_conf = csv_config['departments']
        departments = pd.read_csv(dept_conf['path'], header=0, names=dept_conf['columns'])
        
        jobs_conf = csv_config['jobs']
        jobs = pd.read_csv(jobs_conf['path'], header=0, names=jobs_conf['columns'])
        
        self.logger.info("Archivos CSV leídos y columnas asignadas correctamente.")
        return hired_employees, departments, jobs

    def migrate_data(self):
        """
        Orquesta la migración de datos:
          - Lee los CSV.
          - Valida e inserta los datos en la base de datos.
        """
        self.logger.info("Iniciando migración de datos...")
        hired_employees, departments, jobs = self.read_csv_files()
        self.logger.info("Insertando datos en la base de datos con validación...")
        try:
            process_and_insert(hired_employees, self.config['csv']['hired_employees']['columns'], "hired_employees", self.engine, process_type="migracion")
            process_and_insert(departments, self.config['csv']['departments']['columns'], "departments", self.engine, process_type="migracion")
            process_and_insert(jobs, self.config['csv']['jobs']['columns'], "jobs", self.engine, process_type="migracion")
            self.logger.info("Migración completada con éxito.")
        except Exception as e:
            self.logger.error(f"Error durante la migración: {e}")
            raise e

if __name__ == "__main__":
    migration = Migration()
    migration.migrate_data()
