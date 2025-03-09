import pandas as pd
from core.custom_logger import CustomLogger
from core.db_manager import DBManager
from core.config_manager import ConfigManager

class Migration:
    """
    Clase Migration para migrar datos desde archivos CSV a una base de datos PostgreSQL.
    Utiliza DBManager para obtener la conexión y ConfigManager para la configuración.
    """
    def __init__(self, config_path="config.yaml"):
        self.logger = CustomLogger(self.__class__.__name__)
        self.logger.info("Inicializando migración.")
        
        # Cargar la configuración usando ConfigManager
        self.config = ConfigManager.load_config(config_path)
        
        # Inicializar DBManager y obtener el engine
        db_manager = DBManager(config_path)
        self.engine = db_manager.get_engine()
        
        self.logger.info("Conexión a la base de datos establecida.")

    def read_csv_files(self):
        """
        Lee los archivos CSV especificados en la configuración usando los metadatos de columnas definidos en el YAML.
        :return: Tuple con DataFrames para hired_employees, departments y jobs.
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
          - Lee los archivos CSV.
          - Inserta los datos en las tablas correspondientes en la base de datos.
          - Registra cada paso del proceso.
        """
        self.logger.info("Iniciando migración de datos...")
        hired_employees, departments, jobs = self.read_csv_files()
        self.logger.info("Insertando datos en la base de datos...")
        try:
            hired_employees.to_sql('hired_employees', self.engine, index=False, if_exists='replace')
            departments.to_sql('departments', self.engine, index=False, if_exists='replace')
            jobs.to_sql('jobs', self.engine, index=False, if_exists='replace')
            self.logger.info("Migración completada con éxito.")
        except Exception as e:
            self.logger.error(f"Error durante la migración: {e}")
            raise e

if __name__ == "__main__":
    migration = Migration()
    migration.migrate_data()
