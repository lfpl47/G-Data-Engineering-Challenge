import yaml
import pandas as pd
from sqlalchemy import create_engine
from api.custom_logger import CustomLogger

class Migration:
    """
    Clase para migrar datos desde archivos CSV a una base de datos PostgreSQL.
    Utiliza un archivo de configuración YAML para obtener rutas de archivos CSV y parámetros de conexión.
    Emplea la clase CustomLogger para registrar el flujo de ejecución y errores.
    """
    def __init__(self, config_path="config.yaml"):
        """
        Inicializa la migración: configura el logger, carga la configuración y crea el motor de la base de datos.
        :param config_path: Ruta del archivo YAML de configuración (por defecto "config.yaml").
        """
        self.logger = CustomLogger(self.__class__.__name__)
        self.logger.info("Inicializando migración.")
        self.config = self.load_config(config_path)
        self.logger.info("Configuración cargada.")
        self.engine = self.get_db_engine()

    def load_config(self, config_path: str) -> dict:
        """
        Carga la configuración desde un archivo YAML.
        :param config_path: Ruta al archivo YAML de configuración.
        :return: Diccionario con la configuración cargada.
        """
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config

    def read_csv_files(self):
        """
        Lee los archivos CSV especificados en la configuración.
        :return: Tres DataFrames correspondientes a hired_employees, departments y jobs.
        """
        self.logger.info("Leyendo archivos CSV...")
        csv_config = self.config['csv']
        # Leer cada archivo CSV usando pandas
        hired_employees = pd.read_csv(csv_config['hired_employees'])
        departments = pd.read_csv(csv_config['departments'])
        jobs = pd.read_csv(csv_config['jobs'])
        self.logger.info("Archivos CSV leídos correctamente.")
        return hired_employees, departments, jobs

    def get_db_engine(self):
        """
        Crea y retorna un motor SQLAlchemy para conectarse a la base de datos PostgreSQL.
        Se construye la URL de conexión utilizando los parámetros (usuario, contraseña, host, puerto, base de datos)
        especificados en la configuración YAML.
        :return: Objeto engine de SQLAlchemy.
        """
        self.logger.info("Creando motor de base de datos...")
        db_config = self.config['database']
        user = db_config['user']
        password = db_config['password']
        host = db_config['host']
        port = db_config['port']
        dbname = db_config['dbname']
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(db_url)
        self.logger.info("Motor de base de datos creado.")
        return engine

    def migrate_data(self):
        """
        Orquesta la migración de datos:
        1. Lee los archivos CSV definidos en la configuración.
        2. Inserta los datos en las tablas correspondientes en la base de datos, reemplazando las tablas si existen.
        3. Registra cada paso y captura cualquier error que ocurra durante la migración.
        """
        self.logger.info("Iniciando migración de datos...")
        # Leer los datos de los archivos CSV
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
