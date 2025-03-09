from sqlalchemy import create_engine
from core.config_manager import ConfigManager

class DBManager:
    """
    DBManager se encarga de cargar la configuración de la base de datos y crear el motor SQLAlchemy.
    """
    def __init__(self, config_path="config.yaml"):
        # Carga la configuración utilizando ConfigManager
        self.config = ConfigManager.load_config(config_path)
        self.engine = self.create_engine()

    def create_engine(self):
        """
        Crea el motor SQLAlchemy basado en los parámetros de la base de datos definidos en la configuración.
        :return: Motor SQLAlchemy.
        """
        db_config = self.config['database']
        user = db_config['user']
        password = db_config['password']
        host = db_config['host']
        port = db_config['port']
        dbname = db_config['dbname']
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(db_url)
        return engine

    def get_engine(self):
        """
        Retorna el motor SQLAlchemy creado.
        :return: Motor SQLAlchemy.
        """
        return self.engine

    def close_connection(self):
        """
        Cierra todas las conexiones del pool del engine, liberando recursos.
        """
        if self.engine:
            self.engine.dispose()
