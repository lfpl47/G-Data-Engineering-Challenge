from sqlalchemy import create_engine
from core.config_manager import ConfigManager

class DBManager:
    """
    DBManager is responsible for loading the database configuration and creating the SQLAlchemy engine.
    """
    def __init__(self, config_path="config.yaml"):
        self.config = ConfigManager.load_config(config_path)
        self.engine = self.create_engine()

    def create_engine(self):
        """
        Creates the SQLAlchemy engine based on the database parameters defined in the configuration.
        :return: SQLAlchemy engine.
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
        Returns the created SQLAlchemy engine.
        :return: SQLAlchemy engine.
        """
        return self.engine

    def close_connection(self):
        """
        Closes all connections from the engine's pool, releasing resources.
        """
        if self.engine:
            self.engine.dispose()
