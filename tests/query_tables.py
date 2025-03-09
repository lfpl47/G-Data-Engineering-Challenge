import sys
import os
from sqlalchemy import text
from core.db_manager import DBManager
from core.custom_logger import CustomLogger

# Aseguramos que el directorio raíz del proyecto esté en el sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

logger = CustomLogger("QueryTables")

def query_table(table_name: str, engine):
    """
    Consulta todos los registros de la tabla indicada y los imprime.
    
    :param table_name: Nombre de la tabla a consultar.
    :param engine: Motor SQLAlchemy para conectarse a la base de datos.
    """
    logger.info(f"Consultando la tabla '{table_name}'...")
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()
        logger.info(f"Registros en '{table_name}':")
        for row in rows:
            print(row)

def main():
    # Inicializamos DBManager para obtener la conexión a la base de datos
    db_manager = DBManager()
    engine = db_manager.get_engine()

    # Lista de tablas a consultar
    tables = ["hired_employees", "departments", "jobs"]

    for table in tables:
        query_table(table, engine)

if __name__ == "__main__":
    main()
