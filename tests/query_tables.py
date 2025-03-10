import sys
import os
from sqlalchemy import text, inspect
from core.db_manager import DBManager
from core.custom_logger import CustomLogger
from core.config_manager import ConfigManager
from typing import List
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

logger = CustomLogger("QueryTables")

def get_expected_columns(table_name: str) -> List[str]:
    """
    Retorna la lista de columnas esperadas para la tabla, según lo definido en la sección 'csv' del YAML.
    """
    config = ConfigManager.load_config()
    return config['csv'][table_name]['columns']

def validate_dataframe(df: pd.DataFrame, expected_columns: List[str], table_name: str) -> None:
    """
    Valida que el DataFrame no contenga registros con valores nulos en las columnas esperadas.
    Si se encuentran, se loguean los registros inválidos.
    :param df: DataFrame a validar.
    :param expected_columns: Lista de columnas requeridas.
    :param table_name: Nombre de la tabla (para los logs).
    """
    invalid = df[df[expected_columns].isnull().any(axis=1)]
    if not invalid.empty:
        logger.error(f"Se encontraron registros inválidos (con nulls) en {table_name}:")
        logger.error(invalid)
    else:
        logger.info(f"No se encontraron registros inválidos en {table_name}.")

def query_table(table_name: str, engine):
    """
    Consulta todos los registros de la tabla indicada, valida que no contengan nulos
    en las columnas esperadas y los imprime.
    :param table_name: Nombre de la tabla a consultar.
    :param engine: Motor SQLAlchemy para conectarse a la base de datos.
    """
    logger.info(f"Consultando la tabla '{table_name}'...")
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()
        
        insp = inspect(engine)
        columns_info = insp.get_columns(table_name)
        col_names = [col["name"] for col in columns_info]
        
        df = pd.DataFrame(rows, columns=col_names)
    
    expected_cols = get_expected_columns(table_name)
    validate_dataframe(df, expected_cols, table_name)
    
    logger.info(f"Registros en '{table_name}':")
    for row in rows:
        print(row)

def main():
    db_manager = DBManager()
    engine = db_manager.get_engine()

    config = ConfigManager.load_config()
    tables = list(config.get("csv", {}).keys())

    for table in tables:
        query_table(table, engine)

if __name__ == "__main__":
    main()
