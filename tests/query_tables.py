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
    Returns the list of expected columns for the table, as defined in the 'csv' section of the YAML.
    :param table_name: Name of the table.
    :return: List of expected column names.
    """
    config = ConfigManager.load_config()
    return config['csv'][table_name]['columns']

def validate_dataframe(df: pd.DataFrame, expected_columns: List[str], table_name: str) -> None:
    """
    Validates that the DataFrame does not contain any records with null values in the expected columns.
    If any are found, the invalid records are logged.
    :param df: DataFrame to validate.
    :param expected_columns: List of required columns.
    :param table_name: Name of the table (for logging purposes).
    """
    invalid = df[df[expected_columns].isnull().any(axis=1)]
    if not invalid.empty:
        logger.error(f"Invalid records (with nulls) found in {table_name}:")
        logger.error(invalid)
    else:
        logger.info(f"No invalid records found in {table_name}.")

def query_table(table_name: str, engine):
    """
    Queries all records from the specified table, validates that they do not contain null values
    in the expected columns, and prints them.
    :param table_name: Name of the table to query.
    :param engine: SQLAlchemy engine to connect to the database.
    """
    logger.info(f"Querying table '{table_name}'...")
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT * FROM {table_name}"))
        rows = result.fetchall()
        
        insp = inspect(engine)
        columns_info = insp.get_columns(table_name)
        col_names = [col["name"] for col in columns_info]
        
        df = pd.DataFrame(rows, columns=col_names)
    
    expected_cols = get_expected_columns(table_name)
    validate_dataframe(df, expected_cols, table_name)
    
    logger.info(f"Records in '{table_name}':")
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
