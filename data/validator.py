import os
import json
import pandas as pd
from datetime import datetime
from core.custom_logger import CustomLogger
from core.config_manager import ConfigManager

logger = CustomLogger("Validator")

def validate_row(row: pd.Series, expected_columns: list) -> list:
    """
    Validates a single row by checking each expected column.
    Returns a list of errors if any required field is missing or invalid.
    :param row: A pandas Series representing a row of data.
    :param expected_columns: A list of required column names.
    :return: A list of error messages.
    """
    errors = []
    for col in expected_columns:
        value = row.get(col)
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
            errors.append(f"Field '{col}' is empty or null")
        if col == "datetime" and not pd.isna(value) and isinstance(value, str):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                errors.append("Invalid 'datetime' format")
    return errors

def validate_dataframe(df: pd.DataFrame, expected_columns: list, table_name: str, process_type: str) -> pd.DataFrame:
    """
    Iterates over the DataFrame to validate each row based on expected columns.
    Logs any invalid records and returns a DataFrame containing only the valid rows.
    :param df: The DataFrame to validate.
    :param expected_columns: List of required column names.
    :param table_name: The name of the table (for logging purposes).
    :param process_type: The type of process (e.g., "migration" or "ingestion").
    :return: A DataFrame with only valid rows.
    """
    error_log = []
    valid_indices = []

    for idx, row in df.iterrows():
        row_errors = validate_row(row, expected_columns)
        if row_errors:
            error_log.append({
                "table": table_name,
                "row_index": idx,
                "data": row.to_dict(),
                "errors": row_errors
            })
        else:
            valid_indices.append(idx)
    
    if error_log:
        config = ConfigManager.load_config()
        log_dir = config.get("logs", {}).get("path", "data/logs")
        os.makedirs(log_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        log_file = os.path.join(log_dir, f"{process_type}_error_log_{table_name}_{timestamp}.json")
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(error_log, f, indent=4, ensure_ascii=False, default=str)
        logger.error(f"Validation errors found in table '{table_name}'. Check the log file: {log_file}")
    
    return df.loc[valid_indices]

def process_and_insert(df: pd.DataFrame, expected_columns: list, table_name: str, engine, process_type: str):
    """
    Validates the DataFrame, logs any errors, and inserts the valid records into the database.
    :param df: The DataFrame to process.
    :param expected_columns: A list of required columns.
    :param table_name: The name of the table.
    :param engine: The SQLAlchemy engine for database connection.
    :param process_type: The type of process ("migration" or "ingestion").
    """
    df_valid = validate_dataframe(df, expected_columns, table_name, process_type)
    if df_valid.empty:
        logger.error(f"No valid records found for table {table_name}.")
        return

    with engine.begin() as connection:
        df_valid.to_sql(table_name, con=connection, if_exists="append", index=False)
    logger.info(f"{len(df_valid)} valid records inserted into table {table_name}.")
