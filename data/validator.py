# data/validator.py
import os
import json
import pandas as pd
from datetime import datetime
from core.custom_logger import CustomLogger
from core.config_manager import ConfigManager

logger = CustomLogger("Validator")

def validate_row(row: pd.Series, expected_columns: list) -> list:
    errors = []
    for col in expected_columns:
        value = row.get(col)
        if pd.isna(value) or (isinstance(value, str) and value.strip() == ""):
            errors.append(f"Campo '{col}' vacío o nulo")
        if col == "datetime" and not pd.isna(value) and isinstance(value, str):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                errors.append("Formato de 'datetime' inválido")
    return errors

def validate_dataframe(df: pd.DataFrame, expected_columns: list, table_name: str, process_type: str) -> pd.DataFrame:
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
        log_file = os.path.join(log_dir, f"{process_type}_log_errores_{table_name}_{timestamp}.json")
        with open(log_file, "w", encoding="utf-8") as f:
            json.dump(error_log, f, indent=4, ensure_ascii=False, default=str)
        logger.error(f"Se encontraron errores en la validación de la tabla '{table_name}'. Verifica {log_file}")
    
    return df.loc[valid_indices]

def process_and_insert(df: pd.DataFrame, expected_columns: list, table_name: str, engine, process_type: str):
    df_valid = validate_dataframe(df, expected_columns, table_name, process_type)
    if df_valid.empty:
        logger.error(f"Ningún registro válido para la tabla {table_name}.")
        return

    with engine.begin() as connection:
        df_valid.to_sql(table_name, con=connection, if_exists="append", index=False)
    logger.info(f"Se insertaron {len(df_valid)} registros válidos en la tabla {table_name}.")
