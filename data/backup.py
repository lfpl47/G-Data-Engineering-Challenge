import os
import yaml
import pandas as pd
from fastavro import writer, reader, parse_schema
from sqlalchemy import text, inspect, MetaData, Table, Column, String
from core.db_manager import DBManager
from core.custom_logger import CustomLogger
from core.config_manager import ConfigManager

logger = CustomLogger("BackupRestore")

def generate_avro_schema(table_name: str, df: pd.DataFrame) -> dict:
    """
    Dynamically generates an AVRO schema based on the DataFrame's columns and types.
    :param table_name: Name of the table.
    :param df: DataFrame containing the data.
    :return: Parsed AVRO schema.
    """
    type_mapping = {
        'int64': 'long',
        'float64': 'double',
        'object': 'string',
        'datetime64[ns]': 'string'
    }
    fields = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        avro_type = type_mapping.get(dtype, 'string')
        fields.append({"name": col, "type": avro_type})
    schema = {
        "doc": f"Backup for table {table_name}",
        "name": table_name,
        "namespace": "backup.avro",
        "type": "record",
        "fields": fields
    }
    return parse_schema(schema)

def update_backup_config(table_name: str, backup_file: str, config_path="config.yaml"):
    """
    Updates the configuration file by adding the backup_file path in the backup.files section.
    :param table_name: Name of the table.
    :param backup_file: Path of the backup file.
    :param config_path: Path to the YAML configuration file.
    """
    config = ConfigManager.load_config(config_path)
    if "backup" not in config:
        config["backup"] = {}
    if "files" not in config["backup"]:
        config["backup"]["files"] = {}
    config["backup"]["files"][table_name] = backup_file
    with open(config_path, "w") as file:
        yaml.dump(config, file)
    logger.info(f"Backup file updated in config.yaml for table {table_name}.")

def backup_table(table_name: str, config_path="config.yaml") -> None:
    """
    Backs up a table by querying its data and saving it to an AVRO file.
    Also updates the configuration file with the backup file path.
    :param table_name: Name of the table to backup.
    :param config_path: Path to the YAML configuration file.
    """
    logger.info(f"Starting backup for table {table_name}.")
    
    config = ConfigManager.load_config(config_path)
    backup_dir = config.get("backup", {}).get("backup_dir", "data/backup")
    
    db_manager = DBManager()
    engine = db_manager.get_engine()
    
    query = text(f"SELECT * FROM {table_name}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str)
    if "datetime" in df.columns:
        df["datetime"] = df["datetime"].fillna("").astype(str)
    
    schema = generate_avro_schema(table_name, df)
    records = df.to_dict(orient="records")
    
    os.makedirs(backup_dir, exist_ok=True)
    backup_file = os.path.join(backup_dir, f"{table_name}.avro")
    with open(backup_file, "wb") as out:
        writer(out, schema, records)
    
    logger.info(f"Backup for table {table_name} completed at {backup_file}.")
    update_backup_config(table_name, backup_file, config_path)

def restore_table(table_name: str, config_path="config.yaml"):
    """
    Restores a table's data from an AVRO backup file.
    If the table does not exist, it is created using the structure defined in the YAML (all fields as TEXT).
    If it exists, the data is appended.
    :param table_name: Name of the table to restore.
    :param config_path: Path to the YAML configuration file.
    """
    config = ConfigManager.load_config(config_path)
    backup_file = config.get("backup", {}).get("files", {}).get(table_name)
    if not backup_file or not os.path.exists(backup_file):
        logger.error(f"No backup found for table {table_name} in config.yaml.")
        return

    logger.info(f"Starting restoration for table {table_name} from {backup_file}.")
    records = []
    with open(backup_file, "rb") as fo:
        for record in reader(fo):
            records.append(record)
    
    if not records:
        logger.warning("No records found in the backup.")
        return

    df = pd.DataFrame(records)
    db_manager = DBManager(config_path)
    engine = db_manager.get_engine()
    
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        logger.info(f"Table {table_name} does not exist. Creating it using the structure defined in the YAML.")
        table_conf = config.get("csv", {}).get(table_name, {})
        columns = table_conf.get("columns", [])
        if not columns:
            logger.error(f"No columns defined for table {table_name} in config.yaml.")
            return
        metadata = MetaData()
        # Create columns as TEXT (you can adjust types as needed)
        cols = [Column(col, String) for col in columns]
        new_table = Table(table_name, metadata, *cols)
        metadata.create_all(engine)
        logger.info(f"Table {table_name} created successfully.")
    
    with engine.begin() as connection:
        df.to_sql(table_name, con=connection, if_exists="append", index=False)
    logger.info(f"Restoration for table {table_name} completed.")
    
def main():
    import sys
    config = ConfigManager.load_config()
    tables = list(config.get("csv", {}).keys())

    if len(sys.argv) < 2:
        logger.error("No action specified. Usage: python -m data.backup [backup|restore]")
        logger.info("Usage: python -m data.backup [backup|restore]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    logger.info(f"Requested action: {action}")
    if action == "backup":
        for table in tables:
            logger.info(f"Performing backup for table {table}.")
            backup_table(table)
    elif action == "restore":
        for table in tables:
            logger.info(f"Restoring table {table}.")
            restore_table(table)
    else:
        logger.error("Invalid action. Use 'backup' or 'restore'.")
        logger.info("Invalid action. Use 'backup' or 'restore'.")
        sys.exit(1)

if __name__ == "__main__":
    main()
