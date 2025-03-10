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
    Genera dinámicamente un esquema AVRO a partir de las columnas y tipos del DataFrame.
    
    Para simplificar, se utiliza la siguiente asignación de tipos:
      - int64           -> "long"
      - float64         -> "double"
      - object          -> "string"
      - datetime64[ns]  -> "string"
    
    :param table_name: Nombre de la tabla.
    :param df: DataFrame con los datos.
    :return: Esquema AVRO parseado.
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
        "doc": f"Backup de la tabla {table_name}",
        "name": table_name,
        "namespace": "backup.avro",
        "type": "record",
        "fields": fields
    }
    return parse_schema(schema)

def update_backup_config(table_name: str, backup_file: str, config_path="config.yaml"):
    """
    Actualiza el archivo de configuración agregando el backup_file en la sección backup.files.
    :param table_name: Nombre de la tabla.
    :param backup_file: Ruta del archivo de backup.
    :param config_path: Ruta al archivo de configuración YAML.
    """
    config = ConfigManager.load_config(config_path)
    if "backup" not in config:
        config["backup"] = {}
    if "files" not in config["backup"]:
        config["backup"]["files"] = {}
    config["backup"]["files"][table_name] = backup_file
    with open(config_path, "w") as file:
        yaml.dump(config, file)
    logger.info(f"Archivo de backup actualizado en config.yaml para la tabla {table_name}.")

def backup_table(table_name: str, config_path="config.yaml") -> None:
    """
    Realiza el backup de una tabla consultando los datos y guardándolos en un archivo AVRO.
    Actualiza el archivo de configuración con el path del backup generado.
    :param table_name: Nombre de la tabla a respaldar.
    :param config_path: Ruta al archivo YAML de configuración.
    """
    logger.info(f"Iniciando backup de la tabla {table_name}.")
    
    config = ConfigManager.load_config(config_path)
    backup_dir = config.get("backup", {}).get("backup_dir", "data/backup")
    
    db_manager = DBManager()
    engine = db_manager.get_engine()
    
    query = text(f"SELECT * FROM {table_name}")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    
    # Limpieza de datos: para cada columna de tipo object, reemplazar nulos y forzar a cadena
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
    
    logger.info(f"Backup de la tabla {table_name} completado en {backup_file}.")
    update_backup_config(table_name, backup_file, config_path)

def restore_table(table_name: str, config_path="config.yaml"):
    """
    Restaura los datos de una tabla a partir de un archivo AVRO.
    Si la tabla no existe, la crea usando la estructura definida en el YAML (todos los campos como TEXT).
    Si ya existe, se agregan los datos (modo "append").
    
    :param table_name: Nombre de la tabla a restaurar.
    :param config_path: Ruta al archivo de configuración YAML.
    """
    config = ConfigManager.load_config(config_path)
    backup_file = config.get("backup", {}).get("files", {}).get(table_name)
    if not backup_file or not os.path.exists(backup_file):
        logger.error(f"No se encontró backup para la tabla {table_name} en config.yaml.")
        return

    logger.info(f"Iniciando restauración de la tabla {table_name} desde {backup_file}.")
    records = []
    with open(backup_file, "rb") as fo:
        for record in reader(fo):
            records.append(record)
    
    if not records:
        logger.warning("No se encontraron registros en el backup.")
        return

    df = pd.DataFrame(records)
    db_manager = DBManager(config_path)
    engine = db_manager.get_engine()
    
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        logger.info(f"La tabla {table_name} no existe. Creándola usando la estructura definida en el YAML.")
        table_conf = config.get("csv", {}).get(table_name, {})
        columns = table_conf.get("columns", [])
        if not columns:
            logger.error(f"No se encontraron columnas definidas para la tabla {table_name} en config.yaml.")
            return
        metadata = MetaData()
        # Crear columnas como TEXT (puedes ajustar los tipos según lo necesites)
        cols = [Column(col, String) for col in columns]
        new_table = Table(table_name, metadata, *cols)
        metadata.create_all(engine)
        logger.info(f"Tabla {table_name} creada exitosamente.")
    
    # Si la tabla existe, simplemente se agregan los datos (append)
    with engine.begin() as connection:
        df.to_sql(table_name, con=connection, if_exists="append", index=False)
    logger.info(f"Restauración de la tabla {table_name} completada.")
    
    
def main():
    import sys
    config = ConfigManager.load_config()
    tables = list(config.get("csv", {}).keys())

    if len(sys.argv) < 2:
        logger.error("No se especificó la acción. Uso: python -m data.backup [backup|restore]")
        logger.info("Uso: python -m data.backup [backup|restore]")
        sys.exit(1)
    
    action = sys.argv[1].lower()
    logger.info(f"Acción solicitada: {action}")
    if action == "backup":
        for table in tables:
            logger.info(f"Realizando backup de la tabla {table}.")
            backup_table(table)
    elif action == "restore":
        for table in tables:
            logger.info(f"Restaurando la tabla {table}.")
            restore_table(table)
    else:
        logger.error("Acción inválida. Use 'backup' o 'restore'.")
        logger.info("Acción inválida. Use 'backup' o 'restore'.")
        sys.exit(1)

if __name__ == "__main__":
    main()

