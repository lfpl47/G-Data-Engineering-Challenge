from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field, field_validator
from typing import List
import pandas as pd
from core.custom_logger import CustomLogger
from core.db_manager import DBManager
from datetime import datetime
from core.config_manager import ConfigManager
import traceback

app = FastAPI(title="Data Ingestion API", description="API para ingesta de nuevos datos", version="1.0")
logger = CustomLogger("FastAPI")

class HiredEmployee(BaseModel):
    id: int = Field(..., description="Identificador único del empleado")
    name: str = Field(..., description="Nombre y apellido del empleado")
    datetime: str = Field(..., description="Fecha y hora de contratación en formato ISO")
    department_id: int = Field(..., description="Identificador del departamento")
    job_id: int = Field(..., description="Identificador del puesto de trabajo")

    @field_validator("datetime")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        """
        Valida que el campo 'datetime' esté en formato ISO.
        Se permite la 'Z' para designar zona UTC.
        """
        try:
            # Reemplaza 'Z' por '+00:00' para que fromisoformat lo entienda
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("El campo 'datetime' debe estar en formato ISO.")
        return value

class Department(BaseModel):
    id: int = Field(..., description="Identificador único del departamento")
    department: str = Field(..., description="Nombre del departamento")

class Job(BaseModel):
    id: int = Field(..., description="Identificador único del puesto")
    job: str = Field(..., description="Nombre del puesto")

class IngestData(BaseModel):
    """
    Modelo que agrupa los datos para la ingesta en un solo request.
    Se requiere que cada lista contenga entre 1 y 1000 registros.
    """
    hired_employees: List[HiredEmployee] = Field(..., min_items=1, max_items=1000)
    departments: List[Department] = Field(..., min_items=1, max_items=1000)
    jobs: List[Job] = Field(..., min_items=1, max_items=1000)

def get_db_engine():
    """
    Dependencia que retorna el engine de la base de datos usando DBManager.
    """
    db_manager = DBManager()
    return db_manager.get_engine()

def get_expected_columns(table_name: str) -> List[str]:
    """
    Retorna la lista de columnas esperadas para la tabla indicada,
    según lo definido en la sección 'csv' del archivo de configuración.
    """
    config = ConfigManager.load_config()
    return config['csv'][table_name]['columns']

@app.post("/ingest", summary="Ingesta de nuevos datos", response_model=dict)
def ingest_data(data: IngestData, engine = Depends(get_db_engine)):
    logger.info("Solicitud recibida para ingesta de datos.")
    try:

        df_hired = pd.DataFrame([emp.model_dump() for emp in data.hired_employees])
        df_departments = pd.DataFrame([dep.model_dump() for dep in data.departments])
        df_jobs = pd.DataFrame([job.model_dump() for job in data.jobs])
        
        expected_hired = set(get_expected_columns("hired_employees"))
        if set(df_hired.columns) != expected_hired:
            logger.error(f"Las columnas de hired_employees no coinciden. Esperado: {expected_hired}, Recibido: {set(df_hired.columns)}")
            raise HTTPException(status_code=400, detail="Error en el formato de datos de hired_employees")
        
        expected_dept = set(get_expected_columns("departments"))
        if set(df_departments.columns) != expected_dept:
            logger.error(f"Las columnas de departments no coinciden. Esperado: {expected_dept}, Recibido: {set(df_departments.columns)}")
            raise HTTPException(status_code=400, detail="Error en el formato de datos de departments")
        
        expected_jobs = set(get_expected_columns("jobs"))
        if set(df_jobs.columns) != expected_jobs:
            logger.error(f"Las columnas de jobs no coinciden. Esperado: {expected_jobs}, Recibido: {set(df_jobs.columns)}")
            raise HTTPException(status_code=400, detail="Error en el formato de datos de jobs")
        
        with engine.begin() as connection:
            df_hired.to_sql("hired_employees", con=connection, if_exists="append", index=False)
            df_departments.to_sql("departments", con=connection, if_exists="append", index=False)
            df_jobs.to_sql("jobs", con=connection, if_exists="append", index=False)
        
        logger.info("Datos ingresados correctamente en la base de datos.")
        return {"message": "Data ingested successfully"}
    except Exception as e:
        logger.error(f"Error al ingerir datos: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error ingesting data")
