from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, Field, field_validator
from typing import List
import pandas as pd
from core.custom_logger import CustomLogger
from core.db_manager import DBManager
from datetime import datetime, timedelta
from core.config_manager import ConfigManager
from data.backup import backup_table, restore_table
from data.migration import Migration
import traceback
from data.validator import process_and_insert 
from api.metrics import router as metrics_router
from core.security import token_endpoint, get_current_user  # Import the token endpoint and dependency

app = FastAPI(
    title="Data Ingestion API", 
    description="API for ingesting new data and managing backups/migration",
    version="1.0")

app.include_router(metrics_router)
logger = CustomLogger("MainAPI")

@app.post("/token", summary="Obtain JWT Token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    return token_endpoint(form_data)

class HiredEmployee(BaseModel):
    id: int = Field(..., description="Unique identifier for the employee")
    name: str = Field(..., description="Employee's full name")
    datetime: str = Field(..., description="Hire date and time in ISO format")
    department_id: int = Field(..., description="Department identifier")
    job_id: int = Field(..., description="Job identifier")

    @field_validator("datetime")
    @classmethod
    def validate_datetime(cls, value: str) -> str:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("The 'datetime' field must be in ISO format.")
        return value

class Department(BaseModel):
    id: int = Field(..., description="Unique identifier for the department")
    department: str = Field(..., description="Name of the department")

class Job(BaseModel):
    id: int = Field(..., description="Unique identifier for the job")
    job: str = Field(..., description="Name of the job")

class IngestData(BaseModel):
    """
    Model that groups the data for ingestion into a single request.
    Each list must contain between 1 and 1000 records.
    """
    hired_employees: List[HiredEmployee] = Field(..., min_items=1, max_items=1000)
    departments: List[Department] = Field(..., min_items=1, max_items=1000)
    jobs: List[Job] = Field(..., min_items=1, max_items=1000)

def get_db_engine():
    db_manager = DBManager()
    return db_manager.get_engine()

def get_expected_columns(table_name: str) -> List[str]:
    config = ConfigManager.load_config()
    return config['csv'][table_name]['columns']

@app.post("/ingest", summary="Ingest new data", response_model=dict)
def ingest_data(
    data: IngestData, 
    engine = Depends(get_db_engine),
    current_user: dict = Depends(get_current_user)):
    logger.info(f"Ingestion request received from: {current_user['username']}")
    try:
        # Convert models to DataFrames
        df_hired = pd.DataFrame([emp.model_dump() for emp in data.hired_employees])
        df_departments = pd.DataFrame([dep.model_dump() for dep in data.departments])
        df_jobs = pd.DataFrame([job.model_dump() for job in data.jobs])
        
        # Validate and insert each table using the validation process ("ingestion")
        expected_hired = set(get_expected_columns("hired_employees"))
        if set(df_hired.columns) != expected_hired:
            logger.error(f"Columns of hired_employees do not match. Expected: {expected_hired}, Received: {set(df_hired.columns)}")
            raise HTTPException(status_code=400, detail="Data format error in hired_employees")
        process_and_insert(df_hired, list(expected_hired), "hired_employees", engine, process_type="ingestion")
        
        expected_dept = set(get_expected_columns("departments"))
        if set(df_departments.columns) != expected_dept:
            logger.error(f"Columns of departments do not match. Expected: {expected_dept}, Received: {set(df_departments.columns)}")
            raise HTTPException(status_code=400, detail="Data format error in departments")
        process_and_insert(df_departments, list(expected_dept), "departments", engine, process_type="ingestion")
        
        expected_jobs = set(get_expected_columns("jobs"))
        if set(df_jobs.columns) != expected_jobs:
            logger.error(f"Columns of jobs do not match. Expected: {expected_jobs}, Received: {set(df_jobs.columns)}")
            raise HTTPException(status_code=400, detail="Data format error in jobs")
        process_and_insert(df_jobs, list(expected_jobs), "jobs", engine, process_type="ingestion")
        
        logger.info("Data ingested successfully into the database.")
        return {"message": "Data ingested successfully"}
    except Exception as e:
        logger.error(f"Error ingesting data: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Error ingesting data")

@app.post("/backup", summary="Perform table backup")
def backup_endpoint(
    table: str = Query(None, description="Name of the table to backup. If omitted, all tables will be backed up."),
    current_user: dict = Depends(get_current_user)
):
    try:
        config = ConfigManager.load_config()
        if table:
            logger.info(f"Performing backup for table {table}.")
            backup_table(table)
            return {"message": f"Backup for {table} completed."}
        else:
            tables = list(config.get("csv", {}).keys())
            for tbl in tables:
                logger.info(f"Performing backup for table {tbl}.")
                backup_table(tbl)
            return {"message": "Backup for all tables completed."}
    except Exception as e:
        logger.error(f"Backup error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/restore", summary="Restore tables from backup")
def restore_endpoint(
    table: str = Query(None, description="Name of the table to restore. If omitted, all tables will be restored."),
    current_user: dict = Depends(get_current_user)
):
    try:
        config = ConfigManager.load_config()
        if table:
            logger.info(f"Restoring table {table}.")
            restore_table(table)
            return {"message": f"Restoration for {table} completed."}
        else:
            tables = list(config.get("csv", {}).keys())
            for tbl in tables:
                logger.info(f"Restoring table {tbl}.")
                restore_table(tbl)
            return {"message": "Restoration for all tables completed."}
    except Exception as e:
        logger.error(f"Restoration error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/migrate", summary="Migrate CSV data to the database")
def migrate_endpoint(current_user: dict = Depends(get_current_user)):
    try:
        migration = Migration()
        migration.migrate_data()
        return {"message": "Migration completed successfully."}
    except Exception as e:
        logger.error(f"Migration endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
