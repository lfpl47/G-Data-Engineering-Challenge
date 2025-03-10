from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from core.db_manager import DBManager
from core.config_manager import ConfigManager
import pandas as pd
from datetime import datetime

router = APIRouter()
db_manager = DBManager()

def get_engine():
    return db_manager.get_engine()

@router.get("/metrics/hired_by_quarter", summary="Empleados contratados por departamento y puesto, divididos por trimestre (2021)")
def hired_by_quarter(
    engine = Depends(get_engine),
    format: str = Query("json", description="Formato de salida: 'json' o 'html'")):
    """
    Retorna el número de empleados contratados por trimestre (2021),
    ordenados por department y job.
    Parámetros:
    - format: 'json' (por defecto) o 'html'
    """
    try:
        df = pd.read_sql("SELECT * FROM hired_employees", con=engine)
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df = df[df['datetime'].dt.year == 2021]
        
        df['quarter'] = df['datetime'].dt.quarter
        
        grouped = df.groupby(['department_id', 'job_id', 'quarter']).size().reset_index(name='count')
        pivot = grouped.pivot_table(
            index=['department_id', 'job_id'], 
            columns='quarter', 
            values='count', 
            fill_value=0
        )
        pivot.columns = [f"Q{col}" for col in pivot.columns]
        pivot.reset_index(inplace=True)
        
        dept_df = pd.read_sql("SELECT * FROM departments", con=engine)
        jobs_df = pd.read_sql("SELECT * FROM jobs", con=engine)
        result = pivot.merge(dept_df, left_on='department_id', right_on='id', how='left')
        result = result.merge(jobs_df, left_on='job_id', right_on='id', how='left', suffixes=('_dept', '_job'))
        
        result = result[['department', 'job', 'Q1', 'Q2', 'Q3', 'Q4']]
        result = result.sort_values(by=['department', 'job'])
        
        if format.lower() == "html":
            table_html = result.to_html(index=False, justify="left")
            html_content = f"""
            <html>
            <head>
                <meta charset="UTF-8"/>
                <title>Hired by Quarter</title>
            </head>
            <body>
                <h1>Empleados Contratados por Trimestre (2021)</h1>
                {table_html}
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        else:
            return result.fillna("").to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/departments_above_mean", summary="Departamentos que contrataron más que el promedio general en 2021")
def departments_above_mean(
    engine = Depends(get_engine),
    format: str = Query("json", description="Formato de salida: 'json' o 'html'")):
    """
    Devuelve la lista de departamentos que contrataron más que el promedio general de 2021.
    Salida:
    - JSON (por defecto): Lista de diccionarios con 'id', 'department' y 'hired'
    - HTML: Una tabla con la información
    """
    try:
        df = pd.read_sql("SELECT * FROM hired_employees", con=engine)
        df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
        df = df[df['datetime'].dt.year == 2021]
        dept_counts = df.groupby('department_id').size().reset_index(name='hired')
        mean_hired = dept_counts['hired'].mean()
        above_mean = dept_counts[dept_counts['hired'] > mean_hired]
        dept_df = pd.read_sql("SELECT * FROM departments", con=engine)
        result = above_mean.merge(dept_df, left_on='department_id', right_on='id', how='left')
        result = result[['id', 'department', 'hired']].sort_values(by='hired', ascending=False)
        
        if format.lower() == "html":
            table_html = result.to_html(index=False, justify="left")
            html_content = f"""
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Departments Above Mean (2021)</title>
            </head>
            <body>
                <h1>Departamentos con contrataciones superiores al promedio (2021)</h1>
                {table_html}
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        else:
            return result.fillna("").to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))