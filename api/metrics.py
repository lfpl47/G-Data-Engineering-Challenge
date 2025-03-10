from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from core.db_manager import DBManager
from core.config_manager import ConfigManager
import pandas as pd
from datetime import datetime
import plotly.express as px
import plotly.io as pio
from core.security import get_current_user  # Import dependency to protect endpoints

router = APIRouter()
db_manager = DBManager()

def get_engine():
    return db_manager.get_engine()

@router.get("/metrics/hired_by_quarter", summary="Employees hired by department and job, divided by quarter (2021)")
def hired_by_quarter(
    engine = Depends(get_engine),
    format: str = Query("json", description="Output format: 'json' or 'html'"),
    current_user: dict = Depends(get_current_user)
):
    """
    Returns the number of employees hired in 2021 by department and job,
    divided by quarter.
    If 'html' is requested, it returns a table along with an interactive chart.
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
        result = result.fillna("")

        if format.lower() == "html":
            table_html = result.to_html(index=False, justify="left")
            # Create a combined column for chart labels
            result['Dept - Job'] = result['department'] + " - " + result['job']
            fig = px.bar(result, x='Dept - Job', y=['Q1', 'Q2', 'Q3', 'Q4'],
                         title="Quarterly Hires (2021) - Details by Department and Job",
                         labels={"value": "Hires", "Dept - Job": "Department - Job"})
            fig.update_layout(barmode='stack', xaxis_tickangle=-45)
            chart_html = pio.to_html(fig, full_html=False)

            html_content = f"""
            <html>
            <head>
                <meta charset="UTF-8"/>
                <title>Hired by Quarter</title>
            </head>
            <body>
                <h1>Employees Hired by Quarter (2021)</h1>
                {table_html}
                <br/><br/>
                {chart_html}
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        else:
            return result.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/metrics/departments_above_mean", summary="Departments with hires above the overall average in 2021")
def departments_above_mean(
    engine = Depends(get_engine),
    format: str = Query("json", description="Output format: 'json' or 'html'"),
    current_user: dict = Depends(get_current_user)
):
    """
    Returns a list of departments that hired more than the overall average in 2021.
    If 'html' is requested, it displays a table along with an interactive chart.
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
        result = result[['id', 'department', 'hired']]
        result = result.sort_values(by='hired', ascending=False)
        result = result.fillna("")

        if format.lower() == "html":
            table_html = result.to_html(index=False, justify="left")
            fig = px.bar(result, x='hired', y='department', orientation='h',
                         title="Departments with Hires Above Average (2021)",
                         labels={"department": "Department", "hired": "Hires"})
            chart_html = pio.to_html(fig, full_html=False)
            
            html_content = f"""
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Departments Above Average</title>
            </head>
            <body>
                <h1>Departments with Hires Above Average (2021)</h1>
                {table_html}
                <br/><br/>
                {chart_html}
            </body>
            </html>
            """
            return HTMLResponse(content=html_content)
        else:
            return result.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
