# Globant Data Engineering Challenge

This project is a comprehensive solution for the Globant technical challenge, which consists of two main challenges:

1.  **Challenge 1:**
    * **Historical Data Migration:**
        * Reads CSV files, validates records (filtering incomplete or erroneous data), and inserts them into a PostgreSQL database. Additionally, it records errors in JSON files within the logs folder.
    * **New Data Ingestion:**
        * Offers a REST API (using FastAPI) for batch ingestion (between 1 and 1000 records) of data from the `hired_employees`, `departments`, and `jobs` tables. Model-level (Pydantic) and DataFrame-level (using the `validator.py` module) validations are applied.
    * **Backup and Restore:**
        * Allows performing backups of tables in AVRO format and restoring them later through specific endpoints.

2.  **Challenge 2:**
    * **Hiring Metrics:**
        * An endpoint to obtain the number of employees hired in 2021 by department and job, divided by quarter (Q1, Q2, Q3, and Q4).
        * Another endpoint that lists the departments that exceed the average hiring in 2021.
    * **Both endpoints offer two output formats:**
        * **JSON:** Ideal for integration with front-ends or services.
        * **HTML:** Displays a results table and, below, an interactive graph (generated with Plotly) for dynamic visualization.

Additionally, the project implements authentication using OAuth2 with JWT to protect sensitive endpoints.

---

## Table of Contents

* [Project Structure](#project-structure)
* [Requirements and Functionalities](#requirements-and-functionalities)
* [Main Endpoints](#main-endpoints)
* [Authentication and Security](#authentication-and-security)
* [Configuration and Dependencies](#configuration-and-dependencies)
* [Execution and Deployment](#execution-and-deployment)
* [Tests](#tests)
* [Future Improvements](#future-improvements)
* [Conclusion](#conclusion)

---

## Project Structure

| **Path** | **Description** |
| --- | --- |
| `Dockerfile` | Docker image definition for the application. |
| `README.md` | This file. |
| `api/` | Folder containing REST API endpoints. |
| &nbsp;&nbsp;`__init__.py` | API module initialization. |
| &nbsp;&nbsp;`main.py` | Main REST API: ingestion, migration, backup/restore, and metrics endpoints. |
| &nbsp;&nbsp;`metrics.py` | Metrics endpoints (hiring by quarter and departments above average) with JSON/HTML output and interactive graphs. |
| `config.yaml` | Central configuration (paths for CSV, backups, database, and logs). |
| `core/` | Core project components. |
| &nbsp;&nbsp;`__init__.py` | Core module initialization. |
| &nbsp;&nbsp;`config_manager.py` | Configuration management (reading `config.yaml`). |
| &nbsp;&nbsp;`custom_logger.py` | Custom logger for tracking and debugging. |
| &nbsp;&nbsp;`db_manager.py` | PostgreSQL connection management (SQLAlchemy). |
| &nbsp;&nbsp;`security.py` | OAuth2 and JWT implementation for authentication. |
| `data/` | Data management: migration, backup, and validation. |
| &nbsp;&nbsp;`__init__.py` | Data module initialization. |
| &nbsp;&nbsp;`backup/` | Backup files in AVRO format. |
| &nbsp;&nbsp;`logs/` | Validation logs (JSON files, ignored in `.gitignore`). |
| &nbsp;&nbsp;`migration.py` | CSV data migration to the database (uses the `validator.py` module). |
| &nbsp;&nbsp;`sources/` | Source CSV files. |
| &nbsp;&nbsp;`validator.py` | Record validation: checks each row, records errors, and filters invalid data. |
| `docker-compose.yaml` | Container orchestration (API and PostgreSQL). |
| `requirements.txt` | Project dependencies. |
| `script.txt` | Script with usage and test commands (docker, psql, migration, backup, etc.). |
| `tests/` | Tests and examples. |
| &nbsp;&nbsp;`__init__.py` | Tests module initialization. |
| &nbsp;&nbsp;`payload.json` | Example payload for the `/ingest` endpoint. |
| &nbsp;&nbsp;`query_tables.py` | Script to query tables and verify records. |

---

## Requirements and Functionalities

### Challenge 1

* **Historical Data Migration:**
    * **Reading and Validation:**
        * CSV files defined in `config.yaml` (located in `data/sources`) are read and records are validated using `data/validator.py`. Invalid records are excluded and recorded in JSON files within `data/logs`.
    * **Database Insertion:**
        * Valid data is inserted into PostgreSQL using SQLAlchemy and transactions to ensure data integrity.
* **New Data Ingestion:**
    * **`/ingest` Endpoint:**
        * Receives a structured JSON payload and uses Pydantic models to validate data structure. Additional validation is performed in `data/validator.py` before inserting records.
    * **Batch Insertion:**
        * Allows inserting 1 to 1000 records per table, ensuring that only complete and correct data is inserted.
* **Backup and Restore:**
    * **`/backup` Endpoint:**
        * Allows generating backups of a specific table (or all tables) in AVRO format, updating the configuration with the backup path.
    * **`/restore` Endpoint:**
        * Allows restoring data from AVRO files, creating the table if it does not exist or appending if it already exists.

### Challenge 2

* **Hiring Metrics by Quarter:**
    * **`/metrics/hired_by_quarter` Endpoint:**
        * Filters 2021 records, groups by `department_id` and `job_id`, and pivots data to obtain the number of hires in quarters Q1, Q2, Q3, and Q4.
    * **Configurable Output:**
        * Allows obtaining output in **JSON** or **HTML**. When HTML is selected, a results table and, below, an interactive graph (stacked bars) generated with Plotly are displayed.
* **Departments Above Average:**
    * **`/metrics/departments_above_mean` Endpoint:**
        * Calculates the average hiring in 2021 and filters departments that exceed that average, displaying the list in descending order.
    * **Configurable Output:**
        * Response can be obtained in **JSON** or **HTML** with a table and an interactive graph (horizontal bars).

---

## Main Endpoints

### Authentication and Security

* **POST /token:**
    * Allows obtaining a JWT token using OAuth2 (user: `admin`, password: `secret`). This token must be included in the `Authorization: Bearer <token>` header to access protected endpoints.

### Ingestion and Migration

* **POST /ingest:**
    * Receives data in JSON format for defined tables. Requires a valid JWT token.
* **POST /migrate:**
    * Executes the CSV file migration process to the database. Requires authentication.

### Backup and Restore

* **POST /backup:**
    * Performs backup of all tables or a specific table (using the `table` parameter). Requires authentication.
* **POST /restore:**
    * Restores one or all tables from generated backups. Requires authentication.

### Metrics (Challenge 2)

* **GET /metrics/hired_by_quarter:**
    * Returns a report of hires by quarter in 2021, grouped by department and job.
    * `format` parameter: `json` (default) or `html` to obtain output in table format with an interactive graph.
* **GET /metrics/departments_above_mean:**
    * Returns the list of departments that exceed the average hiring in 2021.
    * `format` parameter: `json` or `html`.

---

## Authentication and Security

Security is implemented using OAuth2 and JWT:

* **`/token` Endpoint:**
    * Allows obtaining a JWT token by sending credentials (user: `admin`, password: `secret`).
* **Endpoint Protection:**
    * Critical endpoints (ingestion, migration, backup, restore, and metrics) require sending the token in the authorization header.
* **Secret Key:**
    * The key to sign tokens is obtained from the `SECRET_KEY` environment variable, configured in the deployment environment or in Docker Compose.

---

## Configuration and Dependencies

* **config.yaml:**
    * Defines the configuration of:
        * **Backups:** Destination path for AVRO files.
        * **CSV:** Paths and columns for each table.
        * **Database:** PostgreSQL connection parameters.
        * **Logs:** Path to store validation log files.
* **requirements.txt:**
    * Lists the necessary dependencies, including:
        * FastAPI, Uvicorn, SQLAlchemy, Pandas, python-multipart, Plotly, python-jose[cryptography], and others.

---

## Execution and Deployment

### With Docker Compose

1.  **Build and start the containers:**

    ```bash
    docker-compose up --build
    ```

2.  **Verify application logs:**

    ```bash
    docker-compose logs -f app
    ```

3.  **Stop and clean up containers:**

    ```bash
    docker-compose down
    ```

### Local Execution

1.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Start the application with Uvicorn:**

    ```bash
    uvicorn api.main:app --host 0.0.0.0 --port 8000
    ```

### Tests

1.  **Get a JWT token:**

    ```bash
    curl -X POST "http://localhost:8000/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "username=admin&password=secret"
    ```

2.  **Test data ingestion:**

    ```bash
    curl -X POST "http://localhost:8000/ingest" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer <TU_TOKEN>" \
        -d @tests/payload.json
    ```

    * Replace `<TU_TOKEN>` with the obtained JWT token.

3.  **Test data migration:**

    ```bash
    curl -X POST "http://localhost:8000/migrate" \
        -H "Authorization: Bearer <TU_TOKEN>" \
        -H "Content-Type: application/json"
    ```

    * Replace `<TU_TOKEN>` with the obtained JWT token.

4.  **Test data backup and restore:**

    * **Backup all tables:**

        ```bash
        curl -X POST "http://localhost:8000/backup" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

    * **Backup a specific table (example: `hired_employees`):**

        ```bash
        curl -X POST "http://localhost:8000/backup?table=hired_employees" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

    * **Restore a specific table (example: `hired_employees`):**

        ```bash
        curl -X POST "http://localhost:8000/restore?table=hired_employees" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

5.  **Test metrics endpoints:**

    * **Hires by quarter (JSON):**

        ```bash
        curl -X GET "http://localhost:8000/metrics/hired_by_quarter" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

    * **Hires by quarter (HTML):**

        ```bash
        curl -X GET "http://localhost:8000/metrics/hired_by_quarter?format=html" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

    * **Departments above average (JSON):**

        ```bash
        curl -X GET "http://localhost:8000/metrics/departments_above_mean" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

    * **Departments above average (HTML):**

        ```bash
        curl -X GET "http://localhost:8000/metrics/departments_above_mean?format=html" \
            -H "Authorization: Bearer <TU_TOKEN>"
        ```

        * Replace `<TU_TOKEN>` with the obtained JWT token.

### Swagger UI

You can access the interactive documentation at:

```bash
http://localhost:8000/docs