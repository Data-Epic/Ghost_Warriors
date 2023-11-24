#  Data Ingestion Pipeline

## Overview
This project contains the code for a data Ingestion pipeline, which is designed to streamline the process of extracting data from Google Sheets and inserting it into a PostgreSQL database using python and SQL.

## Code Structure
The `project5` directory  includes the following files:

- `app.py`: The central executable script that manages the workflow of the data pipeline, initiating the processes of data extraction and loading (EL).

- `config.py`: A script for the database configuration details which provides parameters to the main application as needed.

- `create_table.sql`: An SQL script that defines the schema for the PostgreSQL table, ensuring that the destination database is properly structured to receive the data.

- `insert_values.sql`: This SQL script is used to insert data into the PostgreSQL table, parameterized to prevent SQL injection and ensure efficient data entry.

- `database.ini`: Contains the PostgreSQL database connection parameters. This is an alternative to using environment variables for configuration.

- `.env`: A hidden file that is not checked into version control, containing sensitive environment variables such as API keys and database credentials.

- `.gitignore`: Specifies intentionally untracked files to ignore when using Git, typically containing compiled Python files (`*.pyc`), virtual environment directories, and other configuration files with sensitive information.

- `poetry.lock` & `pyproject.toml`: Managed by Poetry, these files ensure consistent dependency management. `pyproject.toml` defines the project's dependencies, while `poetry.lock` locks them to specific versions for reliable reproduction of the environment.

## Tools
- Python 3.8+
- PostgreSQL
- Poetry for dependency management
- DBeaver for executing query

## Step by step approach

1. **Navigate to the project directory** and install dependencies with Poetry:
   ```sh
   poetry install
   ```
2. **Set up the database** by running the `create_table.sql` script in PostgreSQL to prepare the database schema, this is done by the `connect_and_create_table`.

3. **Configure environment variables** in the `.env` file, including the PostgreSQL connection parameters and any API keys necessary for Google Sheets access.

### Execution
- With the environment configured, execute the `app.py` script to start the ETL process:
  ```sh
  poetry run python app.py
  ```

## Using DBeaver for Database Interaction
We utilize DBeaver for querying tool. After connecting DBeaver to your PostgreSQL database:

- Open the SQL editor to run custom queries or manage the database.
- To view a subset of the data:
  ```sql
  SELECT * FROM weather LIMIT 10;
  ```
<img width="951" alt="Screenshot 2023-11-24 230304" src="https://github.com/Data-Epic/Ghost_Warriors/assets/122959675/112737f3-9d1e-47cf-b837-8ae6861e4001">

  This SQL query retrieves the first ten rows from the `weather` table.
