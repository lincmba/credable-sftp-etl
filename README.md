# SFTP ETL
This repository provides a complete data pipeline that retrieves CSV and JSON files from an SFTP server, cleans and flattens the data, and stores it in a PostgreSQL database. It also includes a FastAPI service with authentication, cursor-based pagination, and date filtering for efficient and secure data retrieval. 

### Configuration

The pipeline uses environment variables for configuration. Before running the script, source the .env file to load the necessary settings.

Create an .env file with the following structure and replace the values with actual credentials:

```commandline
# SFTP Credentials
export SFTP_HOST=host_ip_address
export SFTP_PORT=22
export SFTP_USER=your_sftp_username
export SFTP_PASS=your_sftp_password
export SFTP_REMOTE_DIR=/remote/directory/path
export SFTP_LOCAL_DIR=/local/directory/path

# PostgreSQL Database
export PG_DB=your_database_name
export PG_USER=your_database_user
export PG_PASS=your_database_password
export PG_HOST=localhost
export PG_PORT=5432
export PG_TABLE=your_table_name
export PG_DATE_COLUMN=date_column_name

# Data Cleaner
export DATA_PRIMARY_ID=primary_key_column

# API Authentication
export API_KEY=your_api_key

# Pagination Settings
export PAGE_SIZE_DEFAULT=10
export PAGE_SIZE_MAX=100
```


### Prerequisites

Ensure the following dependencies are installed:

```shell
pip install -r requirements.txt
```

This is divided into two:

## 1. ETL Pipeline Documentation


This ETL (Extract, Transform, Load) pipeline retrieves data from an SFTP server, cleans it, and stores it in a PostgreSQL database. The pipeline is managed using Python and requires an environment file for configuration.


### Running the ETL Pipeline

#### 1. Source the environment file:

```shell
source .env
```

#### 2. Run the ETL script:

```shell
python3 etl.py
```

### Components

#### 1. SFTP Client

Connects to an SFTP server and downloads .csv and .json files.
Uses paramiko for secure file transfer.

#### 2. Data Cleaning

Reads CSV and JSON files. Flattens nested JSON structures. Cleans column names by:
* Removing spaces and special characters.
* Standardizing case and merging consecutive underscores.
* Handles missing values using a default strategy.
* Ensures data consistency by fixing types and removing duplicates.

#### 3. PostgreSQL Storage

Stores cleaned data into a PostgreSQL table. Uses SQLAlchemy for database interactions. Creates the table if it does not exist. Appends new records dynamically.

## 2. Cursor-Based Pagination API

This API provides authenticated access to a PostgreSQL database table data. It supports:
* Authentication via Bearer Token.
* Cursor-based pagination for efficient data retrieval.
* Date filtering using the <PG_DATE_COLUMN> column.

### Running the API
Start the FastAPI server:

```shell
uvicorn api:app --reload
```
The API will be available at http://127.0.0.1:8000.

### Authentication
Include the API Key as a Bearer Token in requests:

```shell
Authorization: Bearer supersecretkey
```

### Endpoints
#### 4.1 Retrieve Data
`GET /data`

Fetch paginated and filtered records.

Query Parameters

```shell
Parameter	Type	Required  Default	Description
page_size	int	No	10	Number of records to fetch (max: 100).
cursor	int	No	None	Last retrieved id (used for pagination).
start_date	str	No	None	Filter by start column (format: YYYY-MM-DD).
end_date	str	No	None	Filter by start column (format: YYYY-MM-DD).
```

Example Request
```shell
curl -X 'GET' 'http://127.0.0.1:8000/data?page_size=5&start_date=2024-03-01' \
-H 'Authorization: Bearer supersecretkey'
```

Example Response
```json
{
  "page_size": 5,
  "next_cursor": 5,
  "total_records": 5,
  "data": [
    {"id": 1, "name": "John", "start": "2024-03-01"},
    {"id": 2, "name": "Alice", "start": "2024-03-02"},
    {"id": 3, "name": "Bob", "start": "2024-03-03"}
  ]
}

```

#### Paginating with Cursor
To get the next set of results, use next_cursor in the next request:

```shell
curl -X 'GET' 'http://127.0.0.1:8000/data?page_size=5&cursor=5' \
-H 'Authorization: Bearer supersecretkey'
```
