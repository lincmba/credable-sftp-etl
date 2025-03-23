import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env
load_dotenv()

# FastAPI app
app = FastAPI(title="Data API with Cursor-Based Pagination", version="1.1")

# Security: Token-based authentication
security = HTTPBearer()

# Get environment variables
DB_NAME = os.getenv("PG_DB")
DB_USER = os.getenv("PG_USER")
DB_PASS = os.getenv("PG_PASS")
DB_HOST = os.getenv("PG_HOST", "localhost")
DB_PORT = os.getenv("PG_PORT", "5432")
TABLE_NAME = os.getenv("PG_TABLE", "data_store")
API_KEY = os.getenv("API_KEY", "supersecretkey") # Change this in production
DATA_PRIMARY_ID = os.getenv("DATA_PRIMARY_ID", "_id")
ALGORITHM = "HS256"

# Create database engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Authentication function
def authenticate(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API Key")
    return credentials.credentials

def convert_numpy_types(df):
    return [
        {key: int(value) if isinstance(value, (pd.NA, pd.Int64Dtype().type)) else value
         for key, value in row.items()}
        for row in df.to_dict(orient="records")
    ]

# API Endpoint: Retrieve data with cursor-based pagination
@app.get("/data", tags=["Data"])
def get_data(
    credentials: str = Depends(authenticate),
    page_size: int = Query(10, alias="page_size", ge=1, le=100),
    cursor: Optional[int] = Query(None, alias="cursor"),  # Cursor points to the last seen 'DATA_PRIMARY_ID'
    start_date: Optional[str] = Query(None, alias="start_date"),
    end_date: Optional[str] = Query(None, alias="end_date")
):
    """
    Retrieve data from PostgreSQL using cursor-based pagination and date filtering.
    """
    try:
        # Build SQL query
        query = f"SELECT * FROM {TABLE_NAME}"
        conditions = []

        # Filter by date range
        if start_date:
            conditions.append(f"date >= '{start_date}'")
        if end_date:
            conditions.append(f"date <= '{end_date}'")

        # Cursor-based pagination: Fetch records **after** the given cursor (DATA_PRIMARY_ID)
        if cursor:
            conditions.append(f"{DATA_PRIMARY_ID} > {cursor}")  # Fetch records **after** the last seen 'id'

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY {DATA_PRIMARY_ID} ASC LIMIT {page_size};"  # Get the next 'page_size' rows

        # Fetch data
        df = pd.read_sql(query, engine)

        if df.empty:
            return {"message": "No more records.", "data": []}

        # Get the last 'DATA_PRIMARY_ID' as the next cursor
        next_cursor = df[f"{DATA_PRIMARY_ID}"].iloc[-1] if not df.empty else None

        # Convert to JSON response
        return {
            "page_size": page_size,
            "next_cursor": int(next_cursor),  # Pass this in the next request
            "total_records": len(df),
            "data": df.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
