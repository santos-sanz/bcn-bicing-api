from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from enum import Enum

from flow import flow, flow_parquet
from station_stats import station_stats, station_stats_parquet
from utils_local import *
import datetime

app = FastAPI()

# Allowed origins
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:3000",
    "https://bcn-bicing-dashboard.vercel.app/"
]

# Define valid file formats
class FileFormat(str, Enum):
    json = "json"
    parquet = "parquet"

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["GET"],  # Allowed methods
    allow_headers=["*"],  # Allowed headers
)

@app.get("/")
def read_root():
    return {"message": "Welcome to the BCN Bicing Analytics API!"}

@app.get("/timeframe/")
def get_timeframe_endpoint(format: FileFormat = FileFormat.json):
    try:
        min_timestamp, max_timestamp = get_timeframe()
        return {
            "from_date": min_timestamp,
            "to_date": max_timestamp,
            "format": format
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Station Status Request model
class StationStats(BaseModel):
    from_date: str
    to_date: str
    model: str
    model_code: str
    format: FileFormat = FileFormat.json

@app.get("/stats/")
def get_stats_data(
    from_date: str,
    to_date: str,
    model: str,
    model_code: str,
    format: FileFormat = FileFormat.json
):
    try:
        if format == FileFormat.parquet:
            response = station_stats_parquet(
                from_date=from_date,
                to_date=to_date,
                model=model,
                model_code=model_code
            )
        else:
            response = station_stats(
                from_date=from_date,
                to_date=to_date,
                model=model,
                model_code=model_code
            )
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Flow Request model
class FlowRequest(BaseModel):
    from_date: str
    to_date: str
    model: str
    model_code: str
    output: Optional[str] = 'both'
    aggregation_timeframe: Optional[str] = '1h'
    format: FileFormat = FileFormat.parquet

@app.get("/flow/")
def get_flow_data(
    from_date: str,
    to_date: str,
    model: str,
    model_code: str,
    output: str = 'both',
    aggregation_timeframe: str = '1h',
    format: FileFormat = FileFormat.parquet
):
    try:
        # Validate input parameters before calling flow
        if not all([from_date, to_date, model, model_code]):
            raise ValueError("Missing required parameters")
        
        # Validate date format
        try:
            datetime.datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
            datetime.datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            raise ValueError("Invalid date format. Use 'YYYY-MM-DD HH:MM:SS'")

        # Validate output parameter
        if output not in ['both', 'in', 'out']:
            raise ValueError("Invalid output parameter. Must be 'both', 'in', or 'out'")

        if format == FileFormat.parquet:
            response = flow_parquet(
                from_date=from_date,
                to_date=to_date,
                model=model,
                model_code=model_code,
                output=output,
                aggregation_timeframe=aggregation_timeframe
            )
        else:
            response = flow(
                from_date=from_date,
                to_date=to_date,
                model=model,
                model_code=model_code,
                output=output,
                aggregation_timeframe=aggregation_timeframe
            )
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Log the full error for debugging
        print(f"Error in flow endpoint: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    
@app.get("/flow_now/")
def get_flow_data_now(
    model: str,
    model_code: str,
    format: FileFormat = FileFormat.parquet
):
    try:
        if format == FileFormat.parquet:
            response = flow_parquet(
                from_date=(datetime.datetime.strptime(get_last_timestamp(), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
                to_date=get_last_timestamp(),
                model=model,
                model_code=model_code,
                output='both',
                aggregation_timeframe='1h'
            )
        else:
            response = flow(
                from_date=(datetime.datetime.strptime(get_last_timestamp(), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
                to_date=get_last_timestamp(),
                model=model,
                model_code=model_code,
                output='both',
                aggregation_timeframe='1h'
            )
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))





