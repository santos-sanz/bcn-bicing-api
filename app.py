from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Literal
from enum import Enum
import os
from dotenv import load_dotenv

from flow import flow, flow_parquet
from station_stats import station_stats, station_stats_parquet
from utils_local import *
import datetime

# Load environment variables
load_dotenv()

app = FastAPI(
    title="BCN Bicing Analytics API",
    description="""
    This API provides analytics and statistics for Barcelona's Bicing bike-sharing system.
    It offers various endpoints to analyze bike station data, including flow analysis and station statistics.
    
    ## Features
    * Get station statistics over time periods
    * Analyze bike flow (incoming/outgoing) at stations
    * Support for both JSON and Parquet data formats
    * Configurable time aggregation for flow analysis
    """,
    version="1.0.0",
    contact={
        "name": "Andrés Santos Sanz",
        "url": "https://github.com/santos-sanz",
        "email": "asantossanz@uoc.edu",
    },
)

# Get environment variables
PORT = int(os.getenv('PORT', 8000))
HOST = os.getenv('HOST', '0.0.0.0')
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', '').split(',')
DEFAULT_FORMAT = os.getenv('DEFAULT_FORMAT', 'parquet')
DEFAULT_AGGREGATION_TIMEFRAME = os.getenv('DEFAULT_AGGREGATION_TIMEFRAME', '1h')

# Define valid file formats
class FileFormat(str, Enum):
    """Supported file formats for data retrieval"""
    json = "json"
    parquet = "parquet"

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

@app.get("/",
    summary="API Welcome Endpoint",
    description="Returns a welcome message for the BCN Bicing Analytics API"
)
def read_root():
    return {"message": "Welcome to the BCN Bicing Analytics API!"}

@app.get("/timeframe/",
    summary="Get Data Timeframe",
    description="Returns the earliest and latest timestamps available in the dataset",
    response_description="Object containing from_date, to_date, and format used"
)
def get_timeframe_endpoint(format: FileFormat = FileFormat.parquet):
    """
    Retrieve the time range for which data is available.
    
    Args:
        format: Data format to use (json or parquet)
    
    Returns:
        dict: Contains from_date, to_date, and format used
    """
    try:
        if format == FileFormat.parquet:
            min_timestamp, max_timestamp = get_timeframe_parquet()
        else:
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
    """Request model for station statistics"""
    from_date: str = Field(..., description="Start date in format 'YYYY-MM-DD HH:MM:SS'")
    to_date: str = Field(..., description="End date in format 'YYYY-MM-DD HH:MM:SS'")
    model: str = Field(..., description="Station model type")
    model_code: str = Field(..., description="Specific station identifier")
    format: FileFormat = Field(default=FileFormat.json, description="Data format (json or parquet)")

@app.get("/stats/",
    summary="Get Station Statistics",
    description="Retrieve statistical data for a specific station over a time period",
    response_description="Statistical data for the requested station"
)
def get_stats_data(
    from_date: str = Field(..., description="Start date (YYYY-MM-DD HH:MM:SS)"),
    to_date: str = Field(..., description="End date (YYYY-MM-DD HH:MM:SS)"),
    model: str = Field(..., description="Station model type"),
    model_code: str = Field(..., description="Station identifier"),
    format: FileFormat = FileFormat(DEFAULT_FORMAT)
):
    """
    Get statistical data for a specific station.
    
    Args:
        from_date: Start date for analysis
        to_date: End date for analysis
        model: Station model type
        model_code: Station identifier
        format: Data format to use
    
    Returns:
        dict: Statistical data for the specified station
    """
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
    """Request model for flow analysis"""
    from_date: str = Field(..., description="Start date in format 'YYYY-MM-DD HH:MM:SS'")
    to_date: str = Field(..., description="End date in format 'YYYY-MM-DD HH:MM:SS'")
    model: str = Field(..., description="Station model type")
    model_code: str = Field(..., description="Specific station identifier")
    output: Optional[Literal['both', 'in', 'out']] = Field(
        default='both',
        description="Type of flow to analyze: 'both' for in/out, 'in' for incoming, 'out' for outgoing"
    )
    aggregation_timeframe: Optional[str] = Field(
        default='1h',
        description="Time window for data aggregation (e.g., '1h', '30min')"
    )
    format: FileFormat = Field(default=FileFormat.parquet, description="Data format (json or parquet)")

@app.get("/flow/",
    summary="Get Station Flow Analysis",
    description="""
    Analyze bike flow (incoming/outgoing) for a specific station over a time period.
    Supports different aggregation timeframes and can focus on incoming, outgoing, or both flows.
    """,
    response_description="Flow analysis data for the requested station"
)
def get_flow_data(
    from_date: str = Field(..., description="Start date (YYYY-MM-DD HH:MM:SS)"),
    to_date: str = Field(..., description="End date (YYYY-MM-DD HH:MM:SS)"),
    model: str = Field(..., description="Station model type"),
    model_code: str = Field(..., description="Station identifier"),
    output: str = Field(default='both', description="Flow type: 'both', 'in', or 'out'"),
    aggregation_timeframe: str = Field(default=DEFAULT_AGGREGATION_TIMEFRAME, description="Time window for aggregation"),
    format: FileFormat = FileFormat(DEFAULT_FORMAT)
):
    """
    Get flow analysis data for a specific station.
    
    Args:
        from_date: Start date for analysis
        to_date: End date for analysis
        model: Station model type
        model_code: Station identifier
        output: Type of flow to analyze ('both', 'in', 'out')
        aggregation_timeframe: Time window for data aggregation
        format: Data format to use
    
    Returns:
        dict: Flow analysis data for the specified station
    
    Raises:
        HTTPException: If input validation fails or processing errors occur
    """
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
    
@app.get("/flow_now/",
    summary="Get Recent Flow Analysis",
    description="Get flow analysis for the last 24 hours for a specific station",
    response_description="Recent flow analysis data for the requested station"
)
def get_flow_data_now(
    model: str = Field(..., description="Station model type"),
    model_code: str = Field(..., description="Station identifier"),
    format: FileFormat = FileFormat(DEFAULT_FORMAT)
):
    """
    Get flow analysis for the last 24 hours.
    
    Args:
        model: Station model type
        model_code: Station identifier
        format: Data format to use
    
    Returns:
        dict: Recent flow analysis data for the specified station
    """
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





