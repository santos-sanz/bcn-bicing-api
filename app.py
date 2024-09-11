from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from flow import flow
from station_status import station_status
from status_flow import status_flow
from utils_local import *

app = FastAPI()

# TODO: Change post_code format to string (4802.0 -> '04802')

# TODO: Split the request in different functions

# TODO: Add status FULL (when docks = 0) & EMPTY (when bikes = 0)

# Allowed origins
origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://example.com",
    "http://localhost:3000",
    "https://bcn-bicing-dashboard.vercel.app/"
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["GET"],  # Allowed methods
    allow_headers=["*"],  # Allowed headers
)



# Station Status Request model
class StationStatus(BaseModel):
    station_timestamp: str
    model: str
    model_code: str

@app.get("/status/")
def get_status_data(
    station_timestamp: str,
    model: str,
    model_code: str
):
    try:
        response = station_status(
            station_timestamp=station_timestamp,
            model=model,
            model_code=model_code
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/status_now/")
def get_status_data_now(
    model: str,
    model_code: str
):

    try:
        response = station_status(
            station_timestamp= get_last_timestamp(),
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

@app.get("/flow/")
def get_flow_data(
    from_date: str,
    to_date: str,
    model: str,
    model_code: str,
    output: str = 'both',
    aggregation_timeframe: str = '1h'
):
    try:
        response = flow(
            from_date=from_date,
            to_date=to_date,
            model=model,
            model_code=model_code,
            output=output,
            aggregation_timeframe=aggregation_timeframe
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    
@app.get("/flow_now/")
def get_flow_data_now(
    model: str,
    model_code: str
):
    try:
        response = flow(
            from_date= (datetime.datetime.strptime(get_last_timestamp(), '%Y-%m-%d %H:%M:%S') - datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'),
            to_date= get_last_timestamp(),
            model=model,
            model_code=model_code,
            output='both',
            aggregation_timeframe='1h'
        )
        return response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Status & Flow Request model
class StatusFlowRequest(BaseModel):
    station_timestamp: str
    model: str
    model_code: str
    output: Optional[str] = 'both'
    aggregation_timeframe: Optional[str] = '1h'

@app.get("/status_flow/")
def get_status_flow_data(
    station_timestamp: str,
    model: str,
    model_code: str,
    output: str = 'both',
    aggregation_timeframe: str = '1h'
):
    try:
        flow_response, station_status_response = status_flow(
            station_timestamp=station_timestamp,
            model=model,
            model_code=model_code,
            output=output,
            aggregation_timeframe=aggregation_timeframe
        )
        return flow_response, station_status_response
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/")
def read_root():
    return {"message": "Welcome to the BCN Bicing API!"}




