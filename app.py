from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from flow import flow
from station_status import station_status
from status_flow import status_flow

app = FastAPI()
# TODO: Add to station_status the average position(latitude and longitude) of the stations
# TODO: Add a function to only retrieve the most recent station_status and flow
# TODO: Match the API headers with frontend headers
# TODO: Change flow header from timestamp to readable datetime

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




