from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

from flow import flow
from station_stats import station_stats
from utils_local import *

app = FastAPI()

# Allowed origins
origins = [
    "http://localhost",
    "http://localhost:8080",
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

@app.get("/")
def read_root():
    return {"message": "Welcome to the BCN Bicing Analytics API!"}

@app.get("/timeframe/")
def get_timeframe_endpoint():
    try:
        min_timestamp, max_timestamp = get_timeframe()
        return {
            "from_date": min_timestamp,
            "to_date": max_timestamp
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Station Status Request model
class StationStats(BaseModel):
    from_date: str
    to_date: str
    model: str
    model_code: str

@app.get("/stats/")
def get_stats_data(
    from_date: str,
    to_date: str,
    model: str,
    model_code: str
):
    try:
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





