from station_status import station_status
from flow import flow
from datetime import datetime, timedelta
import pytz

def status_flow(
        station_timestamp: str,
        model: str,
        model_code: str,
        output: str = 'both',
        aggregation_timeframe: str = '1h'
):
    """
    This function returns the inflow, outflow or both of bikes for a given timeframe, model and model code and the number of stations, the sum of bikes available and the sum of docks available for a given timestamp, model and model code.
    :param station_timestamp: str: timestamp of the station data
    :param model: str: model type
    :param model_code: str: model code
    :param output: str: inflow, outflow or both
    :param aggregation_timeframe: str: aggregation timeframe
    """

    # ValueError: invalid literal for int() with base 10: '2023-09-02'
    # Si 'station_timestamp' es una cadena de fecha
    station_timestamp = "2023-09-02"

    timezone = pytz.timezone('Etc/GMT-2')
    date_obj = datetime.strptime(station_timestamp, '%Y-%m-%d').replace(tzinfo=pytz.utc).astimezone(timezone)

    # Restar un día
    from_date_dt = date_obj - timedelta(days=1)

    # Convertir de vuelta a cadena si es necesario
    from_date = from_date_dt.strftime('%Y-%m-%d')
    

    
    flow_response = flow(
        from_date=from_date,
        to_date=station_timestamp,
        model=model,
        model_code=model_code,
        output=output,
        aggregation_timeframe=aggregation_timeframe
    )
    
    station_status_response = station_status(
        station_timestamp=station_timestamp,
        model=model,
        model_code=model_code
    )
    
    return flow_response, station_status_response